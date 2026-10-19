# Unstructured Data Controller with LocalStack

Simple guide to get the Unstructured Data Controller up and running using [LocalStack](https://localstack.cloud/) for local development and testing.

## Prerequisites

- [Docker](https://docs.docker.com/get-docker/) (or Podman with Docker socket)
- Kubernetes cluster (or Kind for local dev)
- [AWS CLI](https://aws.amazon.com/cli/) with [awslocal](https://github.com/localstack/awscli-local)
- [Docling-server](https://github.com/docling-project/docling-serve) using `pip install "docling-serve[ui]"`
- [Ollama](https://ollama.com/) for embedding models
- `kubectl` and access to a Kubernetes cluster (CRDs, secrets, and custom resources live in the cluster; the controller itself runs locally via `make run` in step 6)
- **LocalStack setup:** Follow [Setup LocalStack](setup-localstack.md) first if you haven't already.

### 1. Create Namespace

```bash
kubectl create namespace unstructured-controller-namespace
```

## 2. Run docling locally

```bash
docling-serve run --enable-ui
```

## 3. Run Ollama locally

```bash
ollama serve
ollama pull nomic-embed-text:latest
ollama cp nomic-embed-text:latest nomic-ai/nomic-embed-text-v1.5
```

### 4. Create secrets

The controller and pipeline stages each reference their own secrets. Create them:

```bash
kubectl apply -f config/samples/unstructured-secret.yaml -n unstructured-controller-namespace
```

This creates:
- `operator-secret`: operator-level credentials for the ControllerConfig, covering filestore S3 (`FILE_STORE_AWS_*`), docling (`DOCLING_USER_KEY`), embedding and VLM endpoints/keys (`NOMIC_*`, `GEMINI_*`, `VLM_API_KEY`), Git source tokens (`SOURCE_CRAWLER_GITLAB_TOKEN`), and the groups-reader Google service account
- `pipeline-secret`: pipeline-level credentials referenced by the pipeline and its stages via `secretRef`, covering source S3 (`SOURCE_S3_*`), source Google Drive service account (`SOURCE_GOOGLE_DRIVE_SERVICE_ACCOUNT_JSON`), and destination S3 (`DESTINATION_S3_*`)

### 5. Setup local cache directory

```bash
mkdir -p tmp/cache/
```

### 6. Deploy Controller

```bash
make install  # install CRDs
make run      # run controller locally
```

## 7. Create the ControllerConfig

The ControllerConfig sets up operator-level infrastructure (filestore, docling, concurrency limits):

```bash
kubectl apply -f config/samples/operator_v1alpha1_controllerconfig.yaml -n unstructured-controller-namespace
```

Verify it's healthy:

```bash
kubectl get controllerconfig controllerconfig -n unstructured-controller-namespace -o yaml
```

The status should show `ConfigReady` with `status: "True"`.

> **Note (running the controller locally):** the sample sets `doclingServeURL: "http://docling-serve:5001"`, an in-cluster service name. When running the controller locally with `make run` (step 6), point it at your local docling server instead:
>
> ```bash
> kubectl patch controllerconfig controllerconfig -n unstructured-controller-namespace --type merge -p '{"spec":{"doclingServeURL":"http://localhost:5001"}}'
> ```

## 8. Create the UnstructuredDataPipeline

```bash
kubectl apply -f config/samples/operator_v1alpha1_unstructureddatapipeline.yaml -n unstructured-controller-namespace
```

This creates the pipeline and all stage CRs automatically. Each stage watches its upstream dependencies and processes files as they appear.

## Verifying

Check pipeline and stage statuses:

```bash
kubectl get unstructureddatapipeline -n unstructured-controller-namespace -o wide
kubectl get sourcecrawler -n unstructured-controller-namespace -o wide
kubectl get documentprocessor -n unstructured-controller-namespace -o wide
kubectl get chunksgenerator -n unstructured-controller-namespace -o wide
kubectl get vectorembeddingsgenerator -n unstructured-controller-namespace -o wide
kubectl get destinationsyncer -n unstructured-controller-namespace -o wide
```

## Other source types

Sample pipelines for additional sources are provided:

- Git: `config/samples/operator_v1alpha1_unstructureddatapipeline_git.yaml`
- Google Drive: `config/samples/operator_v1alpha1_unstructureddatapipeline_gdrive.yaml`

Apply them the same way as the S3 pipeline (step 8). For Google Drive pipelines, uncomment and configure the `googleDriveConfig` (and, if you need identity mapping, `ldapConfig`) blocks in `config/samples/operator_v1alpha1_controllerconfig.yaml`, and provide the Drive service account key in `pipeline-secret`.

## Deploying the MCP server

To expose your pipelines to LLM agents via the Model Context Protocol, deploy the companion MCP server:

```bash
make docker-build-mcp docker-push-mcp   # build and push the MCP server image
make deploy-mcp                         # apply config/mcp to the cluster
```

`make deploy-mcp` applies the manifests in `config/mcp` using image `${IMG_MCP}` (default `ghcr.io/redhat-data-and-ai/unstructured-data-controller-mcp-server:latest`) into namespace `${DEPLOYMENT_NAMESPACE}` (default `unstructured-controller-namespace`). See [MCP Server](mcp-server.md) for configuration and usage.
