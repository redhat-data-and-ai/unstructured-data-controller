# Local Development Setup

This guide walks you through setting up the Unstructured Data Controller for local development.

## Prerequisites

- Docker
- [Kind](https://kind.sigs.k8s.io/)
- kubectl
- Go
- [yq](https://github.com/mikefarah/yq)
- [AWS CLI](https://aws.amazon.com/cli/) with [awslocal](https://github.com/localstack/awscli-local)
- [Docling](https://github.com/docling-project/docling-serve) (`pip install "docling-serve[ui]"`)
- [Ollama](https://ollama.com/) (for embedding models)

## Quick start (recommended)

Edit these files with your credentials and config before running setup:

- `config/samples/unstructured-secret.yaml` — S3 credentials, embedding endpoint, API keys
- `config/samples/operator_v1alpha1_controllerconfig.yaml` — Docling URL, storage bucket, concurrency

Then run:

```bash
make local-dev-setup
```

This single command:

1. Creates a Kind cluster (`unstructured-data-controller-local`)
2. Creates namespace `unstructured-controller-namespace`
3. Creates the local cache directory
4. Starts Docling (if `docling-serve` is installed)
5. Starts Ollama and pulls the embedding model
6. Starts LocalStack in Docker on `localhost:4566` (container `localstack-dev`)
7. Creates S3 buckets (ingestion, storage, output)
8. Installs CRDs
9. Applies secrets (`operator-secret` and `pipeline-secret`), ControllerConfig, and UnstructuredDataPipeline
10. Runs the controller (`make run`)

Press Ctrl+C to stop the controller. LocalStack and the Kind cluster keep running.

### Subsequent runs

If setup was already done and you only need to run the controller:

```bash
make run
```

Ensure LocalStack is still running:

```bash
curl -sf http://127.0.0.1:4566/_localstack/health
```

If not healthy, start it again:

```bash
./scripts/localstack.sh start
```

### Cleanup

Remove the full local environment (Docling, Ollama, LocalStack container, cache, Kind cluster):

```bash
make local-dev-cleanup
```

## LocalStack only

To start or manage LocalStack without the full setup:

```bash
./scripts/localstack.sh start   # start Docker container
./scripts/localstack.sh setup   # create S3 buckets
./scripts/localstack.sh status  # health check and resource list
./scripts/localstack.sh stop    # stop and remove container
```

View LocalStack API requests:

```bash
docker logs -f localstack-dev
```

See [Setting up LocalStack](docs/setup-localstack.md) for manual setup steps.

## Manual setup

If you prefer step-by-step setup instead of `make local-dev-setup`:

1. [Setting up LocalStack](docs/setup-localstack.md)
2. [Unstructured Data Controller with LocalStack](docs/setup-unstructured-controller.md)

## Verification

```bash
kubectl get controllerconfig controllerconfig -n unstructured-controller-namespace -o yaml
kubectl get unstructureddatapipeline -n unstructured-controller-namespace -o wide
kubectl get sourcecrawler,documentprocessor,chunksgenerator,vectorembeddingsgenerator,destinationsyncer -n unstructured-controller-namespace
```

The ControllerConfig should show `ConfigReady` with `status: "True"`. Pipeline stages should appear after the pipeline is applied.

## Next steps

Follow the [Creating Sample File Guide](docs/creating-sample-file.md) to upload a test file and process it through the pipeline.

## Running e2e tests

E2e tests use in-cluster LocalStack (not the host Docker container). See the test workflow in `.github/workflows/test-e2e.yml`.

```bash
export IMG=<your-image-registry>/<image-name>:<tag>
make docker-build docker-push test-e2e
```
