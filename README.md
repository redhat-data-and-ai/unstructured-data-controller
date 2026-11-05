# Unstructured Data Controller

A Kubernetes operator that turns unstructured documents into searchable knowledge for LLM agents. Pipelines crawl documents from S3, Git repositories, and Google Drive; convert them to Markdown with [docling](https://github.com/docling-project/docling-serve); chunk and embed them; store the embeddings in Snowflake; and sync the processed outputs to a destination S3 bucket. A companion OAuth-secured [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server exposes the results to AI agents (Cursor, Claude Code, custom agents).

## How it works

An `UnstructuredDataPipeline` custom resource chains processing stages. The operator creates one custom resource per stage and wires the dependencies (`dependsOn`); each stage processes files as they appear in its input directory in the S3-backed filestore (`pipelines/<pipeline>/stages/<stage>/`).

| Stage | Custom resource | What it does |
| ----- | --------------- | ------------ |
| Crawl | `SourceCrawler` | Ingest from S3 (optionally driven by SQS event notifications), Git repositories, or Google Drive folders |
| Convert | `DocumentProcessor` | Convert documents to Markdown using docling |
| Chunk | `ChunksGenerator` | Split content (e.g. recursive character or Markdown text splitting) |
| Embed | `VectorEmbeddingsGenerator` | Generate vector embeddings and store them in a Snowflake table (per-stage `queryConfig`) |
| Sync | `DestinationSyncer` | Copy processed outputs to a destination S3 bucket |

Operator-level infrastructure (filestore, docling, embedding/VLM endpoints, Google Drive and LDAP settings, Snowflake connection) is configured in a `ControllerConfig` custom resource.

All custom resources are served under the `operator.dataverse.redhat.com/v1alpha1` API group.

### MCP server

The MCP server (`cmd/unstructured-data-mcp-server`) lets agents discover pipelines and query them:

- `list_unstructured_data_pipelines_for_user`: pipelines the authenticated user can access
- `get_chunks_for_embeddings`: semantic (vector) search over a pipeline's documents
- `get_processed_document`: full processed Markdown for a document

Users authenticate through your OIDC identity provider (Keycloak, Red Hat SSO, Okta, Azure AD); the server acts as an OAuth Authorization Server proxy with PKCE and dynamic client registration. See [docs/mcp-server.md](docs/mcp-server.md).

## Documentation

- [Setting up LocalStack](docs/setup-localstack.md): local S3/SQS for development
- [Setting up the controller](docs/setup-unstructured-controller.md): full local walkthrough (LocalStack, docling, Ollama)
- [Processing your first file](docs/creating-sample-file.md)
- [MCP server](docs/mcp-server.md): configuration, endpoints, OAuth flow, tools
- [MCP skill file](skills/unstructured-data-mcp-skill.md): usage instructions for AI assistants

## Quick start (local development)

Prerequisites: Go 1.25+, `kubectl`, Docker or Podman, and a Kubernetes cluster (e.g. [Kind](https://kind.sigs.k8s.io/)). The full walkthrough also uses [LocalStack](https://localstack.cloud/), [docling-serve](https://github.com/docling-project/docling-serve), and [Ollama](https://ollama.com/). See the [setup guide](docs/setup-unstructured-controller.md) for details, including pointing the ControllerConfig at a locally running docling.

```bash
# 1. Namespace and secrets (see setup guide for prerequisites)
kubectl create namespace unstructured-controller-namespace
kubectl apply -f config/samples/unstructured-secret.yaml -n unstructured-controller-namespace

# 2. Install CRDs and run the controller locally
make install
make run

# 3. Create the operator config and a sample pipeline
kubectl apply -f config/samples/operator_v1alpha1_controllerconfig.yaml -n unstructured-controller-namespace
kubectl apply -f config/samples/operator_v1alpha1_unstructureddatapipeline.yaml -n unstructured-controller-namespace
```

Then [process your first file](docs/creating-sample-file.md). Sample pipelines for Git and Google Drive sources are provided alongside the S3 one in `config/samples/`.

## Deploying to a cluster

```bash
# Controller
make docker-build docker-push IMG=<registry>/unstructured-data-controller:tag
make deploy IMG=<registry>/unstructured-data-controller:tag

# MCP server (optional)
make docker-build-mcp docker-push-mcp IMG_MCP=<registry>/unstructured-data-mcp-server:tag
make deploy-mcp
```

`make deploy` and `make deploy-mcp` apply the manifests in `config/deploy` and `config/mcp` into `unstructured-controller-namespace` (override with `DEPLOYMENT_NAMESPACE=<ns>`). Undo with `make undeploy` / `make undeploy-mcp`, and remove the CRDs with `make uninstall`.

To generate a consolidated installer manifest (CRDs + controller deployment) for distribution:

```bash
make build-installer IMG=<registry>/unstructured-data-controller:tag
# writes dist/install.yaml
kubectl apply -f dist/install.yaml
```

## Development

```bash
make build      # build the controller binary
make test       # unit tests (controller-runtime envtest)
make test-e2e   # e2e tests on an isolated Kind cluster
make lint       # golangci-lint + yamllint
```

Run `make help` for the full list of targets.

## Contributing

Issues and pull requests are welcome. For substantial changes, please open an issue first to discuss what you would like to change.

## License

[Apache License 2.0](LICENSE)
