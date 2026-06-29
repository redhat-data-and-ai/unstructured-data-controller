# Unstructured Data Controller - Developer Guide

> A guide for new developers to understand, build, and contribute to the Unstructured Data Controller.

---

## Table of Contents

1. [What Is This Project?](#1-what-is-this-project)
2. [Core Concepts](#2-core-concepts)
3. [Architecture](#3-architecture)
4. [Data Models and API Types](#4-data-models-and-api-types)
5. [Business Logic](#5-business-logic)
6. [External Integrations](#6-external-integrations)
7. [Validation and Middleware](#7-validation-and-middleware)
8. [Shared Packages](#8-shared-packages)
9. [Development Setup](#9-development-setup)
10. [Build System](#10-build-system)
11. [Testing](#11-testing)
12. [Code Patterns and Conventions](#12-code-patterns-and-conventions)
13. [Deployment and Environments](#13-deployment-and-environments)
14. [Release Process](#14-release-process)
15. [Adding a New Feature End-to-End](#15-adding-a-new-feature-end-to-end)
16. [Troubleshooting](#16-troubleshooting)
17. [Glossary](#17-glossary)
18. [Reference Links](#18-reference-links)
19. [FAQ](#19-faq)

---

## 1. What Is This Project?

The Unstructured Data Controller is a Kubernetes operator that manages end-to-end pipelines for processing unstructured data (PDFs, DOCX, PPTX, Markdown files). It automates the flow from raw file ingestion through document conversion, text chunking, vector embedding generation, and final delivery to a destination store.

| Attribute | Value |
|-----------|-------|
| Language | Go 1.25+ |
| Framework | [Kubebuilder](https://book.kubebuilder.io/) v4 / controller-runtime v0.23 |
| Platform | Kubernetes 1.11.3+ |
| API Group | `operator.dataverse.redhat.com/v1alpha1` |
| External Services | AWS S3, AWS SQS, Docling Serve, Embedding APIs (Nomic) |
| Repository | `github.com/redhat-data-and-ai/unstructured-data-controller` |
| License | Apache 2.0 |

The operator watches for `UnstructuredDataPipeline` custom resources and orchestrates a chain of sub-resources (`DocumentProcessor`, `ChunksGenerator`, `VectorEmbeddingsGenerator`) to move files through the processing stages. An `SQSInformer` listens for S3 event notifications to trigger pipelines when new files arrive.

---

## 2. Core Concepts

### Kubernetes Operator Pattern

An operator extends Kubernetes by defining Custom Resource Definitions (CRDs) and running controllers that [reconcile](https://book.kubebuilder.io/cronjob-tutorial/controller-overview) each resource toward its desired state. When you create or update a CR, the controller detects the change and runs its reconciliation logic.

### Reconciliation Loop

Each controller implements a `Reconcile` method called by controller-runtime whenever a watched resource changes. The loop is idempotent - running it twice with the same input produces the same result. See the [controller-runtime reconciler docs](https://pkg.go.dev/sigs.k8s.io/controller-runtime/pkg/reconcile).

### Force-Reconcile Label Pattern

This project uses a custom label (`operator.dataverse.redhat.com/force-reconcile`) to trigger reconciliation between dependent controllers. When one controller finishes its work, it adds this label to the next controller's CR, which triggers its reconciliation via a custom predicate. The label is removed at the start of each reconciliation.

### FileStore (Local Cache + S3)

All file artifacts (raw files, converted files, chunks, embeddings) are stored in a dual-layer FileStore: a local filesystem cache backed by an S3 bucket. Operations write to both layers atomically, and reads check the local cache first before falling back to S3.

### Pipeline Stages

Files move through four processing stages, each tracked by a separate CR and suffix convention:

```
raw file (file.pdf)
  -> metadata    (file.pdf-metadata.json)
  -> converted   (file.pdf-converted.json)      [Docling]
  -> chunks      (file.pdf-chunks.json)          [LangChain]
  -> embeddings  (file.pdf-vector-embeddings.json) [Embedding API]
```

### Generation-Based Idempotency

Controllers compare `status.lastAppliedGeneration` against the resource's `.metadata.generation` to skip redundant reconciliations. When the spec has not changed, the controller short-circuits.

---

## 3. Architecture

### Component Diagram

```
                         +---------------------------+
                         |    SQS Queue (S3 events)  |
                         +------------+--------------+
                                      |
                                      v
+-------------------+     +-----------+-----------+
| ControllerConfig  |     |     SQSInformer       |
| (global config,   |     | (polls SQS, triggers  |
|  AWS clients,     |     |  pipeline reconcile)  |
|  Docling client)  |     +-----------+-----------+
+--------+----------+                |
         |                           | adds force-reconcile label
         | globals                   v
         |              +------------+--------------+
         +------------->| UnstructuredDataPipeline  |
                        | (orchestrator: creates    |
                        |  child CRs, syncs files,  |
                        |  delivers to destination) |
                        +--+-----+-----+-----------+
                           |     |     |
              creates/     |     |     |     creates/
              updates      |     |     |     updates
                           v     v     v
              +------------+ +---+---+ +------------------+
              |Document    | |Chunks | |VectorEmbeddings  |
              |Processor   | |Gen.   | |Generator         |
              |(Docling    | |(Lang- | |(Nomic embed API, |
              | convert)   | | chain)| | batch embed)     |
              +-----+------+ +---+---+ +--------+---------+
                    |             |              |
                    | force-      | force-       | force-
                    | reconcile   | reconcile    | reconcile
                    +----->-------+----->--------+----->Pipeline
```

### Directory Structure

```
unstructured-data-controller/
+-- api/v1alpha1/              # CRD type definitions (6 resources)
|   +-- chunksgenerator_types.go
|   +-- const.go               # Shared status reason constants
|   +-- controllerconfig_types.go
|   +-- documentprocessor_types.go
|   +-- groupversion_info.go   # API group registration
|   +-- sqsinformer_types.go
|   +-- unstructureddatapipeline_types.go
|   +-- vectorembeddingsgenerator_types.go
|   +-- zz_generated.deepcopy.go
+-- cmd/
|   +-- main.go                # Operator entrypoint
|   +-- mcp-server/main.go     # MCP server entrypoint (OAuth-secured)
+-- config/
|   +-- crd/bases/             # Generated CRD YAML manifests
|   +-- default/               # Kustomize default overlay
|   +-- deploy/                # Deployment kustomization
|   +-- manager/               # Deployment, PVC, Service manifests
|   +-- rbac/                  # Roles, bindings, service account
|   +-- samples/               # Example CR YAML files
+-- docs/                      # Setup guides and documentation
+-- internal/controller/       # Controller implementations
|   +-- chunksgenerator_controller.go
|   +-- config_utils.go        # Config CR health checks, AWS error helpers
|   +-- controllerconfig_controller.go
|   +-- documentprocessor_controller.go
|   +-- sqsinformer_controller.go
|   +-- unstructureddatapipeline_controller.go
|   +-- vectorembeddingsgenerator_controller.go
|   +-- controllerutils/       # Shared controller utilities
|       +-- conflict_retry.go  # StatusPatch, force-reconcile retry helpers
|       +-- force_reconcile.go # Label add/remove, custom predicate
+-- pkg/                       # Shared packages
|   +-- auth/                  # OAuth PKCE authentication (for MCP server)
|   +-- awsclienthandler/      # AWS S3, SQS, KMS client wrappers
|   +-- docling/               # Docling Serve HTTP client
|   +-- embedding/             # Vector embedding HTTP client
|   +-- filestore/             # Dual-layer file storage (local + S3)
|   +-- langchain/             # LangChain Go text splitter client
|   +-- unstructured/          # Data model types and file path utilities
+-- test/                      # Test infrastructure
|   +-- e2e/                   # End-to-end tests
|   +-- docling-serve/         # Docling K8s test manifests
|   +-- localstack/            # LocalStack K8s test manifests
|   +-- ollama-embedding/      # Ollama K8s test manifests
|   +-- resources/             # Test fixtures (secrets, notifications)
|   +-- utils/                 # Test helper functions
+-- vendor/                    # Vendored Go dependencies
```

### Application Bootstrap (`cmd/main.go`)

1. Registers the `operator.dataverse.redhat.com/v1alpha1` scheme
2. Parses CLI flags (metrics address, leader election, watch namespace, TLS config)
3. Creates the controller-runtime Manager with namespace-scoped caching
4. Registers all 6 controllers with the manager
5. Sets up certificate watchers for metrics and webhooks
6. Adds health (`/healthz`) and readiness (`/readyz`) probes
7. Starts the manager with OS signal handling

### Key Environment Configuration

| Flag / Env | Default | Purpose |
|------------|---------|---------|
| `--metrics-bind-address` | `0` (disabled) | Metrics endpoint address |
| `--health-probe-bind-address` | `:8081` | Health/readiness probe address |
| `--leader-elect` | `false` | Enable leader election for HA |
| `--watch-namespace` | `""` (all) | Namespace restriction for watches |
| `--metrics-secure` | `true` | Serve metrics over HTTPS |
| `--enable-http2` | `false` | Enable HTTP/2 (disabled for CVE mitigation) |

---

## 4. Data Models and API Types

All CRDs belong to the `operator.dataverse.redhat.com` group, version `v1alpha1`.

### 4.1 ControllerConfig

Global operator configuration. Only one instance is expected per namespace (named `controllerconfig`).

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: ControllerConfig
metadata:
  name: controllerconfig
spec:
  unstructuredSecret: "unstructured-data-secrets"
  unstructuredDataProcessingConfig:
    maxConcurrentDoclingTasks: 5
    maxConcurrentLangchainTasks: 10
    ingestionBucket: "ingestion-bucket"
    doclingServeURL: "http://docling-serve:5000"
    cacheDirectory: "/var/data/unstructured/"
    dataStorageBucket: "data-storage-bucket"
    unstructuredDataPipelineResyncInterval: 60
```

| Field | Type | Description |
|-------|------|-------------|
| `spec.unstructuredSecret` | string | Name of the K8s Secret containing AWS and Docling credentials |
| `spec.unstructuredDataProcessingConfig.maxConcurrentDoclingTasks` | int | Max parallel Docling conversion requests (semaphore) |
| `spec.unstructuredDataProcessingConfig.maxConcurrentLangchainTasks` | int | Max parallel LangChain chunking requests (semaphore) |
| `spec.unstructuredDataProcessingConfig.ingestionBucket` | string | Source S3 bucket for raw file ingestion |
| `spec.unstructuredDataProcessingConfig.doclingServeURL` | string | Docling Serve API base URL |
| `spec.unstructuredDataProcessingConfig.cacheDirectory` | string | Local filesystem path for file cache |
| `spec.unstructuredDataProcessingConfig.dataStorageBucket` | string | S3 bucket for the FileStore backing store |
| `spec.unstructuredDataProcessingConfig.unstructuredDataPipelineResyncInterval` | *int | Optional periodic resync interval in minutes (minimum: 1) |
| `status.conditions` | []Condition | Standard Kubernetes conditions (`ConfigReady`) |
| `status.lastAppliedGeneration` | int64 | Last successfully reconciled generation |

**Status condition type:** `ConfigReady`

### 4.2 UnstructuredDataPipeline

The top-level orchestrator resource. One per data product.

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: UnstructuredDataPipeline
metadata:
  name: my-data-product
spec:
  sourceConfig:
    type: s3
    s3Config:
      bucket: ingestion-bucket
      prefix: my-data-product
  destinationConfig:
    type: s3
    s3DestinationConfig:
      bucket: destination-bucket
      prefix: my-data-product
  documentProcessorConfig:
    type: docling
    doclingConfig:
      from_formats: ["pdf", "docx"]
      to_formats: ["md"]
      do_ocr: true
  chunksGeneratorConfig:
    strategy: recursiveCharacterTextSplitter
    recursiveCharacterSplitterConfig:
      chunkSize: 1000
      chunkOverlap: 200
  vectorEmbeddingsGeneratorConfig:
    modelName: "nomic-ai/nomic-embed-text-v1.5"
```

| Field | Type | Description |
|-------|------|-------------|
| `spec.sourceConfig.type` | `UnstructuredDataType` | Source type (currently only `s3`) |
| `spec.sourceConfig.s3Config.bucket` | string | Source S3 bucket name |
| `spec.sourceConfig.s3Config.prefix` | string | S3 key prefix to scope file discovery |
| `spec.destinationConfig.type` | `UnstructuredDataType` | Destination type (currently only `s3`) |
| `spec.destinationConfig.s3DestinationConfig` | S3Config | Destination bucket and prefix |
| `spec.documentProcessorConfig` | DocumentProcessorConfig | Docling conversion settings |
| `spec.chunksGeneratorConfig` | ChunksGeneratorConfig | Text chunking strategy and parameters |
| `spec.vectorEmbeddingsGeneratorConfig` | VectorEmbeddingsGeneratorConfig | Embedding model configuration |

**Status condition type:** `UnstructuredDataPipelineReady`

**Chunking strategies** (enum `ChunkingStrategy`):

| Value | Description |
|-------|-------------|
| `recursiveCharacterTextSplitter` | Split by recursive character separators |
| `markdownTextSplitter` | Split by markdown structure (headings, code blocks) |
| `tokenTextSplitter` | Split by token count using a tokenizer model |

### 4.3 DocumentProcessor

Manages Docling document conversion jobs for a data product.

| Field | Type | Description |
|-------|------|-------------|
| `spec.dataProduct` | string | Name of the parent data product |
| `spec.config` | DocumentProcessorConfig | Docling conversion configuration |
| `status.jobs` | []Job | Active conversion jobs with task IDs and status |
| `status.permanentlyFailingFiles` | []string | Files that exceeded max retry attempts |

**Status condition type:** `DocumentProcessorReady`

The `Job` struct tracks per-file conversion state:

| Job Field | Type | Description |
|-----------|------|-------------|
| `filePath` | string | Path of the raw file |
| `fileIdentifier` | string | ETag-based unique identifier |
| `documentConverter` | string | Converter used (always `docling`) |
| `taskID` | string | Docling async task ID |
| `status` | string | Docling task status (pending/started/success/failure) |
| `attempts` | int | Number of conversion attempts (max 3) |

### 4.4 ChunksGenerator

Generates text chunks from converted documents.

| Field | Type | Description |
|-------|------|-------------|
| `spec.dataProduct` | string | Name of the parent data product |
| `spec.config` | ChunksGeneratorConfig | Chunking strategy and parameters |

**Status condition type:** `ChunksGeneratorReady`

### 4.5 VectorEmbeddingsGenerator

Generates vector embeddings from text chunks.

| Field | Type | Description |
|-------|------|-------------|
| `spec.dataProduct` | string | Name of the parent data product |
| `spec.embeddingGeneratorConfig` | VectorEmbeddingsGeneratorConfig | Embedding model and config |

**Status condition type:** `VectorEmbeddingGenerationReady`

### 4.6 SQSInformer

Polls an SQS queue for S3 event notifications and triggers pipeline reconciliation.

| Field | Type | Description |
|-------|------|-------------|
| `spec.queueURL` | string | Full SQS queue URL to poll |

**Status condition type:** `SQSInformerReady`

### Shared Status Constants (`const.go`)

```go
const (
    SuccessfullyReconciled = "SuccessfullyReconciled"
    ReconcileFailed        = "ReconcileFailed"
)
```

### Common Type Methods

Every CRD type implements:

- `SetWaiting()` - Sets status condition to `Unknown` with reason `Waiting`
- `UpdateStatus(message string, err error)` - Sets condition to `True` (success) or `False` (failure)
- `init()` - Registers the type with the scheme builder

`ControllerConfig` additionally implements:
- `IsHealthy() bool` - Returns true if the `ConfigReady` condition is `True`

`DocumentProcessor` additionally implements:
- `AddOrUpdateJob(job Job)` - Upserts a job by file path
- `GetJobByFilePath(path string) *Job` - Finds a job by file path
- `DeleteJobByFilePath(path string)` - Removes a job by file path
- `AddPermanentlyFailingFile(path string)` - Marks a file as permanently failing
- `IsFilePermanentlyFailing(path string) bool` - Checks if a file is permanently failing

---

## 5. Business Logic

### Controller Inventory

| Controller | CRD | External Services | Triggers Next |
|------------|-----|-------------------|---------------|
| ControllerConfig | ControllerConfig | AWS (S3, SQS), Docling, LangChain | None (initializes globals) |
| UnstructuredDataPipeline | UnstructuredDataPipeline | AWS S3 (source + destination) | DocumentProcessor |
| DocumentProcessor | DocumentProcessor | Docling Serve | ChunksGenerator |
| ChunksGenerator | ChunksGenerator | LangChain (text splitters) | VectorEmbeddingsGenerator |
| VectorEmbeddingsGenerator | VectorEmbeddingsGenerator | Embedding API (Nomic) | UnstructuredDataPipeline |
| SQSInformer | SQSInformer | AWS SQS, S3 | UnstructuredDataPipeline |

### 5.1 ControllerConfig Controller

**Purpose:** Initializes all external service clients and sets global configuration.

**Reconciliation flow:**
1. Lists all `ControllerConfig` resources, takes the first one
2. Sets package-level globals: `ingestionBucket`, `dataStorageBucket`, `cacheDirectory`
3. Fetches the Kubernetes Secret named in `spec.unstructuredSecret`
4. Initializes the Docling client from `spec.doclingServeURL` with optional auth key from secret
5. Initializes the LangChain client with concurrency limits
6. Creates AWS clients from secret data:
   - Source S3 client (for ingestion bucket)
   - Source SQS client
   - Presign client (for generating presigned URLs)
   - Destination S3 client (for output delivery)
   - FileStore S3 client (for the intermediate storage bucket)
7. Sets the optional resync interval
8. Skips status update if the generation has not changed
9. Updates status to `ConfigReady`

**AWS Secret keys expected:**

| Secret Key | Purpose |
|------------|---------|
| `SOURCE_AWS_REGION` | Source AWS region |
| `SOURCE_AWS_ACCESS_KEY_ID` | Source AWS access key |
| `SOURCE_AWS_SECRET_ACCESS_KEY` | Source AWS secret key |
| `SOURCE_AWS_SESSION_TOKEN` | Source AWS session token (optional) |
| `SOURCE_AWS_ENDPOINT` | Source AWS endpoint (for LocalStack) |
| `DESTINATION_AWS_REGION` | Destination AWS region |
| `DESTINATION_AWS_ACCESS_KEY_ID` | Destination AWS access key |
| `DESTINATION_AWS_SECRET_ACCESS_KEY` | Destination AWS secret key |
| `DESTINATION_AWS_SESSION_TOKEN` | Destination session token (optional) |
| `DESTINATION_AWS_ENDPOINT` | Destination endpoint (for LocalStack) |
| `FILE_STORE_AWS_REGION` | FileStore AWS region |
| `FILE_STORE_AWS_ACCESS_KEY_ID` | FileStore AWS access key |
| `FILE_STORE_AWS_SECRET_ACCESS_KEY` | FileStore AWS secret key |
| `FILE_STORE_AWS_SESSION_TOKEN` | FileStore session token (optional) |
| `FILE_STORE_AWS_ENDPOINT` | FileStore endpoint (for LocalStack) |
| `DOCLING_USER_KEY` | Docling API authentication key (optional) |
| `NOMIC_ENDPOINT` | Nomic embedding API endpoint |
| `NOMIC_API_KEY` | Nomic embedding API key |

### 5.2 UnstructuredDataPipeline Controller

**Purpose:** Top-level orchestrator that creates child CRs and manages file ingestion and delivery.

**Reconciliation flow:**
1. Checks if `ControllerConfig` is healthy; requeues after 10s if not
2. Fetches the `UnstructuredDataPipeline` CR
3. Removes the force-reconcile label (if present)
4. Sets status to `Waiting`
5. Creates or updates `DocumentProcessor` CR for the data product
6. Creates or updates `ChunksGenerator` CR for the data product
7. Creates or updates `VectorEmbeddingsGenerator` CR for the data product
8. Syncs raw files from the source S3 bucket to the FileStore
9. Adds a force-reconcile label to the `DocumentProcessor` CR to trigger conversion
10. Lists all vector embedding files in the FileStore
11. Syncs embedding files to the destination S3 bucket
12. Updates status to `Ready`
13. If a resync interval is configured, requeues after that duration

### 5.3 DocumentProcessor Controller

**Purpose:** Converts raw files to markdown using Docling Serve.

**Reconciliation flow:**
1. Checks config health, fetches the CR, removes force-reconcile label
2. Reconciles existing jobs (checks Docling task status for each):
   - `success`/`partial_success`: Stores converted file, removes job from status
   - `failure`/`skipped`: Retries up to 3 attempts, then marks as permanently failing
   - `pending`/`started`: No action (waits)
3. Finds raw files that need conversion:
   - Checks if converted file exists with matching config
   - Checks if file is permanently failing
   - Checks if a job already exists with matching config
4. For each file needing conversion: calls Docling async API, adds job to status
5. Adds force-reconcile label to `ChunksGenerator` CR
6. If jobs are still running, requeues after 15 seconds

**Semaphore handling:** Uses `TryAcquire` (non-blocking) on the Docling semaphore. If the semaphore is full, the file is skipped this cycle. Recovers from semaphore panics on stale jobs from previous sessions.

### 5.4 ChunksGenerator Controller

**Purpose:** Splits converted markdown into text chunks using LangChain.

**Reconciliation flow:**
1. Checks config health, fetches the CR
2. Lists converted files (`*-converted.json`) from the FileStore
3. For each converted file, checks if chunking is needed (compares metadata)
4. Chunks the file using the configured strategy:
   - `recursiveCharacterTextSplitter`: Splits by character separators
   - `markdownTextSplitter`: Splits by markdown structure
   - `tokenTextSplitter`: Splits by token count
5. Stores chunks file (`*-chunks.json`) in the FileStore
6. Adds force-reconcile label to `VectorEmbeddingsGenerator` CR

### 5.5 VectorEmbeddingsGenerator Controller

**Purpose:** Generates vector embeddings from text chunks.

**Reconciliation flow:**
1. Checks config health, fetches the CR
2. Lists chunks files (`*-chunks.json`) from the FileStore
3. Removes force-reconcile label early (to accept more events)
4. For each chunks file, checks if embedding is needed (compares metadata)
5. Generates embeddings in batches of 10 via the HTTP embedding API
6. Handles rate limiting (HTTP 429) with 5-second retry
7. Stores embeddings file (`*-vector-embeddings.json`) in the FileStore
8. If any file was embedded, adds force-reconcile label to `UnstructuredDataPipeline` CR

### 5.6 SQSInformer Controller

**Purpose:** Polls an SQS queue for S3 event notifications to trigger pipeline processing.

**Reconciliation flow:**
1. Fetches the `SQSInformer` CR
2. Receives up to 10 messages from SQS (15-second long poll, 5-minute visibility timeout)
3. For each message, parses the S3 event notification JSON
4. Validates each record:
   - Bucket matches the ingestion bucket
   - Path has at least 2 levels (e.g., `dataproduct/file.pdf`)
   - For creation events: file exists and format is supported (`pdf`, `md`, `docx`, `pptx`)
   - For deletion events: only bucket and path validation
5. Extracts data product name from the first path segment
6. Adds force-reconcile label to the corresponding `UnstructuredDataPipeline` CR
7. Deletes processed messages from the SQS queue
8. Always requeues after 15 seconds (continuous polling)

---

## 6. External Integrations

### 6.1 AWS S3 (`pkg/awsclienthandler/s3_client.go`)

Three separate S3 clients are maintained as package-level singletons:

- **Source S3 client** - Reads from the ingestion bucket
- **Destination S3 client** - Writes to the output bucket
- **FileStore S3 client** - Reads/writes to the intermediate storage bucket

Clients are created from `AWSConfig` structs populated from Kubernetes Secret data. Custom endpoints are supported for LocalStack testing.

Key operations: `GetObject`, `PutObject`, `DeleteObject`, `ListObjectsV2`, `HeadObject`, presigned URL generation.

### 6.2 AWS SQS (`pkg/awsclienthandler/sqs_client.go`)

A singleton SQS client used by the SQSInformer controller. Created from the source AWS config. Supports `ReceiveMessage` and `DeleteMessage` operations.

### 6.3 AWS KMS (`pkg/awsclienthandler/kms_client.go`)

KMS client for key management operations. Available but not actively used in the main reconciliation flow.

### 6.4 Docling Serve (`pkg/docling/client.go`)

HTTP client for the [Docling](https://ds4sd.github.io/docling/) document conversion service.

- **Async conversion:** `POST /v1/convert/source/async` - submits a file URL for conversion
- **Status polling:** `GET /v1/status/poll/{taskID}` - checks task progress
- **Result retrieval:** `GET /v1/result/{taskID}` - fetches converted document

Uses a weighted semaphore to limit concurrent requests (`MaxConcurrentRequests`). Supports Bearer token or plain API key authentication. Merges user config with defaults (OCR enabled, PDF backend `dlparse_v4`, table mode `fast`).

Task statuses: `pending`, `started`, `success`, `failure`, `partial_success`, `skipped`.

### 6.5 Embedding API (`pkg/embedding/client.go`)

HTTP client implementing an OpenAI-compatible embeddings API.

- **Endpoint:** Configurable via the `NOMIC_ENDPOINT` secret key
- **Auth:** Bearer token from `NOMIC_API_KEY` secret key
- **Interface:** `EmbeddingGenerator` with `GenerateEmbeddings(ctx, inputs, encodingFormat)`
- **Batch size:** 10 texts per API call
- **Rate limiting:** Retries on HTTP 429 with 5-second backoff

Currently supports `nomic-ai/nomic-embed-text-v1.5` model. The model-to-credential mapping is defined in `controllerconfig_controller.go`.

### 6.6 LangChain Go (`pkg/langchain/client.go`)

Wraps the `github.com/tmc/langchaingo/textsplitter` package for text chunking. Uses a weighted semaphore for concurrency control (`MaxConcurrentRequests`).

Three splitting strategies:
- `SplitTextViaRecursiveCharacterTextSplitter` - character-based recursive splitting
- `SplitTextViaMarkdownTextSplitter` - markdown-aware splitting
- `SplitTextViaTokenTextSplitter` - tokenizer-based splitting

### 6.7 OAuth / MCP Server (`pkg/auth/`, `cmd/mcp-server/main.go`)

An OAuth-secured MCP (Model Context Protocol) server is available at `cmd/mcp-server/main.go`. The `pkg/auth/` package implements:

- PKCE-based OAuth 2.0 authorization code flow
- Generic OAuth provider support
- Token storage (in-memory)
- HTML callback pages
- Local HTTP server for OAuth callback handling

---

## 7. Validation and Middleware

### Event Filtering

All controllers use a combination of two predicates to filter which events trigger reconciliation:

```go
predicate.Or(predicate.GenerationChangedPredicate{}, labelPredicate)
```

- `GenerationChangedPredicate` - Triggers on spec changes (ignores status-only updates)
- `ForceReconcilePredicate` - Custom predicate that triggers when the `operator.dataverse.redhat.com/force-reconcile` label is added

### Status Updates via Patch

Status updates use `controllerutils.StatusPatch()` which:
1. Deep-copies the object
2. Applies the mutation function
3. Patches only the status subresource via merge-patch

This avoids conflict errors on status updates since it does not require a full re-fetch.

### Config Health Check

All controllers (except SQSInformer) call `IsConfigCRHealthy()` before processing. This fetches the `controllerconfig` resource and checks its `ConfigReady` condition. If not healthy, the controller requeues after 10 seconds.

---

## 8. Shared Packages

### `pkg/unstructured/` - Data Model and File Utilities

**File path utilities (`file.go`):**

| Function | Signature | Description |
|----------|-----------|-------------|
| `GetMetadataFilePath` | `(rawFilePath string) string` | Appends `-metadata.json` |
| `GetConvertedFilePath` | `(rawFilePath string) string` | Appends `-converted.json` |
| `GetChunksFilePath` | `(rawFilePath string) string` | Appends `-chunks.json` |
| `GetVectorEmbeddingsFilePath` | `(rawFilePath string) string` | Appends `-vector-embeddings.json` |
| `GetRawFilePathFromConvertedFilePath` | `(convertedFilePath string) string` | Strips `-converted.json` suffix |
| `FilterRawFilePaths` | `(filePaths []string) []string` | Filters to paths with both raw + metadata |
| `FilterConvertedFilePaths` | `(filePaths []string) []string` | Filters to `*-converted.json` |
| `FilterChunksFilePaths` | `(filePaths []string) []string` | Filters to `*-chunks.json` |
| `FilterVectorEmbeddingsFilePaths` | `(filePaths []string) []string` | Filters to `*-vector-embeddings.json` |

**Data types (`converted_file.go`, `chunks_file.go`, `embeddings_file.go`):**

- `ConvertedFile` - Contains `ConvertedDocument` (metadata + markdown content)
- `ChunksFile` - Contains `ConvertedDocument` + `ChunksDocument` (metadata + text chunks)
- `EmbeddingsFile` - Contains all above + `EmbeddingDocument` (metadata + embedding vectors)

Each file type has a metadata struct with an `Equal()` method for change detection.

**Interfaces (`source.go`, `destination.go`, `chunking.go`):**

| Interface | Method | Implementations |
|-----------|--------|-----------------|
| `DataSource` | `SyncFilesToFilestore(ctx, fs) ([]RawFileMetadata, error)` | `S3BucketSource` |
| `Destination` | `SyncFilesToDestination(ctx, fs, paths) error` | `S3Destination` |
| `Chunker` | `Chunk(text string) ([]string, error)` | `MarkdownSplitter`, `RecursiveCharacterSplitter`, `TokenSplitter` |

### `pkg/filestore/` - Dual-Layer File Storage

The `FileStore` struct provides a local cache + S3 backing store:

| Method | Signature | Description |
|--------|-----------|-------------|
| `New` | `(ctx, rootPath, s3Bucket) (*FileStore, error)` | Creates a new FileStore |
| `Store` | `(ctx, path, data) error` | Writes to local + S3 atomically |
| `Retrieve` | `(ctx, path) ([]byte, error)` | Reads from local cache, falls back to S3 |
| `Exists` | `(ctx, path) (bool, error)` | Checks local then S3 |
| `Delete` | `(ctx, path) error` | Deletes from both local and S3 |
| `ListFilesInPath` | `(ctx, path) ([]string, error)` | Lists objects in S3 prefix |
| `GetFileURL` | `(ctx, path) (string, error)` | Generates a presigned S3 URL |

Thread safety: Uses per-file `sync.Mutex` locks for both local and S3 operations.

### `internal/controller/controllerutils/` - Controller Utilities

| Function | Signature | Description |
|----------|-----------|-------------|
| `StatusPatch` | `(ctx, client, obj, mutate) error` | Patches status via merge-from |
| `AddForceReconcileLabel` | `(ctx, client, obj) error` | Adds the force-reconcile label |
| `RemoveForceReconcileLabel` | `(ctx, client, obj) error` | Removes the force-reconcile label |
| `AddForceReconcileLabelWithRetry` | `(ctx, client, key, newEmpty) error` | Add with conflict retry |
| `RemoveForceReconcileLabelWithRetry` | `(ctx, client, key, newEmpty) error` | Remove with conflict retry |
| `ForceReconcilePredicate` | `() predicate.Predicate` | Returns the custom label predicate |

---

## 9. Development Setup

### Prerequisites

| Tool | Version | Purpose |
|------|---------|---------|
| Go | 1.25.0+ | Language runtime |
| Docker / Podman | 17.03+ | Container builds |
| kubectl | 1.11.3+ | Kubernetes CLI |
| kind | latest | Local Kubernetes cluster |
| kustomize | v5.6.0 | Manifest generation (auto-downloaded) |
| controller-gen | v0.18.0 | CRD/RBAC code generation (auto-downloaded) |
| golangci-lint | v2.10.0 | Linting (auto-downloaded) |

### Quick Start

```bash
# 1. Clone the repository
git clone https://github.com/redhat-data-and-ai/unstructured-data-controller.git
cd unstructured-data-controller

# 2. Create a local Kind cluster
kind create cluster --name unstructured-data-controller-test-e2e

# 3. Install CRDs
make install

# 4. Apply sample resources
kubectl apply -k config/samples/

# 5. Run the operator locally
make run
```

### Local Development with LocalStack

For full local development with AWS services, set up LocalStack:

1. Deploy LocalStack in your Kind cluster (see `docs/setup-localstack.md`)
2. Apply the test manifests from `test/localstack/`
3. Create the required secrets from `config/samples/unstructured-secret.yaml`
4. Configure the `ControllerConfig` CR to point to LocalStack endpoints

See `docs/setup-unstructured-controller.md` for a detailed walkthrough.

### Dev Container

A dev container configuration is provided in `.devcontainer/`:

```json
// .devcontainer/devcontainer.json
{
  "image": "mcr.microsoft.com/devcontainers/go:1-1.24-bookworm",
  "features": { "ghcr.io/devcontainers/features/docker-in-docker:2": {} },
  "postCreateCommand": "bash .devcontainer/post-install.sh"
}
```

The `post-install.sh` script installs additional development tools.

---

## 10. Build System

### Makefile Targets

**Development:**

| Target | Description |
|--------|-------------|
| `make help` | Display all available targets |
| `make manifests` | Generate CRD manifests and RBAC from Go markers |
| `make generate` | Generate DeepCopy methods |
| `make fmt` | Format code with `go fmt` |
| `make vet` | Run `go vet` |
| `make lint` | Run golangci-lint |
| `make lint-fix` | Run golangci-lint with auto-fixes |

**Testing:**

| Target | Description |
|--------|-------------|
| `make test` | Run unit tests with coverage (excludes e2e) |
| `make test-e2e` | Run end-to-end tests on a Kind cluster |
| `make setup-test-e2e` | Create the Kind cluster for e2e tests |
| `make cleanup-test-e2e` | Delete the Kind e2e cluster |

**Build and Deploy:**

| Target | Description |
|--------|-------------|
| `make build` | Build the manager binary (`bin/manager`) |
| `make run` | Run the operator locally |
| `make docker-build` | Build Docker image |
| `make docker-push` | Push Docker image |
| `make docker-buildx` | Multi-platform Docker build |
| `make install` | Install CRDs into cluster |
| `make uninstall` | Remove CRDs from cluster |
| `make deploy` | Deploy controller to cluster |
| `make undeploy` | Remove controller from cluster |
| `make build-installer` | Generate consolidated `dist/install.yaml` |
| `make generate-deploy-manifests` | Generate deployment manifests to file |

**Configurable variables:**

| Variable | Default | Description |
|----------|---------|-------------|
| `IMG` | `ghcr.io/redhat-data-and-ai/unstructured-data-controller:latest` | Container image tag |
| `VERSION` | `0.0.1` | Project version |
| `DEPLOYMENT_NAMESPACE` | `unstructured-controller-namespace` | Target namespace |
| `PVC_SIZE` | `10Gi` | PersistentVolumeClaim size for cache |
| `CONTAINER_TOOL` | `docker` | Container runtime |

### Dockerfile

Multi-stage build using Red Hat UBI images:

1. **Builder stage** (`ubi9/go-toolset:1.26`): Compiles the Go binary with vendored dependencies
2. **Runtime stage** (`ubi9/ubi-minimal`): Minimal image with just the binary, runs as non-root user (UID 65532)

### CI/CD (GitHub Actions)

Four workflow files in `.github/workflows/`:

| Workflow | Trigger | Actions |
|----------|---------|---------|
| `build.yml` | Push/PR | Builds the Docker image |
| `lint.yml` | Push/PR | Runs golangci-lint |
| `test.yml` | Push/PR | Runs unit tests |
| `test-e2e.yml` | Push/PR | Runs end-to-end tests on Kind |

### Dependency Management

Dependencies are vendored (`vendor/` directory). Dependabot is configured for automated Go module updates (`.github/dependabot.yml`).

---

## 11. Testing

### Running Tests

```bash
# Unit tests with coverage
make test

# End-to-end tests (creates Kind cluster automatically)
make test-e2e

# Lint
make lint
```

### Unit Tests

Unit tests use the controller-runtime `envtest` package which provides a real API server without needing a full cluster. Coverage is written to `cover.out`.

The OAuth package has unit tests in `pkg/auth/oauth_test.go`.

### End-to-End Tests

Located in `test/e2e/`, using the `sigs.k8s.io/e2e-framework` package:

- `main_test.go` - Test environment setup and teardown
- `unstructured_test.go` - Integration tests for the full pipeline

**Test infrastructure (deployed as K8s manifests):**

| Component | Manifests | Purpose |
|-----------|-----------|---------|
| LocalStack | `test/localstack/` | Emulates AWS S3 and SQS locally |
| Docling Serve | `test/docling-serve/` | Document conversion service |
| Ollama Embedding | `test/ollama-embedding/` | Local embedding model server |

**Test fixtures:**

- `test/resources/unstructured/notification.json` - Sample SQS notification
- `test/resources/unstructured/unstructured-secret.yaml` - Sample AWS credentials secret
- `test/utils/utils_function.go` - Shared test helper functions

### Linting

golangci-lint v2 is configured via `.golangci.yml`. Run `make lint` to check and `make lint-fix` to auto-fix.

---

## 12. Code Patterns and Conventions

### Error Handling

- Wrap errors with context: `fmt.Errorf("failed to create database: %w", err)`
- Controllers have a dedicated `handleError` method that updates status and returns the error
- AWS "not initialized" errors cause requeue (not failure): checked via `IsAWSClientNotInitializedError()`

### Logging

Structured logging via `sigs.k8s.io/controller-runtime/pkg/log`:

```go
logger := log.FromContext(ctx)
logger.Info("reconciling", "controller", ControllerName)
logger.Error(err, "failed to process", "file", filePath)
```

### File Naming Conventions

| Pattern | Location | Example |
|---------|----------|---------|
| `*_types.go` | `api/v1alpha1/` | `chunksgenerator_types.go` |
| `*_controller.go` | `internal/controller/` | `documentprocessor_controller.go` |
| `*_test.go` | Next to source | `oauth_test.go` |
| `client.go` | `pkg/*/` | `pkg/docling/client.go` |

### Identifier Naming

| Type | Convention | Example |
|------|-----------|---------|
| Package-level vars | camelCase | `doclingClient`, `ingestionBucket` |
| Exported types | PascalCase | `FileStore`, `ChunksGenerator` |
| Constants | PascalCase | `DocumentProcessorCondition` |
| Controller names | PascalCase string const | `DocumentProcessorControllerName` |

### Controller Patterns

Every controller follows this structure:

1. **Health check** - `IsConfigCRHealthy()`
2. **Fetch resource** - `r.Get(ctx, req.NamespacedName, cr)`
3. **Remove force-reconcile label** - via `controllerutils.RemoveForceReconcileLabelWithRetry()`
4. **Set waiting status** - via `controllerutils.StatusPatch()` calling `cr.SetWaiting()`
5. **Business logic** - Process files, call external services
6. **Trigger next stage** - Add force-reconcile label to the next controller's CR
7. **Update success status** - via `controllerutils.StatusPatch()` calling `cr.UpdateStatus()`

---

## 13. Deployment and Environments

### Kubernetes Manifests

**Manager Deployment (`config/manager/manager.yaml`):**
- Single-replica Deployment
- Runs as non-root user (UID 65532)
- Mounts a PVC for the local file cache
- Exposes port 8081 for health probes
- Configurable resource limits

**PersistentVolumeClaim (`config/manager/pvc.yaml`):**
- Size configurable via `PVC_SIZE` (default 10Gi)
- Provides the local cache directory for the FileStore

**Service (`config/manager/service.yaml`):**
- ClusterIP service for the controller manager

### RBAC

- **ServiceAccount:** `unstructured-data-controller-controller-manager`
- **Role:** `manager-role` - Full CRUD on all 6 CRDs + read access to Secrets
- **Leader Election Role:** Manages leases, configmaps, events for leader election
- **Metrics Auth Role:** Allows creating token reviews and subject access reviews

### Kustomize Overlays

- `config/crd/` - CRD-only installation
- `config/default/` - Full deployment with metrics and RBAC
- `config/deploy/` - Deployment overlay with configurable namespace and suffix

### Deploy Command

```bash
# Deploy with custom image and namespace
make deploy \
  IMG=ghcr.io/redhat-data-and-ai/unstructured-data-controller:v0.1.0 \
  DEPLOYMENT_NAMESPACE=my-namespace \
  PVC_SIZE=20Gi
```

---

## 14. Release Process

### Versioning

The project uses semantic versioning. The version is set via the `VERSION` variable in the Makefile (default: `0.0.1`).

### Container Images

Images are published to `ghcr.io/redhat-data-and-ai/unstructured-data-controller`.

```bash
# Build and push a release image
make docker-build docker-push IMG=ghcr.io/redhat-data-and-ai/unstructured-data-controller:v0.1.0

# Generate OLM bundle
make bundle VERSION=0.1.0
```

### Multi-Platform Builds

```bash
make docker-buildx IMG=ghcr.io/redhat-data-and-ai/unstructured-data-controller:v0.1.0
```

Supports: `linux/arm64`, `linux/amd64`, `linux/s390x`, `linux/ppc64le`.

---

## 15. Adding a New Feature End-to-End

This walkthrough adds a hypothetical `MetadataEnricher` stage between chunking and embedding.

### Step 1: Define the CRD type

Create `api/v1alpha1/metadataenricher_types.go`:

```go
package v1alpha1

import metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

const MetadataEnricherCondition = "MetadataEnricherReady"

type MetadataEnricherSpec struct {
    DataProduct string `json:"dataProduct,omitempty"`
    // Add your spec fields here
}

type MetadataEnricherStatus struct {
    LastAppliedGeneration int64              `json:"lastAppliedGeneration,omitempty"`
    Conditions            []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
type MetadataEnricher struct {
    metav1.TypeMeta   `json:",inline"`
    metav1.ObjectMeta `json:"metadata,omitempty"`
    Spec   MetadataEnricherSpec   `json:"spec,omitempty"`
    Status MetadataEnricherStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true
type MetadataEnricherList struct {
    metav1.TypeMeta `json:",inline"`
    metav1.ListMeta `json:"metadata,omitempty"`
    Items           []MetadataEnricher `json:"items"`
}

// Implement SetWaiting() and UpdateStatus() following existing patterns

func init() {
    SchemeBuilder.Register(&MetadataEnricher{}, &MetadataEnricherList{})
}
```

### Step 2: Generate code and manifests

```bash
make generate   # Generates DeepCopy methods
make manifests  # Generates CRD YAML and RBAC
```

### Step 3: Create the controller

Create `internal/controller/metadataenricher_controller.go` following the standard pattern:

1. Add RBAC annotations
2. Check config health
3. Fetch resource, remove force-reconcile label, set waiting
4. Implement your business logic
5. Add force-reconcile label to `VectorEmbeddingsGenerator` CR
6. Update status

### Step 4: Register the controller

In `cmd/main.go`, add:

```go
if err := (&controller.MetadataEnricherReconciler{
    Client: mgr.GetClient(),
    Scheme: mgr.GetScheme(),
}).SetupWithManager(mgr); err != nil {
    setupLog.Error(err, "unable to create controller", "controller", "MetadataEnricher")
    os.Exit(1)
}
```

### Step 5: Update the chain

Modify `ChunksGenerator` controller to trigger `MetadataEnricher` instead of `VectorEmbeddingsGenerator`. Modify `UnstructuredDataPipeline` controller to create the `MetadataEnricher` CR.

### Step 6: Add sample and test

Create `config/samples/operator_v1alpha1_metadataenricher.yaml` and add tests.

### Step 7: Verify

```bash
make generate manifests fmt vet
make test
make lint
make install
make run
kubectl apply -f config/samples/operator_v1alpha1_metadataenricher.yaml
```

---

## 16. Troubleshooting

| Symptom | Cause | Solution |
|---------|-------|---------|
| Controller logs "ControllerConfig CR is not ready yet" | `ControllerConfig` has not been reconciled or is unhealthy | Check `kubectl get controllerconfig -o yaml` for error conditions. Verify the unstructured secret exists. |
| Controller logs "S3 client not initialized yet" | ControllerConfig controller has not finished initializing AWS clients | Wait for ControllerConfig to reconcile. Check the secret has valid AWS credentials. |
| DocumentProcessor stuck in "Waiting" | Docling semaphore is full or Docling Serve is unreachable | Check `maxConcurrentDoclingTasks`, verify Docling Serve URL and health. |
| File marked as "permanently failing" | Docling conversion failed 3 times | Check the file format is supported. Inspect Docling Serve logs. Remove from `permanentlyFailingFiles` in status to retry. |
| SQSInformer not triggering pipelines | SQS queue URL is wrong or credentials are invalid | Verify `spec.queueURL`, check source AWS credentials in the secret. |
| Embedding returns 429 errors | Rate limit exceeded on the embedding API | The controller retries after 5 seconds automatically. Check API usage limits. |
| "semaphore: released more than held" panic | Stale job from a previous operator session | The controller recovers from this automatically and removes the stale job. |
| CRDs not found | CRDs not installed | Run `make install` |
| Operator exits immediately | Missing `--watch-namespace` or invalid kubeconfig | Check CLI flags and kubeconfig context |

### Debug Commands

```bash
# Check CRDs are installed
kubectl get crds | grep dataverse

# View pipeline status
kubectl get unstructureddatapipelines -o wide

# View all resource statuses
kubectl get controllerconfigs,unstructureddatapipelines,documentprocessors,chunksgenerators,vectorembeddingsgenerators,sqsinformers -o wide

# Check controller logs
kubectl logs deployment/unstructured-data-controller-controller-manager -f

# Describe a resource for events
kubectl describe unstructureddatapipeline my-data-product

# View DocumentProcessor jobs
kubectl get documentprocessor my-data-product -o jsonpath='{.status.jobs}' | jq .
```

---

## 17. Glossary

| Term | Definition |
|------|------------|
| **CR** | Custom Resource - a Kubernetes object instance of a CRD |
| **CRD** | Custom Resource Definition - extends the Kubernetes API with new types |
| **Reconcile** | The process of comparing desired state (spec) to actual state and taking corrective action |
| **FileStore** | The dual-layer storage system (local filesystem + S3) used for intermediate artifacts |
| **Docling** | IBM's open-source document conversion tool that converts PDF/DOCX/PPTX to markdown |
| **Force-reconcile label** | A Kubernetes label used to trigger reconciliation between dependent controllers |
| **Chunking** | Breaking a large text document into smaller pieces for embedding generation |
| **Vector embedding** | A numerical representation of text as a fixed-length array of floats |
| **Ingestion bucket** | The S3 bucket where raw unstructured files are uploaded for processing |
| **Data storage bucket** | The S3 bucket backing the FileStore for intermediate artifacts |
| **Destination bucket** | The S3 bucket where final embedding files are delivered |
| **Data product** | A named collection of unstructured files managed as a single pipeline |
| **Semaphore** | Concurrency limiter for Docling and LangChain requests |
| **MCP** | Model Context Protocol - a protocol for tool-augmented LLM interactions |
| **PKCE** | Proof Key for Code Exchange - an OAuth 2.0 extension for public clients |
| **LocalStack** | AWS service emulator for local development and testing |
| **envtest** | controller-runtime test framework providing a real API server for tests |

---

## 18. Reference Links

### Framework

| Resource | URL |
|----------|-----|
| Kubebuilder Book | https://book.kubebuilder.io/ |
| controller-runtime | https://pkg.go.dev/sigs.k8s.io/controller-runtime |
| Kubebuilder Markers Reference | https://book.kubebuilder.io/reference/markers |
| e2e-framework | https://pkg.go.dev/sigs.k8s.io/e2e-framework |

### Kubernetes

| Resource | URL |
|----------|-----|
| Kubernetes API Conventions | https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md |
| Custom Resources | https://kubernetes.io/docs/concepts/extend-kubernetes/api-extension/custom-resources/ |
| Kind (local clusters) | https://kind.sigs.k8s.io/ |
| Kustomize | https://kustomize.io/ |

### External Services

| Resource | URL |
|----------|-----|
| Docling | https://ds4sd.github.io/docling/ |
| Docling Serve API | https://ds4sd.github.io/docling-serve/ |
| Nomic Embed Text v1.5 | https://huggingface.co/nomic-ai/nomic-embed-text-v1.5 |
| LangChain Go | https://github.com/tmc/langchaingo |
| AWS SDK for Go v2 | https://aws.github.io/aws-sdk-go-v2/ |
| LocalStack | https://localstack.cloud/ |
| MCP Specification | https://spec.modelcontextprotocol.io/ |

### Go

| Resource | URL |
|----------|-----|
| Effective Go | https://go.dev/doc/effective_go |
| Go Modules | https://go.dev/blog/using-go-modules |
| Go Testing | https://go.dev/doc/tutorial/add-a-test |

### Build Tools

| Resource | URL |
|----------|-----|
| golangci-lint | https://golangci-lint.run/ |
| GitHub Actions | https://docs.github.com/en/actions |
| Operator SDK | https://sdk.operatorframework.io/ |

---

## 19. FAQ

### General

**Q: What does this operator do in one sentence?**
A: It automates the end-to-end processing of unstructured documents (PDFs, DOCX, etc.) through conversion, chunking, and vector embedding generation, managed as Kubernetes custom resources.

**Q: How many CRDs does this project define?**
A: Six: `ControllerConfig`, `UnstructuredDataPipeline`, `DocumentProcessor`, `ChunksGenerator`, `VectorEmbeddingsGenerator`, and `SQSInformer`.

**Q: What file formats are supported?**
A: PDF, Markdown (`.md`), DOCX, and PPTX - determined by the `doclingSupportedFormats` variable in `sqsinformer_controller.go`.

**Q: Can I run multiple pipelines for different data products?**
A: Yes. Each `UnstructuredDataPipeline` CR corresponds to one data product. The operator creates separate child CRs (DocumentProcessor, ChunksGenerator, VectorEmbeddingsGenerator) per data product using the pipeline's name.

### API/Types

**Q: Where are the CRD type definitions?**
A: All types are in `api/v1alpha1/`. Each CRD has its own `*_types.go` file.

**Q: What chunking strategies are available?**
A: Three strategies defined in `unstructureddatapipeline_types.go`: `recursiveCharacterTextSplitter`, `markdownTextSplitter`, and `tokenTextSplitter`.

**Q: What embedding models are supported?**
A: Currently only `nomic-ai/nomic-embed-text-v1.5`. The model-to-credential mapping is in the `modelMap` variable in `controllerconfig_controller.go`.

**Q: How is the file suffix convention defined?**
A: In `pkg/unstructured/file.go`: `-metadata.json`, `-converted.json`, `-chunks.json`, `-vector-embeddings.json`.

**Q: What happens when a file fails conversion permanently?**
A: After 3 failed attempts (`maxDoclingConversionAttempts`), the file path is added to `status.permanentlyFailingFiles` in the DocumentProcessor CR and is skipped in future reconciliations.

### Business Logic

**Q: How do controllers communicate with each other?**
A: Via the force-reconcile label pattern. When controller A finishes, it adds the `operator.dataverse.redhat.com/force-reconcile` label to controller B's CR, which triggers B's reconciliation via a custom predicate.

**Q: How does the operator detect new files?**
A: Two ways: (1) The SQSInformer polls an SQS queue for S3 event notifications and triggers pipeline reconciliation. (2) Each pipeline reconciliation scans the source S3 bucket for files.

**Q: What happens if Docling Serve is down?**
A: The DocumentProcessor controller will fail to convert files and update its status to `ReconcileFailed`. The next reconciliation will retry.

**Q: How does the operator handle concurrent Docling requests?**
A: A weighted semaphore limits the number of concurrent requests (configured via `maxConcurrentDoclingTasks`). If the semaphore is full, the file is skipped this cycle using `TryAcquire` (non-blocking).

**Q: When does a pipeline re-sync automatically?**
A: When `unstructuredDataPipelineResyncInterval` is set in the ControllerConfig. The pipeline requeues itself after that many minutes.

### Development Workflow

**Q: How do I add a new CRD?**
A: Create the type in `api/v1alpha1/`, run `make generate manifests`, create a controller in `internal/controller/`, register it in `cmd/main.go`. See Section 15 for a full walkthrough.

**Q: How do I run the operator locally?**
A: `make install` to deploy CRDs, then `make run` to start the operator. You need a kubeconfig pointing at a cluster.

**Q: How do I run tests?**
A: `make test` for unit tests, `make test-e2e` for end-to-end tests (creates a Kind cluster automatically).

**Q: Where do I configure the linter?**
A: `.golangci.yml` in the project root.

**Q: How do I test with local AWS services?**
A: Deploy LocalStack using the manifests in `test/localstack/` and set the `*_AWS_ENDPOINT` keys in the secret to point to the LocalStack service.

### Deployment

**Q: What namespace does the operator run in?**
A: Configurable via `DEPLOYMENT_NAMESPACE` (default: `unstructured-controller-namespace`). Set in the Makefile.

**Q: Does the operator need a PVC?**
A: Yes. The FileStore uses a local filesystem cache, backed by a PVC. Size is configurable via `PVC_SIZE` (default 10Gi).

**Q: How do I deploy to production?**
A: Build and push the image with `make docker-build docker-push IMG=...`, then `make deploy IMG=... DEPLOYMENT_NAMESPACE=...`.

### Common Mistakes

**Q: Why is my controller not reconciling?**
A: Check: (1) CRDs are installed (`make install`). (2) ControllerConfig CR exists and is healthy. (3) The controller is registered in `cmd/main.go`. (4) The resource's spec actually changed (status-only updates do not trigger reconciliation).

**Q: Why are my changes to a CRD not reflected?**
A: Run `make generate manifests` after changing types, then `make install` to update CRDs in the cluster.

**Q: Why does the operator keep requeuing with "not initialized yet"?**
A: The ControllerConfig controller must run first to initialize AWS clients. Make sure a `ControllerConfig` CR exists with the correct secret reference.

**Q: I see "semaphore panic" in logs - is that a problem?**
A: No. This happens when stale jobs from a previous operator session exist. The controller automatically recovers by removing the stale job and re-processing the file.

**Q: Why does the VectorEmbeddingsGenerator remove its force-reconcile label early?**
A: It removes the label after listing files but before processing them. This allows it to accept new events (from ChunksGenerator) while processing the current batch, avoiding missed triggers.
