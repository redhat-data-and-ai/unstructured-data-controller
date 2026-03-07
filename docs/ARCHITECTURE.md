# Architecture

This document describes the architecture of the Unstructured Data Controller, a Kubernetes operator that processes raw unstructured documents (PDFs, DOCX, PPTX, Markdown, HTML, spreadsheets) into LLM-consumable chunks for use by MCP servers and AI agents.

![Architecture Diagram](unstructured-data-controller-architecture-diagram.jpg)

## Table of Contents

- [System Architecture](#system-architecture)
- [Custom Resource Definitions](#custom-resource-definitions)
- [Data Flow](#data-flow)
- [Internal Package Architecture](#internal-package-architecture)
- [Controller Reconciliation Patterns](#controller-reconciliation-patterns)
- [Deployment Architecture](#deployment-architecture)
- [Cross-Cutting Concerns](#cross-cutting-concerns)

---

## System Architecture

The Unstructured Data Controller sits at the center of a larger Dataverse AI platform, orchestrating the end-to-end pipeline from raw document ingestion to processed, chunked data ready for retrieval-augmented generation (RAG) workflows.

### Unstructured Data Sources

Users upload documents in various formats (DOC, PDF, PPT, HTML, and other unstructured data formats) to supported data sources:

- **S3 Buckets** — Primary ingestion source. Documents are uploaded to a configured S3 bucket with a prefix matching the data product name.
- **Google Drive** — Planned integration for ingesting documents from Google Drive.

S3 bucket events are forwarded to an SQS queue, which the controller consumes to trigger processing pipelines.

### Unstructured Data Controller

The controller is a Kubernetes operator built with the [Kubebuilder](https://book.kubebuilder.io/) framework. It manages five Custom Resource Definitions (CRDs) that together define and execute the document processing pipeline. The controller:

- Watches for S3 events via SQS to detect new or updated documents
- Syncs raw documents from S3 to a local cache and a data storage bucket
- Orchestrates document conversion through Docling
- Chunks converted documents using LangChain text splitters
- Uploads processed chunks to destination storage (Snowflake internal stages)

#### BYO Document Processing

The platform supports **Bring Your Own (BYO) Document Processing** — custom processing pipelines managed via Git and deployed as containers. This allows teams to define custom document transformation logic beyond the platform's built-in capabilities.

### Platform Supported Document Processing Capabilities

The platform provides three core processing stages, each with multiple strategy options:

| Stage | Providers | Description |
|-------|-----------|-------------|
| **Document Processing** | Docling, Gemini | Converts raw documents (PDF, DOCX, PPTX, etc.) to structured formats like Markdown |
| **Chunks Generation** | Docling, Gemini, LangChain | Splits converted documents into smaller, semantically meaningful chunks |
| **Vector Embeddings Generation** | Gemini, models.corp | Generates vector embeddings from chunks for similarity search |
| **Data Cleaning** | LangChain | Cleans and normalizes text data before processing |

**Additional Processing Strategies** include Knowledge Graph construction and Agentic Graph RAG for more advanced document understanding and retrieval patterns.

### Vector Database / Processed Documents Storage

Processed and chunked documents are stored in various backends:

- **Snowflake Cortex Search** — Primary destination. Supports both vector and keyword search. Chunks are uploaded to Snowflake internal stages.
- **OpenSearch** — Alternative vector search backend.
- **pgvector (PostgreSQL)** — Vector storage using the pgvector extension.
- **S3 Buckets** — Intermediate storage for processed documents.

### MCP Servers / Agents

Processed data is consumed by:

- **Dataverse MCP Server** — Platform-provided MCP server for querying processed documents.
- **BYO MCP Servers** — User-provided MCP servers that connect to the processed data stores.

### Dataverse Console

A web console that provides:

- Ingestion status monitoring for uploaded documents
- Processing status tracking across pipeline stages
- Event-driven updates as documents move through the pipeline

### AI Recommendation and Fine Tuning Engine

Uses processed data and search results to power:

- **Google Gemini** and **Anthropic Claude** for AI recommendations
- **Cortex Search** (vector + keyword) for retrieval
- Fine-tuning workflows based on processed document corpora

### Evaluation

The **LightSpeed Evaluation Framework** provides quality assessment of the processing pipeline, measuring chunking quality, embedding relevance, and retrieval accuracy.

---

## Custom Resource Definitions

All CRDs belong to the API group `operator.dataverse.redhat.com/v1alpha1` and are namespace-scoped.

### Resource Hierarchy

```
ControllerConfig (singleton, global configuration)
    |
    +-- SQSConsumer (watches S3 events via SQS)
    |       |
    |       +-- triggers --> UnstructuredDataProduct
    |
    +-- UnstructuredDataProduct (orchestrates pipeline)
            |
            +-- creates --> DocumentProcessor (document conversion)
            |
            +-- creates --> ChunksGenerator (text chunking)
```

### ControllerConfig

Global configuration resource that initializes all external service clients.

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: ControllerConfig
```

| Field | Description |
|-------|-------------|
| `spec.snowflakeConfig` | Snowflake connection settings (account, user, role, warehouse, private key secret) |
| `spec.awsSecret` | Reference to Kubernetes secret containing AWS credentials |
| `spec.unstructuredDataProcessingConfig.maxConcurrentDoclingTasks` | Concurrency limit for Docling document conversion |
| `spec.unstructuredDataProcessingConfig.maxConcurrentLangchainTasks` | Concurrency limit for LangChain text splitting |
| `spec.unstructuredDataProcessingConfig.ingestionBucket` | S3 bucket where raw documents are uploaded |
| `spec.unstructuredDataProcessingConfig.dataStorageBucket` | S3 bucket for persistent data storage |
| `spec.unstructuredDataProcessingConfig.doclingServeURL` | URL of the Docling conversion service |
| `spec.unstructuredDataProcessingConfig.doclingSecret` | Secret for Docling authentication |
| `spec.unstructuredDataProcessingConfig.cacheDirectory` | Local filesystem path for caching |
| `status.conditions[type=ConfigReady]` | Health status of the configuration |

The ControllerConfig reconciler initializes:
- AWS clients (S3, SQS, KMS)
- Snowflake client with JWT authentication
- Docling client with semaphore-based concurrency
- LangChain client with semaphore-based concurrency

All other controllers check ControllerConfig health before proceeding.

### SQSConsumer

Listens to an AWS SQS queue for S3 event notifications and triggers document processing.

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: SQSConsumer
```

| Field | Description |
|-------|-------------|
| `spec.queueURL` | AWS SQS queue URL to consume messages from |
| `status.conditions[type=SQSConsumerReady]` | Consumer health status |

The SQSConsumer reconciler:
1. Receives up to 10 messages per poll (15s wait, 300s visibility timeout)
2. Parses S3 event notifications from SQS messages
3. Validates file existence, extension (pdf, md, docx, pptx), and path depth
4. Adds a `force-reconcile` label to the matching `UnstructuredDataProduct` CR
5. Deletes processed messages from the queue
6. Requeues itself every 15 seconds

### UnstructuredDataProduct

The main orchestrator that defines the complete pipeline from source to destination.

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: UnstructuredDataProduct
```

| Field | Description |
|-------|-------------|
| `spec.sourceConfig.type` | Source type (`s3`) |
| `spec.sourceConfig.s3Config` | S3 bucket and prefix configuration |
| `spec.documentProcessorConfig` | Document conversion settings (processor type + Docling config) |
| `spec.chunksGeneratorConfig` | Chunking strategy and settings |
| `spec.destinationConfig.type` | Destination type (`snowflakeInternalStage`) |
| `spec.destinationConfig.snowflakeInternalStageConfig` | Snowflake stage, database, and schema |
| `status.conditions[type=UnstructuredDataProductReady]` | Pipeline health status |

The UnstructuredDataProduct reconciler:
1. Creates/updates child `DocumentProcessor` and `ChunksGenerator` CRs
2. Syncs files from S3 source to local cache and data storage bucket
3. Creates metadata files alongside raw files
4. Triggers document processing via force-reconcile labels
5. Uploads final chunks to the Snowflake destination

### DocumentProcessor

Handles document conversion from raw formats to Markdown using Docling.

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: DocumentProcessor
```

| Field | Description |
|-------|-------------|
| `spec.dataProduct` | Reference to parent UnstructuredDataProduct |
| `spec.config.type` | Processor type (e.g., `docling`) |
| `spec.config.doclingConfig` | Docling-specific settings (formats, OCR, table extraction, PDF backend) |
| `status.jobs[]` | Active processing jobs with task IDs, status, and attempt counts |
| `status.permanentlyFailingFiles[]` | Files that have exceeded retry limits |
| `status.conditions[type=DocumentProcessorReady]` | Processor health status |

**Supported Input Formats:** pdf, docx, doc, txt, html, md, csv, xlsx, pptx

**Docling Configuration Options:**

| Option | Description |
|--------|-------------|
| `from_formats` | Input document formats to accept |
| `to_formats` | Output formats (e.g., `md`) |
| `do_ocr` / `force_ocr` | Enable or force OCR processing |
| `ocr_engine` | OCR engine (`tesseract`, `easyocr`) |
| `pdf_backend` | PDF parsing backend (`pypdf`, `dlparse_v4`) |
| `table_mode` | Table extraction mode (`none`, `fast`, `accurate`) |
| `image_export_mode` | Image handling strategy (`copy`, `embed`) |

The DocumentProcessor reconciler:
1. Polls Docling for status of in-flight tasks
2. Stores converted Markdown on success
3. Retries failed conversions up to 3 attempts, then marks as permanently failing
4. Submits new files via presigned S3 URLs to Docling's async API
5. Triggers ChunksGenerator via force-reconcile label

### ChunksGenerator

Splits converted documents into smaller chunks using LangChain text splitters.

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: ChunksGenerator
```

| Field | Description |
|-------|-------------|
| `spec.dataProduct` | Reference to parent UnstructuredDataProduct |
| `spec.config.strategy` | Chunking strategy to use |
| `status.conditions[type=ChunksGeneratorReady]` | Generator health status |

**Chunking Strategies:**

| Strategy | Description | Key Options |
|----------|-------------|-------------|
| `recursiveCharacterTextSplitter` | Recursive splitting by character separators | `separators`, `chunkSize`, `chunkOverlap`, `keepSeparator` |
| `markdownTextSplitter` | Markdown-aware splitting that preserves document structure | `chunkSize`, `chunkOverlap`, `codeBlocks`, `headingHierarchy`, `joinTableRows` |
| `tokenTextSplitter` | Token-based splitting using model-specific tokenizers | `chunkSize`, `chunkOverlap`, `modelName`, `encodingName` |

---

## Data Flow

### End-to-End Pipeline

```
                    +-----------+
                    |  S3 Event |
                    | Notification
                    +-----+-----+
                          |
                          v
                    +-----+-----+
                    |    SQS    |
                    |   Queue   |
                    +-----+-----+
                          |
                          v
                 +--------+--------+
                 |   SQSConsumer   |
                 | (validates &    |
                 |  triggers)      |
                 +--------+--------+
                          |
              force-reconcile label
                          |
                          v
          +--------------+---------------+
          | UnstructuredDataProduct       |
          | (syncs files, orchestrates)   |
          +---------+----------+---------+
                    |          |
           creates  |          |  creates
                    v          v
        +-----------+--+  +---+-----------+
        | Document     |  | Chunks        |
        | Processor    |  | Generator     |
        | (Docling)    |  | (LangChain)   |
        +-----------+--+  +---+-----------+
                    |          ^
         force-reconcile       |
              label            |
                    +----------+
                          |
                          v
          +--------------+---------------+
          | UnstructuredDataProduct       |
          | (uploads chunks to           |
          |  Snowflake)                  |
          +------------------------------+
```

### File Lifecycle on Local Cache

Each document progresses through a series of derived files stored alongside the original:

| File | Example | Contents |
|------|---------|----------|
| Raw document | `dataproduct/file.pdf` | Original uploaded file |
| Metadata | `dataproduct/file.pdf-metadata.json` | ETag, UID, source tracking |
| Converted | `dataproduct/file.pdf-converted.json` | Markdown output + config hash |
| Chunks | `dataproduct/file.pdf-chunks.json` | Text chunks + config hash |

Config hashes stored in converted and chunks files enable **change detection** — if the processing configuration changes, only affected stages are re-executed.

---

## Internal Package Architecture

### `pkg/awsclienthandler/`

AWS service clients following a singleton pattern:

| File | Responsibility |
|------|---------------|
| `s3_client.go` | S3 operations: GetObject, PutObject, ListObjects, presigned URL generation |
| `sqs_client.go` | SQS message receive and delete operations |
| `kms_client.go` | KMS encryption support |
| `utils.go` | Shared AWS config initialization from Kubernetes secrets |

### `pkg/docling/`

Client for the Docling document conversion service:

- **Async API**: Submits documents via `/v1/convert/source/async`, polls via `/v1/status/poll/{taskID}`, retrieves results via `/v1/result/{taskID}`
- **Semaphore-based concurrency**: Uses `TryAcquire` to avoid blocking the reconciliation loop when the service is at capacity
- **Authentication**: Supports Bearer token and API key via Kubernetes secrets
- **Task statuses**: pending, started, success, partial_success, failure, skipped

### `pkg/langchain/`

Wrapper around the [langchaingo](https://github.com/tmc/langchaingo) library:

- Provides a unified interface for three text splitting strategies
- Semaphore-based concurrency control matching the Docling client pattern
- Converts strategy configuration from CRD specs into langchaingo options

### `pkg/filestore/`

Dual-layer storage system providing both local and remote persistence:

- **Local filesystem** — Fast access for processing, mounted via PVC at the configured cache directory
- **S3 data storage bucket** — Durable remote storage, synchronized on writes
- **Mutex-based file locking** for safe concurrent access
- Operations: Store, Retrieve, Exists, Delete, ListFilesInPath
- Presigned URL generation for giving Docling access to source files

### `pkg/snowflake/`

Snowflake client for uploading processed chunks:

- JWT-based authentication using private key from Kubernetes secrets
- PUT/GET/DELETE operations on internal stages
- Role switching before executing queries
- Mutex protection for SQL operations
- List files in stage (parses JSON responses from Snowflake)

### `pkg/unstructured/`

Core domain types and interfaces:

| File | Contents |
|------|----------|
| `source.go` | `DataSource` interface and `S3BucketSource` implementation |
| `destination.go` | `Destination` interface and `SnowflakeInternalStage` implementation |
| `converted_file.go` | `ConvertedFile` / `ConvertedDocument` structures with config metadata |
| `chunks_file.go` | `ChunksFile` / `ChunksDocument` structures with config metadata |
| `chunking.go` | `Chunker` interface with Markdown, RecursiveCharacter, and Token implementations |
| `file.go` | Path utilities, file extension validation, filtering functions |

### `internal/controller/controllerutils/`

Shared controller utilities:

- **Force-reconcile label** (`operator.dataverse.redhat.com/force-reconcile`): Used for inter-controller coordination
- `ForceReconcilePredicate()`: Custom predicate that triggers reconciliation when the label is added
- `AddForceReconcileLabel()` / `RemoveForceReconcileLabel()`: Label manipulation helpers

---

## Controller Reconciliation Patterns

### Force-Reconcile Label Coordination

Controllers trigger each other through Kubernetes label manipulation rather than direct API calls:

1. **SQSConsumer** adds the `force-reconcile` label to `UnstructuredDataProduct`
2. **UnstructuredDataProduct** adds the label to `DocumentProcessor`
3. **DocumentProcessor** adds the label to `ChunksGenerator`
4. Each controller removes the label from itself at the start of reconciliation

This pattern leverages Kubernetes watch mechanics for reliable, decoupled controller coordination.

### Semaphore-Based Concurrency Control

External service calls (Docling, LangChain) use `TryAcquire` semaphores:

- Configurable limits via `maxConcurrentDoclingTasks` and `maxConcurrentLangchainTasks` in ControllerConfig
- Non-blocking: if the semaphore is full, the reconciler requeues instead of blocking
- Prevents overwhelming external services while maintaining controller responsiveness

### Configuration-Aware Reprocessing

Each processing stage stores a hash of its configuration alongside the output:

- If the DocumentProcessor config changes, converted files are regenerated
- If the ChunksGenerator config changes, chunks are regenerated
- Raw files are only re-synced if their S3 ETag changes

### Dual-Layer FileStore

The filestore synchronizes data across two layers:

- **Local PVC** — Fast reads/writes during processing, bounded by PVC size (default 10Gi)
- **S3 data storage bucket** — Durable persistence, survives pod restarts

### Retry and Error Handling

- DocumentProcessor retries failed Docling conversions up to **3 attempts**
- Files exceeding the retry limit are marked as `permanentlyFailingFiles` in status
- Controllers requeue with configurable delays on transient failures
- ControllerConfig health is checked before processing begins

---

## Deployment Architecture

### Manager Pod

The controller runs as a single-replica Kubernetes Deployment:

- **Image base**: `ghcr.io/redhat-data-and-ai/unstructured-data-controller`
- **Base image**: Red Hat UBI9 (minimal)
- **Leader election**: Enabled via `--leader-elect` flag
- **Health probes**: Liveness (`/healthz`) and readiness (`/readyz`) on port 8081
- **Metrics**: HTTPS endpoint on port 8443, exposing controller-runtime metrics
- **Security context**: Non-root, no privilege escalation, all capabilities dropped (restricted Pod Security Standards)
- **Resources**: 10m-500m CPU, 64Mi-128Mi memory

### Storage

- **PersistentVolumeClaim**: `unstructured-datastore-cache` — 10Gi, ReadWriteOncePod, standard storage class
- Mounted at the configured `cacheDirectory` path for local document caching

### RBAC

| Role | Scope | Permissions |
|------|-------|-------------|
| Manager Role | Namespace | Full CRUD on all 5 CRDs + status subresources; get/list/watch on Secrets |
| Leader Election Role | Namespace | ConfigMaps, Coordination Leases, Events |
| Metrics Auth Role | Cluster | TokenReviews, SubjectAccessReviews |
| Metrics Reader Role | Cluster | GET `/metrics` |

### Namespace

All resources are deployed to a dedicated namespace (`unstructured-controller-namespace`) with kustomize prefix `unstructured-`.

---

## Cross-Cutting Concerns

The architecture diagram highlights several cross-cutting concerns that apply across the entire system:

| Concern | Implementation |
|---------|---------------|
| **Data Governance & Trust** | Controlled pipeline with auditable CRD status, metadata tracking, and processing lineage |
| **Observability** | Prometheus metrics via controller-runtime, status conditions on all CRDs, structured logging |
| **Scalability** | Semaphore-based concurrency limits, configurable parallelism, Kubernetes-native scaling |
| **Compliance** | Namespace-scoped RBAC, secret-based credential management, no hardcoded credentials |
| **Encryption at Rest & In Transit** | TLS for metrics and webhook endpoints, KMS support for S3, Snowflake JWT authentication |
| **RBAC** | Kubernetes RBAC with least-privilege roles, Snowflake role switching, service account isolation |
| **Reliability & Maintenance** | Retry logic with permanent failure tracking, leader election, health probes, graceful termination |
| **Data Versioning** | ETag-based change detection, config hash tracking, generation-based reconciliation |
