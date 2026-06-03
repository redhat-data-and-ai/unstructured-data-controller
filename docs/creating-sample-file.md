# Unstructured Data Controller - Quick Start Guide

Simple guide to get the first file processed by unstructured controller

## What It Does

The controller automatically processes unstructured files from S3:

1. **Reads files** from the ingestion S3 bucket
2. **Converts** them to Markdown using Docling
3. **Chunks** the content and optionally generates **vector embeddings**
4. **Syncs selected artifacts** to a destination — Snowflake internal stage or S3

Only artifacts listed under `destinationConfig.artifacts` are uploaded. Each artifact maps to a processing stage and is stored under `{dataProduct}/stages/{path}/` on the destination.

---

## Prerequisites

- Have local controller running using [Unstructured Data Controller with LocalStack](setup-unstructured-controller.md)

---

## Quick Setup

### 1. Setup Snowflake

Run in Snowflake SQL (using names from your config):

```sql
-- Use your warehouse and database
USE WAREHOUSE default;
USE DATABASE TESTING_DB;

-- Create schema and stage
CREATE SCHEMA IF NOT EXISTS TESTINGSCHEMA;
CREATE OR REPLACE STAGE TESTING_DB.TESTINGSCHEMA.TESTINGSCHEMA_INTERNAL_STG
    FILE_FORMAT = (TYPE = 'JSON');

-- Create role and grant permissions
CREATE ROLE IF NOT EXISTS TESTING_ROLE;
GRANT USAGE ON DATABASE TESTING_DB TO ROLE TESTING_ROLE;
GRANT USAGE ON SCHEMA TESTING_DB.TESTINGSCHEMA TO ROLE TESTING_ROLE;
GRANT READ, WRITE ON STAGE TESTING_DB.TESTINGSCHEMA.TESTINGSCHEMA_INTERNAL_STG TO ROLE TESTING_ROLE;

-- Grant role to your user (from controllerconfig.yaml)
GRANT ROLE TESTING_ROLE TO USER SNOWFLAKE_USER;
```

### 2. Create Unstructured Data Pipeline

**Apply UnstructuredDataPipeline:**

```bash
kubectl apply -f config/samples/operator_v1alpha1_unstructureddatapipeline.yaml -n unstructured-controller-namespace
```

---

## Test It

### Upload a File

```bash
# Upload to S3
aws s3 cp test.pdf s3://data-ingestion-bucket/testunstructureddataproduct/

# The controller will automatically:
# 1. Download the file
# 2. Convert it to Markdown
# 3. Chunk the content (and generate embeddings if configured)
# 4. Sync configured artifacts to the destination
```

### Check Results in Snowflake

```sql
-- Switch to the correct role
USE ROLE TESTING_ROLE;

-- List files in stage
LIST @TESTING_DB.TESTINGSCHEMA.TESTINGSCHEMA_INTERNAL_STG;

-- Example: read a chunks artifact (path depends on your artifacts config)
SELECT $1 AS data
FROM @TESTING_DB.TESTINGSCHEMA.TESTINGSCHEMA_INTERNAL_STG/testunstructureddataproduct/stages/chunks/
LIMIT 1;
```

### Monitor Progress

```bash
# Check UnstructuredDataPipeline status
kubectl get unstructureddatapipeline -n unstructured-controller-namespace

# Check DocumentProcessor status
kubectl get documentprocessor -n unstructured-controller-namespace

# Check ChunksGenerator status
kubectl get chunksgenerator -n unstructured-controller-namespace

# Check VectorEmbeddingsGenerator status (if configured)
kubectl get vectorembeddingsgenerator -n unstructured-controller-namespace

# View controller logs
kubectl logs -f deployment/unstructured-data-controller -n unstructured-controller-namespace
```

---

## Configuration

The `UnstructuredDataPipeline` CR defines the complete pipeline:

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: UnstructuredDataPipeline
metadata:
  name: testunstructureddataproduct
spec:
  # Where to read files from
  sourceConfig:
    type: s3
    s3Config:
      bucket: data-ingestion-bucket
      prefix: testunstructureddataproduct

  # How to convert files
  documentProcessorConfig:
    type: docling
    doclingConfig:
      from_formats: [pdf, docx, md]
      do_ocr: true

  # How to chunk content
  chunksGeneratorConfig:
    strategy: markdownTextSplitter
    markdownSplitterConfig:
      chunkSize: 1000
      chunkOverlap: 200

  # Optional: vector embeddings
  vectorEmbeddingsGeneratorConfig:
    modelName: nomic-ai/nomic-embed-text-v1.5
    nomicEmbedTextV15Config:
      encodingformat: float

  # Where to store results (Snowflake or S3)
  destinationConfig:
    type: snowflakeInternalStage
    # Required: which processing outputs to sync
    artifacts:
      - type: stage
        name: documentProcessorConfig
        path: processed-documents   # optional; see defaults below
      - type: stage
        name: chunksGeneratorConfig
        path: chunks
      - type: stage
        name: vectorEmbeddingsGeneratorConfig
        path: vector-embeddings
    snowflakeInternalStageConfig:
      database: TESTING_DB
      schema: TESTINGSCHEMA
      stage: TESTINGSCHEMA_INTERNAL_STG
```

### Destination artifacts

`destinationConfig.artifacts` is **required** (at least one entry). Each entry selects a processing stage to sync:

| `name` | File suffix | Default `path` (if omitted) |
|--------|-------------|-----------------------------|
| `documentProcessorConfig` | `-converted.json` | `processed-documents` |
| `chunksGeneratorConfig` | `-chunks.json` | `chunks` |
| `vectorEmbeddingsGeneratorConfig` | `-vector-embeddings.json` | `vector-embeddings` |

Set `path` to override the folder under `stages/` on the destination. Unknown `name` values are skipped.

You can list only the artifacts you need — for example, chunks only:

```yaml
destinationConfig:
  type: snowflakeInternalStage
  artifacts:
    - type: stage
      name: chunksGeneratorConfig
      path: chunks
  snowflakeInternalStageConfig:
    database: TESTING_DB
    schema: TESTINGSCHEMA
    stage: TESTINGSCHEMA_INTERNAL_STG
```

### S3 destination

To write artifacts to S3 instead of Snowflake, set `type: s3` and `s3DestinationConfig`. The controller uses destination AWS credentials from the unstructured secret (`DESTINATION_AWS_*` keys). See [setup guide](setup-unstructured-controller.md) for bucket and credential configuration.

Replace `destinationConfig` in the sample (do not set both Snowflake and S3 `type` at once):

```yaml
destinationConfig:
  type: s3
  artifacts:
    - type: stage
      name: chunksGeneratorConfig
      path: chunks
  s3DestinationConfig:
    bucket: output-chunks-bucket
    prefix: testunstructureddataproduct   # optional; defaults to CR name
```

Object keys use `{prefix}/stages/{path}/{filename}`.

---

