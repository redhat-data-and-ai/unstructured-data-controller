# Unstructured Data Controller - Quick Start Guide

Simple guide to get the Unstructured Data Controller up and running.

## What It Does

The controller automatically processes unstructured files from S3:
1. **Reads files** from S3 bucket
2. **Converts** them to Markdown using Docling
3. **Chunks** the content for better processing
4. **Stores** the results in Snowflake

---

## Prerequisites

- Kubernetes cluster (or Kind for local dev)
- AWS/S3 access
- Snowflake account
- Go 1.24+ (for local development)

---

## Quick Setup

### 1. Create Namespace

```bash
kubectl create namespace unstructured-controller-namespace
```

### 2. Create Secrets

**AWS Secret:**
```bash
kubectl apply -f config/samples/aws-secret.yaml -n unstructured-controller-namespace
```

**Snowflake Private Key:**
```bash
kubectl create secret generic rhplatformtest-private-key \
  -n unstructured-controller-namespace \
  --from-file=privateKey=/path/to/rsa_key.p8
```

### 3. Setup Snowflake

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

### 4. Deploy Controller

**Apply ControllerConfig:**
```bash
kubectl apply -f config/samples/operator_v1alpha1_controllerconfig.yaml -n unstructured-controller-namespace
```

**Apply SQSConsumer:**
```bash
kubectl apply -f config/samples/operator_v1alpha1_sqsconsumer.yaml -n unstructured-controller-namespace
```

**Apply UnstructuredDataProduct:**
```bash
kubectl apply -f config/samples/operator_v1alpha1_unstructureddataproduct.yaml -n unstructured-controller-namespace
```

**Run Controller (local dev):**
```bash
make run
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
# 3. Chunk the content
# 4. Upload to Snowflake
```

### Check Results in Snowflake

```sql
-- Switch to the correct role
USE ROLE TESTING_ROLE;

-- List files in stage
LIST @TESTING_DB.TESTINGSCHEMA.TESTINGSCHEMA_INTERNAL_STG;

-- View processed data
SELECT $1 AS data 
FROM @TESTING_DB.TESTINGSCHEMA.TESTINGSCHEMA_INTERNAL_STG 
LIMIT 1;
```

### Monitor Progress

```bash
# Check UnstructuredDataProduct status
kubectl get unstructureddataproduct -n unstructured-controller-namespace

# Check DocumentProcessor status
kubectl get documentprocessor -n unstructured-controller-namespace

# Check ChunksGenerator status
kubectl get chunksgenerator -n unstructured-controller-namespace

# View controller logs
kubectl logs -f deployment/unstructured-data-controller -n unstructured-controller-namespace
```

---

## Configuration

The `UnstructuredDataProduct` CR defines the complete pipeline:

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: UnstructuredDataProduct
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
  
  # Where to store results
  destinationConfig:
    type: snowflakeInternalStage
    snowflakeInternalStageConfig:
      database: TESTING_DB
      schema: TESTINGSCHEMA
      stage: TESTINGSCHEMA_INTERNAL_STG
```

---

## Expected Output

After processing, each file produces:

1. **Local Cache** (`/tmp/cache/testunstructureddataproduct/`):
   - `file.pdf` - Original file
   - `file.pdf-metadata.json` - File metadata
   - `file.pdf-converted.json` - Converted Markdown
   - `file.pdf-chunks.json` - Chunked content

2. **Snowflake Stage** (`TESTING_DB.TESTINGSCHEMA.TESTINGSCHEMA_INTERNAL_STG`):
   - `testunstructureddataproduct/file.pdf-chunks.json` - Complete processed file with:
     - `convertedDocument`: Original conversion with metadata
     - `chunksDocument`: Chunked text ready for processing

