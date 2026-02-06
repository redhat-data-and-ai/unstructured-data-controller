# Unstructured Data Product Controller

A Kubernetes operator that automates the processing of unstructured documents (PDF, DOCX, etc.) stored in S3 buckets. This controller monitors S3 buckets, and sync the raw files from the upstream s3 bucket to the local filestore s3 bucket and local cache directory along with their metadata files (.pdf-metadata.json)

## Prerequisites

Before you begin, ensure you have the following installed on your system:

- **Go** (version 1.24.0 or later) - [Install Go](https://golang.org/doc/install)
- **Docker** or **Podman** (version 17.03 or later) - [Install Docker](https://docs.docker.com/get-docker/) or [Install Podman](https://podman.io/getting-started/installation)
- **kubectl** (version 1.11.3 or later) - [Install kubectl](https://kubernetes.io/docs/tasks/tools/)
- **kind** (Kubernetes in Docker) - [Install kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation)
- **AWS CLI Local** - For LocalStack interaction:
  ```bash
  pip install awscli-local
  ```

## Local Development Setup

Follow these steps to set up and test the Unstructured Data Product Controller locally.

### Step 1: Create a Kubernetes Cluster

Create a local Kubernetes cluster using kind:

```bash
kind create cluster
```

This command creates a local Kubernetes cluster. If a cluster already exists, it will notify you.

### Step 2: Install Custom Resource Definitions (CRDs)

Install the controller's CRDs into your cluster:

```bash
make install
```

This registers the `UnstructuredDataProduct` custom resource type with your Kubernetes cluster.

### Step 3: Start LocalStack

LocalStack simulates AWS services locally. Start it using Podman (or replace `podman` with `docker` if using Docker):

```bash
podman run \
  --rm -it \
  -p 127.0.0.1:4566:4566 \
  -p 127.0.0.1:4510-4559:4510-4559 \
  -v /var/run/docker.sock:/var/run/docker.sock \
  localstack/localstack
```

**Note:** Keep this terminal window open. LocalStack needs to run continuously.

### Step 4: Create S3 Buckets

Open a new terminal and create two S3 buckets in LocalStack:

**Ingestion Bucket** (where you upload source documents):
```bash
awslocal s3 mb s3://data-ingestion-bucket
```

**Storage Bucket** (where processed documents are stored):
```bash
awslocal s3 mb s3://data-storage-bucket
```

### Step 5: Configure Local Cache Directory

Create a local directory for caching processed documents:

```bash
mkdir -p tmp/cache
```

**Important:** Update the cache directory path in the controller code:

1. Open `internal/controller/unstructureddataproduct_controller.go`
2. Find line 39 (the `cacheDirectory` variable)
3. Replace the path with the absolute path to your `tmp/cache` directory:
   ```go
   cacheDirectory = "/absolute/path/to/your/project/tmp/cache/"
   ```

**Example:**
```go
cacheDirectory = "/Users/yourname/unstructured-data-controller/tmp/cache/"
```

### Step 6: Upload PDF Files to Ingestion Bucket

Upload your PDF files to the ingestion bucket with the following structure:

```bash
awslocal s3 cp /path/to/your/file.pdf s3://data-ingestion-bucket/YourPrefix/YourDataProductName/file.pdf
```

**Example:**
```bash
awslocal s3 cp ~/Documents/sample.pdf s3://data-ingestion-bucket/testunstructureddataproduct/sample.pdf
```

**Notes:**
- `YourPrefix`: A folder structure prefix (e.g., `testunstructureddataproduct`)
- `YourDataProductName`: Can be part of the prefix or separate folder
- You can upload multiple PDF files to the same prefix

### Step 7: Create AWS Credentials Secret

Create a Kubernetes secret with AWS credentials for LocalStack:

1. **Verify the endpoint in `config/samples/aws-secret.yaml`:**
   ```bash
   cat config/samples/aws-secret.yaml
   ```

   Ensure line 11 shows:
   ```yaml
   AWS_ENDPOINT: http://localhost:4566
   ```

2. **Apply the secret:**
   ```bash
   kubectl apply -f config/samples/aws-secret.yaml
   ```

This creates a secret named `aws-secret` that the controller uses to access S3.

### Step 8: Run the Operator

Start the controller in development mode:

```bash
make run
```

The controller will start running and processing files from the ingestion bucket. Keep this terminal open to view logs.

### Step 9: Create an UnstructuredDataProduct Resource

Apply the sample custom resource to start processing:

```bash
kubectl apply -f config/samples/operator_v1alpha1_unstructureddataproduct.yaml
```

## Understanding the UnstructuredDataProduct Configuration

The `UnstructuredDataProduct` custom resource defines how documents should be processed. Here's a breakdown of the configuration fields:

### Sample Configuration

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: UnstructuredDataProduct
metadata:
  name: unstructureddataproduct-sample
spec:
  # Source Configuration - Where to fetch documents from
  sourceConfig:
    type: "s3"                              # Options: ["s3"]
    s3Config:
      bucket: data-ingestion-bucket         # S3 bucket name for source documents
      prefix: testunstructureddataproduct   # Folder path within the bucket

  # Document Processing Configuration
  documentProcessorConfig:
    type: "docling"                         # Options: ["docling"]
    doclingConfig:
      from_formats:                         # Input document formats to process
        - pdf                               # Options: ["pdf", "docx", "doc", "txt", "html", "md", "csv", "xlsx"]
        - docx
        - doc
        - txt
        - html
        - md
        - csv
        - xlsx

      to_formats:                           # Output format
        - md                                # Options: ["md"]

      image_export_mode: "copy"             # Options: ["copy", "embed", "none"]
      do_ocr: true                          # Enable OCR for scanned documents - Options: [true, false]
      force_ocr: false                      # Force OCR even for text-based PDFs - Options: [true, false]
      ocr_engine: "tesseract"               # Options: ["tesseract"]
      ocr_lang: ["en"]                      # OCR languages - Options: ["en", "es", "fr", "de", "it", "pt", "ru", "zh", "ja", "ko", etc.]
      pdf_backend: "pypdf"                  # Options: ["pypdf", "pdfminer"]
      table_mode: "none"                    # Options: ["none", "basic", "advanced"]
      abort_on_error: true                  # Stop processing if errors occur - Options: [true, false]
      return_as_file: true                  # Return processed content as files - Options: [true, false]

  # Chunk Generation Configuration
  chunksGeneratorConfig:
    strategy: "markdownTextSplitter"        # Options: ["recursiveCharacterTextSplitter", "markdownTextSplitter", "tokenTextSplitter"]

    # Option 1: Recursive Character Text Splitter (use when strategy is "recursiveCharacterTextSplitter")
    recursiveCharacterSplitterConfig:
      separators: ["\n\n", "\n", " ", ""]   # List of separators to split on (array of strings)
      chunkSize: 1000                       # Maximum characters per chunk (integer value)
      chunkOverlap: 200                     # Character overlap between chunks (integer value)
      keepSeparator: true                   # Keep the separator in the chunks - Options: [true, false]

    # Option 2: Markdown Text Splitter (use when strategy is "markdownTextSplitter")
    markdownSplitterConfig:
      chunkSize: 1000                       # Maximum characters per chunk (integer value)
      chunkOverlap: 200                     # Character overlap between chunks (integer value)
      codeBlocks: true                      # Preserve code blocks - Options: [true, false]
      referenceLinks: true                  # Preserve reference links - Options: [true, false]
      headingHierarchy: true                # Maintain heading structure - Options: [true, false]
      joinTableRows: true                   # Keep table rows together - Options: [true, false]

    # Option 3: Token Text Splitter (use when strategy is "tokenTextSplitter")
    tokenSplitterConfig:
      chunkSize: 1000                       # Maximum tokens per chunk (integer value)
      chunkOverlap: 200                     # Token overlap between chunks (integer value)
      modelName: "gpt-3.5-turbo"            # Model name for tokenization (string value, e.g., "gpt-4", "gpt-3.5-turbo")
      encodingName: "cl100k_base"           # Encoding name (string value, e.g., "cl100k_base", "p50k_base")
      allowedSpecial: []                    # Special tokens to allow (array of strings, e.g., ["<|endoftext|>"])
      disallowedSpecial: ["all"]            # Special tokens to disallow (array of strings, "all" or specific tokens)

  # AWS Credentials
  awsSecret: aws-secret                     # Name of the Kubernetes secret with AWS credentials
```

## Verification

After applying the UnstructuredDataProduct resource, verify that processing is working:

### 1. Check Files in Storage Bucket

List all files in the storage bucket:

```bash
awslocal s3 ls s3://data-storage-bucket/testunstructureddataproduct/ --recursive
```

**Expected Output:**
You should see both the original PDF and its metadata file:
```
2026-02-06 10:30:15      12345 testunstructureddataproduct/sample.pdf
2026-02-06 10:30:16       2456 testunstructureddataproduct/sample.pdf-metadata.json
```

### 2. Check Local Cache Directory

Verify files are cached locally:

```bash
ls -la tmp/cache/testunstructureddataproduct/
```

**Expected Output:**
You should see the same files in your local cache:
```
sample.pdf
sample.pdf-metadata.json
```

### 3. Check Controller Logs

In the terminal where you ran `make run`, you should see logs indicating:
- Files being detected in the ingestion bucket
- Processing starting
- Successful uploads to the storage bucket

### 4. Inspect Metadata File

Download and view the metadata to see processing results:

```bash
awslocal s3 cp s3://data-storage-bucket/testunstructureddataproduct/sample.pdf-metadata.json - | cat
```

## Cleanup

To stop and clean up your local environment:

1. **Delete the UnstructuredDataProduct resource:**
   ```bash
   kubectl delete -f config/samples/operator_v1alpha1_unstructureddataproduct.yaml
   ```

2. **Stop the controller:**
   Press `Ctrl+C` in the terminal running `make run`

3. **Stop LocalStack:**
   Press `Ctrl+C` in the terminal running LocalStack

4. **Delete the kind cluster (optional):**
   ```bash
   kind delete cluster
   ```

5. **Clean up local cache (optional):**
   ```bash
   rm -rf tmp/cache/*
   ```
