# Vector Embeddings Generation Controller

A Kubernetes operator that automates the generation of vector embeddings from processed documents stored in S3 buckets. This controller reads chunked documents from S3, generates embeddings using AI models, and stores the embedded vectors back to S3 with local caching.

## What Does This Do?

This controller processes already chunked documents from S3 and automatically:
- Reads processed document chunks from S3 storage bucket
- Generates vector embeddings using AI models (self-hosted or Gemini)
- Supports both self-hosted embedding models and Google's Gemini API
- Stores embeddings along with metadata in S3
- Maintains a local cache for faster access
- Handles batch processing efficiently

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
- **Embedding Model API** - Access to a self-hosted embedding model API or Google Gemini API

## Local Development Setup

Follow these steps to set up and test the Vector Embeddings Generation Controller locally.

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

This registers the `VectorEmbeddingsGenerationJob` custom resource type with your Kubernetes cluster.

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

### Step 4: Create S3 Bucket

Open a new terminal and create the S3 storage bucket in LocalStack (this is where processed documents are stored):

```bash
awslocal s3 mb s3://data-storage-bucket
```

**Important:** This bucket should already contain processed documents with `-chunks.json` files from the Chunks Generator Controller. The embedding controller specifically looks for files ending with `-chunks.json` to generate embeddings from.

**Example of required files in data-storage-bucket:**
```
testunstructureddataproduct/sample.pdf-chunks.json
```

If you don't have these files yet, populate data-ingestion-bucket with the chunks files.

### Step 5: Configure Local Cache Directory

Create a local directory for caching embeddings:

```bash
mkdir -p tmp/cache
```

**Important:** Update the cache directory path in the controller code:

1. Open `internal/controller/vectorembeddingsgenerationjob_controller.go`
2. Find line 46 (the `cacheDirectory` variable)
3. Replace the path with the absolute path to your `tmp/cache` directory:
   ```go
   cacheDirectory = "/absolute/path/to/your/project/tmp/cache/"
   ```

**Example:**
```go
cacheDirectory = "/Users/yourname/unstructured-data-controller/tmp/cache/"
```

### Step 6: Create AWS Credentials Secret

Create a Kubernetes secret with AWS credentials for LocalStack:

1. **Verify the endpoint in `config/samples/aws-secret.yaml`:**
   ```bash
   cat config/samples/aws-secret.yaml
   ```

   Ensure the file contains:
   ```yaml
   AWS_ENDPOINT: http://localhost:4566
   ```

2. **Apply the secret:**
   ```bash
   kubectl apply -f config/samples/aws-secret.yaml
   ```

This creates a secret named `aws-secret` that the controller uses to access S3.

### Step 7: Create Embedding API Secret

Create a Kubernetes secret with your embedding model API key:

1. **Edit `config/samples/embedding-api-secret.yaml`:**
   ```bash
   cat config/samples/embedding-api-secret.yaml
   ```

   Replace `{API_KEY}` with your actual API key:
   ```yaml
   apiVersion: v1
   kind: Secret
   metadata:
     name: embedding-api-secret
   type: Opaque
   stringData:
     apiKey: your-actual-api-key-here
   ```

2. **Apply the secret:**
   ```bash
   kubectl apply -f config/samples/embedding-api-secret.yaml
   ```

This creates a secret named `embedding-api-secret` that the controller uses to authenticate with the embedding model API.

### Step 8: Prepare TLS Certificate (for self-hosted models)

If using a self-hosted embedding model with TLS, ensure you have the certificate file:

```bash
# Place your certificate file in the project root
cp /path/to/your/certificate.pem certificate.pem
```

**Note:** The certificate path in the CR should match the actual file location.

### Step 9: Run the Operator

Start the controller in development mode:

```bash
make run
```

The controller will start running and processing embedding generation jobs. Keep this terminal open to view logs.

### Step 10: Create a VectorEmbeddingsGenerationJob Resource

Apply the sample custom resource to start generating embeddings:

1. **Edit the sample CR if needed:**
   ```bash
   cat config/samples/operator_v1alpha1_vectorembeddingsgenerationjob.yaml
   ```

   Update the following fields:
   - `dataProduct`: Name of the data product to process (should match the prefix used in Unstructured Data Product Controller)
   - `endpoint`: Your embedding model API endpoint URL

2. **Apply the CR:**
   ```bash
   kubectl apply -f config/samples/operator_v1alpha1_vectorembeddingsgenerationjob.yaml
   ```

## Understanding the VectorEmbeddingsGenerationJob Configuration

The `VectorEmbeddingsGenerationJob` custom resource defines how embeddings should be generated. Here's a breakdown of the configuration fields:

### Sample Configuration

```yaml
apiVersion: operator.dataverse.redhat.com/v1alpha1
kind: VectorEmbeddingsGenerationJob
metadata:
  name: vectorembeddingsgenerationjob-sample
spec:
  # Data Product Configuration
  dataProduct: "testunstructureddataproduct"   # Name of the data product to process (must match S3 prefix)

  # Secret References
  apiKey: "embedding-api-secret"               # Name of the Kubernetes secret containing the API key
  awsSecret: "aws-secret"                      # Name of the Kubernetes secret containing AWS credentials

  # Embedding Generator Configuration
  embeddingGeneratorConfig:
    provider: "self-hosted-model"              # Options: ["self-hosted-model", "gemini-model"]

    # Option 1: Self-Hosted Model Configuration (use when provider is "self-hosted-model")
    selfHostedModelConfig:
      model: "nomic-ai/nomic-embed-text-v1.5"  # Model name - Options: ["nomic-ai/nomic-embed-text-v1.5", "sentence-transformers/all-MiniLM-L6-v2", "BAAI/bge-large-en-v1.5", or any custom model]
      dimensions: 1024                         # Embedding dimensions (integer value, e.g., 768, 1024, 1536)
      encodingformat: "float"                  # Encoding format - Options: ["float", "base64"]
      apikeysecretref: "apiKey"                # Key name in the secret that contains the API key
      endpoint: "https://api.example.com/v1/embeddings"  # Endpoint URL for the embedding API
      certificatepath: "certificate.pem"       # Path to the TLS certificate for secure communication

    # Option 2: Gemini Model Configuration (use when provider is "gemini-model")
    geminiModelConfig:
      # Configuration will be available in future releases
```

## Verification

After applying the VectorEmbeddingsGenerationJob resource, verify that embedding generation is working:

### 1. Check Controller Logs

In the terminal where you ran `make run`, you should see logs indicating:
- Reading chunks from the storage bucket
- Generating embeddings in batches
- Successful uploads of embeddings to S3

### 2. Check Files in Storage Bucket

List all files in the storage bucket to see the generated embeddings:

```bash
awslocal s3 ls s3://data-storage-bucket/testunstructureddataproduct/ --recursive
```

**Expected Output:**
You should see embedding files alongside the document chunks:
```
2026-02-06 10:30:20      45678 testunstructureddataproduct/sample.pdf-chunks.json
2026-02-06 10:30:21      45789 testunstructureddataproduct/sample.pdf-embeddings.json
```

### 3. Check Local Cache Directory

Verify embedding files are cached locally:

```bash
ls -la tmp/cache/testunstructureddataproduct/
```

**Expected Output:**
You should see the embedding files in your local cache.

### 4. Inspect Embedding File

Download and view an embedding file to see the generated vectors:

```bash
awslocal s3 cp s3://data-storage-bucket/testunstructureddataproduct/embeddings/sample.pdf-embeddings.json``` - | cat
```

### 5. Check Job Status

Check the status of your embedding generation job:

```bash
kubectl get vectorembeddingsgenerationjob vectorembeddingsgenerationjob-sample
```

**Expected Output:**
```
NAME                                      STATUS   MESSAGE
vectorembeddingsgenerationjob-sample      True     Successfully generated embeddings
```

## Cleanup

To stop and clean up your local environment:

1. **Delete the VectorEmbeddingsGenerationJob resource:**
   ```bash
   kubectl delete -f config/samples/operator_v1alpha1_vectorembeddingsgenerationjob.yaml
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

