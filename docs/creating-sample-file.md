# Unstructured Data Controller - Quick Start Guide

Simple guide to get the first file processed by the unstructured controller.

## What It Does

The controller automatically processes unstructured files through a pipeline of stages:
1. **Crawls** files from an S3 bucket
2. **Converts** them to Markdown using Docling
3. **Chunks** the content for better processing
4. **Generates embeddings** using a language model
5. **Syncs** the results to a destination S3 bucket

---

## Prerequisites

- Have local controller running using [Unstructured Data Controller with LocalStack](setup-unstructured-controller.md)

---

## Test It

### Upload a File

```bash
# Upload to S3 source bucket
awslocal s3 cp test.pdf s3://data-ingestion-bucket/documents/

# The pipeline will automatically:
# 1. Crawl the file from S3
# 2. Convert it to Markdown
# 3. Chunk the content
# 4. Generate embeddings
# 5. Sync to destination S3
```

### Monitor Progress

```bash
# Check pipeline and stage statuses
kubectl get unstructureddatapipeline -n unstructured-controller-namespace
kubectl get sourcecrawler -n unstructured-controller-namespace
kubectl get documentprocessor -n unstructured-controller-namespace
kubectl get chunksgenerator -n unstructured-controller-namespace
kubectl get vectorembeddingsgenerator -n unstructured-controller-namespace
kubectl get destinationsyncer -n unstructured-controller-namespace

# View controller logs: the controller runs in the terminal where you started
# it with `make run` (see the setup guide), so its output appears there
```

---

## Expected Output

Each stage writes to its own directory in the filestore:

```
pipelines/<pipeline-name>/stages/
  crawl/
    file.pdf                    # raw file from source
    file.pdf.json               # metadata sidecar (ETag-based change tracking)
  convert/
    file.pdf.json               # Markdown conversion
  chunk/
    file.pdf.json               # chunked content
  embed/
    file.pdf.json               # vector embeddings
```

Destination S3 mirrors this structure at:
```
s3://output-bucket/processed/stages/embed/file.pdf.json
```
