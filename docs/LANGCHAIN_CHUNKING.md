# LangChain Chunking Strategies

## Overview

The project implements three chunking strategies, all powered by the [langchaingo](https://github.com/tmc/langchaingo) `textsplitter` package. The chunking layer is defined across several files:

- **CRD types**: `api/v1alpha1/unstructureddataproduct_types.go` — the `ChunksGeneratorConfig` struct and strategy-specific config structs
- **Chunker interface + implementations**: `pkg/unstructured/chunking.go` — thin wrappers around the LangChain client
- **LangChain client**: `pkg/langchain/client.go` — the actual text splitting with semaphore concurrency control
- **Controller logic**: `internal/controller/chunksgenerator_controller.go` — strategy selection and option wiring

## The Three Strategies

### 1. `recursiveCharacterTextSplitter`

Recursively splits text using a list of character separators, trying each separator in order until chunks are small enough.

| Option | Type | Description |
|--------|------|-------------|
| `separators` | `[]string` | Ordered list of separator strings to try (e.g., `["\n\n", "\n", " ", ""]`) |
| `chunkSize` | `int` | Target maximum size of each chunk in characters |
| `chunkOverlap` | `int` | Number of characters to overlap between consecutive chunks |
| `keepSeparator` | `bool` | Whether to keep the separator character in the resulting chunks |

**Best for**: General-purpose text where you want control over splitting boundaries. The recursive approach tries larger separators first (paragraphs, then lines, then words) and falls back to smaller ones.

### 2. `markdownTextSplitter`

A Markdown-aware splitter that understands document structure (headings, code blocks, tables) and splits accordingly.

| Option | Type | Description |
|--------|------|-------------|
| `chunkSize` | `int` | Target maximum chunk size |
| `chunkOverlap` | `int` | Overlap between chunks |
| `codeBlocks` | `bool` | Preserve code blocks as intact units |
| `referenceLinks` | `bool` | Handle Markdown reference-style links |
| `headingHierarchy` | `bool` | Respect heading hierarchy — prepend parent headings to child sections so each chunk retains its context |
| `joinTableRows` | `bool` | Keep table rows together rather than splitting mid-table |

**Best for**: Documents converted to Markdown by Docling. Since the DocumentProcessor outputs Markdown, this is the most natural fit — it preserves semantic structure like headings, code fences, and tables.

### 3. `tokenTextSplitter`

Splits text based on token count using model-specific tokenizers rather than character count.

| Option | Type | Description |
|--------|------|-------------|
| `chunkSize` | `int` | Target maximum chunk size in tokens |
| `chunkOverlap` | `int` | Overlap in tokens |
| `modelName` | `string` | Model name for tokenization (e.g., `gpt-4`) — determines the tokenizer used |
| `encodingName` | `string` | Explicit token encoding name (e.g., `cl100k_base`) — alternative to modelName |
| `allowedSpecial` | `[]string` | Special tokens that are allowed in the output |
| `disallowedSpecial` | `[]string` | Special tokens that should be rejected |

**Best for**: When you need chunks sized by the target LLM's token limits rather than character count. Ensures chunks fit within model context windows.

## How Chunking Fits in the Pipeline

1. The `ChunksGenerator` controller reads `-converted.json` files from the filestore (output of DocumentProcessor/Docling)
2. It checks whether re-chunking is needed by comparing the current `ChunksGeneratorConfig` against the metadata stored in existing `-chunks.json` files
3. If chunking is needed, it selects the appropriate `Chunker` implementation based on `spec.config.strategy`
4. The chunker splits `convertedFile.ConvertedDocument.Content.Markdown` — the Markdown text output from Docling
5. Results are stored in a `ChunksFile` struct that includes both the original converted document and the new chunks, along with full metadata for change detection

## Concurrency Control

All three strategies go through the LangChain client's semaphore. If `maxConcurrentLangchainTasks` is reached, `TryAcquire` returns false, the error is caught as a `SemaphoreAcquireError`, and the file is added to `skippedFiles` — the controller requeues rather than blocking.
