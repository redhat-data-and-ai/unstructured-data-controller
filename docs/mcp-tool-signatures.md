# MCP Tool Signatures

## Overview

Server endpoint: `<MCP_SERVER_URL>/mcp`

Authentication: OAuth 2.1 with PKCE. The authorization server is discovered via
`GET <MCP_SERVER_URL>/.well-known/oauth-protected-resource`.

Access is row-level. Each tool returns only the data the authenticated user is
permitted to see in Snowflake.

---

## Call order

```
list_unstructured_data_pipelines_for_user   <- get pipeline name
         |
get_chunks_for_embeddings                   <- search by query
         |  (optional)
get_processed_document                      <- full markdown document
         |  (optional)
get_citation_image                          <- cropped page image for a chunk
```

---

## Tool 1: `list_unstructured_data_pipelines_for_user`

### Request

No parameters.

### Response

| Field | Type | Description |
|-------|------|-------------|
| `name` | string | Pipeline identifier. Used in all other tools. |
| `description` | string | What the pipeline contains. |
| `guidance` | string (optional) | Instructions specific to this pipeline. Follow them when present. |

---

## Tool 2: `get_chunks_for_embeddings`

### Request

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `pipeline_name` | string | yes | From `list_unstructured_data_pipelines_for_user` |
| `query` | string | yes | Search query |
| `file_path_filter` | string | no | Glob pattern to scope the search to specific files. `*` matches any path segment. Example: `*/README.md`, `repo/docs/*` |
| `limit` | integer | no | Number of chunks to return. Default: 10. Max: 100. Values above 100 are capped. |

### Response

| Field | Type | Description |
|-------|------|-------------|
| `file_id` | string | Source document identifier. Used in `get_processed_document` and `get_citation_image`. |
| `chunk_id` | string | Chunk identifier. Required by `get_citation_image`. |
| `chunk_index` | string | Position of the chunk in the document. |
| `chunk_text` | string | Text content of the chunk, may include heading context. |
| `raw_text` | string (optional) | Chunk body without heading context, for display. |
| `score` | string | Cosine similarity score. |
| `reranker_score` | number (optional) | Cross-encoder score. Present only when a reranker is configured. |
| `block_ids` | array (optional) | Document element identifiers the chunk came from. |
| `regions` | array (optional) | Bounding box of the cited region on the page. Values are 0 to 1, from top-left. |

Chunks are returned in relevance order, best first. Do not re-sort by `score` or `reranker_score`.

---

## Tool 3: `get_processed_document`

### Request

Provide either `file_id` or `file_path_filter`. At least one is required. If both are given, `file_id` takes priority.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `pipeline_name` | string | yes | From `list_unstructured_data_pipelines_for_user` |
| `file_id` | string | one of | Exact file identifier. Take from the `file_id` field in a chunk returned by `get_chunks_for_embeddings`. |
| `file_path_filter` | string | one of | Glob pattern. `*` matches any path segment. Example: `*/README.md`, `*/aemv2/*` |

### Response

| Field | Type | Description |
|-------|------|-------------|
| `file_id` | string | Document identifier. |
| `markdown_content` | string | Full markdown text of the document. |

When `file_path_filter` matches more than 20 documents, only `file_id` values are returned with no content. Narrow the pattern or switch to `file_id`.

---

## Tool 4: `get_citation_image`

### Request

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `pipeline_name` | string | yes | From `list_unstructured_data_pipelines_for_user` |
| `chunk_id` | string | yes | From the `chunk_id` field in a chunk returned by `get_chunks_for_embeddings` |

### Response

| Field | Type | Description |
|-------|------|-------------|
| `summary` | string | Chunk ID, source file, and page numbers covered. |
| `image` | PNG (one per page) | Cropped to the cited region. MIME type: `image/png`. |

One image per page the chunk spans. A chunk that crosses a page break returns two images.

If the source file has no pages (for example, spreadsheets or web pages), only the summary text is returned.

---

## Connection scoping

To restrict a connection to specific pipelines, append `?pipelines=` to the endpoint URL:

```
<MCP_SERVER_URL>/mcp?pipelines=pipeline-a,pipeline-b
```

`list_unstructured_data_pipelines_for_user` will only return the listed pipelines, and all other tools will reject requests for any pipeline not in the list.
