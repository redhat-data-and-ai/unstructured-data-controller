# MCP Server

An OAuth-secured [Model Context Protocol](https://modelcontextprotocol.io) server that exposes the Unstructured Data Controller's capabilities as tools for LLM agents. Built with the [official MCP Go SDK](https://github.com/modelcontextprotocol/go-sdk) using the Streamable HTTP transport.

## Architecture

```
MCP Client (Cursor, Claude Code, custom agent)
    │
    ├─ GET  /.well-known/oauth-protected-resource   ← discover auth requirements
    ├─ GET  /.well-known/oauth-authorization-server  ← discover auth endpoints
    ├─ POST /auth/register                           ← dynamic client registration
    ├─ GET  /auth/authorize                          ← start OAuth flow → redirects to SSO
    ├─ GET  /auth/callback/oidc                      ← SSO callback → redirects to completion page
    ├─ GET  /auth/complete/{token}                   ← completion page → redirects to client
    ├─ POST /auth/token                              ← exchange code / refresh token
    │
    └─ POST /mcp  ←── Bearer token ──→  MCP protocol (tools, resources, prompts)
```

The server acts as an **OAuth Authorization Server proxy**: clients register and authorize through the MCP server, which redirects to the external SSO for actual user authentication, then proxies tokens back.

## Prerequisites

- Go 1.25+
- An OAuth 2.0 / OIDC identity provider (Keycloak, Red Hat SSO, Okta, Azure AD, etc.)

## Configuration

All configuration is via environment variables.

### Required

| Variable                | Description                                                                                                  |
| ----------------------- | ------------------------------------------------------------------------------------------------------------ |
| `SSO_CLIENT_ID`         | OAuth client ID registered with your SSO provider                                                            |
| `SSO_CLIENT_SECRET`     | OAuth client secret                                                                                          |
| `SSO_AUTHORIZATION_URL` | SSO authorization endpoint (e.g. `https://sso.example.com/auth/realms/myrealm/protocol/openid-connect/auth`) |
| `SSO_TOKEN_URL`         | SSO token endpoint (e.g. `https://sso.example.com/auth/realms/myrealm/protocol/openid-connect/token`)        |
| `SSO_INTROSPECTION_URL` | SSO token introspection endpoint (RFC 7662). Required unless `SSO_TOKEN_VALIDATION=jwks` or `SSO_DISABLE_INTROSPECTION=true` (development only) |
| `SSO_CALLBACK_URL`      | Callback URL pointing to this server's `/auth/callback/oidc` endpoint                                        |
| `EMBEDDING_ENDPOINT`   | URL of the embedding service (e.g. `https://host/v1/embeddings`)                                             |
| `EMBEDDING_API_KEY`    | API key for the embedding service                                                                            |
| `EMBEDDING_MODEL_NAME` | Model name to use for generating embeddings                                                                  |
| `SNOWFLAKE_ACCOUNT`    | Snowflake account identifier                                                                                 |

### Optional

| Variable                                 | Default                             | Description                                                                                                                          |
| ---------------------------------------- | ----------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ |
| `MCP_SERVER_PORT`                        | `8080`                              | Port the server listens on                                                                                                           |
| `UNSTRUCTURED_DATA_CONTROLLER_NAMESPACE` | `unstructured-controller-namespace` | Kubernetes namespace to list pipelines from. Injected from the pod's namespace when deployed in-cluster; set it when running locally. |
| `KUBECONFIG`                             | `~/.kube/config`                    | Path to kubeconfig file (only used outside a cluster)                                                                                |
| `MCP_PUBLIC_URL`                         | _(unset)_                           | Public base URL of the server (e.g. `https://mcp.example.com`). Set when running behind a proxy or load balancer so OAuth discovery metadata uses a stable URL instead of request headers. |
| `SSO_DISABLE_INTROSPECTION`              | `false`                             | **Development only.** When `true`, skips SSO introspection and validates tokens locally without signature verification. Never enable in production. Cannot be combined with `SSO_TOKEN_VALIDATION=jwks`. |
| `SSO_TOKEN_VALIDATION`                   | `introspect`                        | How a Bearer token is checked. `introspect` asks the provider about every token. `jwks` verifies the token's RS256 signature against the issuer's published keys instead, for a provider whose introspection endpoint is unavailable. See [Token validation](#token-validation). |
| `SSO_JWKS_URL`                           | _(unset)_                           | The issuer's JSON Web Key Set, e.g. `https://sso.example.com/auth/realms/myrealm/protocol/openid-connect/certs`. Required when `SSO_TOKEN_VALIDATION=jwks`. Must be `https` unless it points at loopback. |
| `SSO_ISSUER`                             | _(unset)_                           | The exact `iss` every accepted token must carry, e.g. `https://sso.example.com/auth/realms/myrealm`. Required when `SSO_TOKEN_VALIDATION=jwks`. |
| `ENABLE_RERANKER`                        | `false`                             | Set to `true` to enable cross-encoder reranking. Reranking is governed by this flag alone, never by whether the variables below happen to be set. |
| `RERANKER_ENDPOINT`                      | _(unset)_                           | Full URL of a cross-encoder rerank endpoint, including the path (e.g. `https://host/v1/rerank`). Must return `{"results":[{"index":N,"relevance_score":F}]}`; TEI endpoints, which return a bare array, are not supported. Required when `ENABLE_RERANKER=true`. |
| `RERANKER_MODEL_NAME`                    | _(unset)_                           | Reranker model name. Required when `ENABLE_RERANKER=true`.                                                                           |
| `RERANKER_API_KEY`                       | _(unset)_                           | API key for the reranker service, if it requires one. Sent as a bearer token, so prefer an `https://` endpoint.                      |
| `RERANKER_CANDIDATE_POOL`                | `50`                                | Candidates sent to the reranker per query. Must be between 1 and 100. An invalid value fails startup rather than falling back.       |

### Token validation

Every request carries a Bearer token, and `SSO_TOKEN_VALIDATION` decides how the server
satisfies itself that the token is genuine.

`introspect` is the default and the right answer for production. The server asks the
identity provider about every token, so a token revoked before it expires stops working
immediately.

`jwks` exists for providers whose introspection endpoint is not usable. Some deployments
authenticate the client on `/token/introspect` and then answer `{"active":false}` for a
token they issued seconds earlier, which leaves the server rejecting everything. In this
mode the server verifies the token's RS256 signature against the issuer's published keys
instead:

```sh
SSO_TOKEN_VALIDATION=jwks
SSO_JWKS_URL=https://sso.example.com/auth/realms/myrealm/protocol/openid-connect/certs
SSO_ISSUER=https://sso.example.com/auth/realms/myrealm
```

It checks the signature, that `exp` is present and in the future, and that `iss` matches
`SSO_ISSUER` exactly. Only RS256 is accepted, so a token presenting `alg: none`, or signed
with HS256 using the public key as the secret, is refused. Signing keys are cached, and
refetched both when a token names an unknown `kid` and once they pass a 15 minute TTL, so a
key the issuer withdraws stops working without a restart.

The trade-off is that this is an offline check. A token revoked before it expires keeps
working until it does, and the server logs a warning at startup saying so. Prefer
`introspect` wherever it works.

`SSO_TOKEN_VALIDATION=jwks` cannot be combined with `SSO_DISABLE_INTROSPECTION=true`, and
the server refuses to start if both are set. The older flag takes precedence internally, so
the combination would leave signatures unchecked while the startup log claimed otherwise.

### Reranking

With `ENABLE_RERANKER=true`, `get_chunks_for_embeddings` over-fetches candidates from
Snowflake and re-scores them against the query with a cross-encoder, returning the best ones
in relevance order. Matching chunks then carry a `reranker_score` field alongside the
original cosine `score`.

**Enabling is explicit, and misconfiguration fails fast.** Reranking is off unless
`ENABLE_RERANKER=true`. When it is on, `RERANKER_ENDPOINT` and `RERANKER_MODEL_NAME` are
required and an invalid `RERANKER_CANDIDATE_POOL` is rejected, so the server refuses to start
rather than running silently in a configuration nobody intended. The startup log states which
mode is active.

**At runtime, reranking is best-effort.** If the reranker is unreachable, times out, or
returns a response in an unsupported shape, the tool logs the failure and returns the
cosine-ranked results instead. A reranker outage degrades retrieval quality; it does not fail
the query.

**Endpoint compatibility.** The client sends `{"model", "query", "documents", "top_n",
"return_documents": false}` and expects an object with a `results` array whose entries carry
`index` and `relevance_score`. Cohere, Jina and vLLM rerank endpoints match this. Native TEI
`/rerank` returns a bare array and is **not** supported: configuring one yields a decode
error and cosine fallback on every query.

**Candidate pool depth.** `RERANKER_CANDIDATE_POOL` sets how many candidates the
cross-encoder re-scores per query, independently of how many chunks the caller asked for.
Depth is the setting that matters most: published ablation on table-heavy corpora puts
recall@5 at 0.458 for a pool of 20, 0.826 for 50 and 0.888 for 100, with reranking doing
nothing below about 20. The default of 50 is the conservative point on that curve. Raising
it to 100 is cheap on the Snowflake side, since the query has no vector index and scans
regardless, but it increases reranker latency proportionally. Benchmark before raising it:
some cross-encoders score a query and document pair autoregressively and can exceed a
second per query at depth.


The Snowflake connection uses the `PUBLIC` role by default to avoid defaulting to an overprivileged role.

### Example `.env`

```bash
SSO_CLIENT_ID=unstructured-data-mcp-server
SSO_CLIENT_SECRET=your-client-secret
SSO_AUTHORIZATION_URL=https://sso.example.com/auth/realms/myrealm/protocol/openid-connect/auth
SSO_TOKEN_URL=https://sso.example.com/auth/realms/myrealm/protocol/openid-connect/token
SSO_INTROSPECTION_URL=https://sso.example.com/auth/realms/myrealm/protocol/openid-connect/token/introspect
SSO_CALLBACK_URL=http://localhost:8080/auth/callback/oidc
SNOWFLAKE_ACCOUNT=your-account
EMBEDDING_ENDPOINT=https://your-embedding-service/v1/embeddings
EMBEDDING_API_KEY=your-api-key
EMBEDDING_MODEL_NAME=your-model-name

# Optional: reranking. Off unless ENABLE_RERANKER=true, and when on the
# endpoint and model name are required.
ENABLE_RERANKER=true
RERANKER_ENDPOINT=https://your-reranker-service/v1/rerank
RERANKER_MODEL_NAME=your-reranker-model-name
RERANKER_API_KEY=your-api-key
RERANKER_CANDIDATE_POOL=50
```

## Running

### In-cluster

When deployed inside Kubernetes, the server automatically uses the pod's service account for cluster access. No `KUBECONFIG` is needed.

### Local development

The server detects it is running outside a cluster and falls back to your local kubeconfig (`KUBECONFIG` env var or `~/.kube/config`). Point your context at a Kind or Minikube cluster with the CRDs installed.

```bash
# Build
go build -o unstructured-data-mcp-server ./cmd/unstructured-data-mcp-server/

# Run (ensure .env is sourced or variables are exported)
./unstructured-data-mcp-server
```

The server starts on `:8080` (or `MCP_SERVER_PORT`) and logs JSON to stdout.

## Client Configuration

### Cursor

Add to `~/.cursor/mcp.json`:

```json
{
  "mcpServers": {
    "unstructured-data-controller": {
      "url": "http://localhost:8080/mcp/"
    }
  }
}
```

Cursor will automatically discover the OAuth flow via the well-known endpoints, prompt you to log in via your SSO provider, and manage tokens.

### Claude Code

Add to your Claude Code MCP settings:

```json
{
  "mcpServers": {
    "unstructured-data-controller": {
      "url": "http://localhost:8080/mcp/"
    }
  }
}
```

### Static Token (any client)

If your client does not support OAuth discovery, you can manually obtain a token and pass it as a header:

```json
{
  "mcpServers": {
    "unstructured-data-controller": {
      "url": "http://localhost:8080/mcp/",
      "headers": {
        "Authorization": "Bearer <your-access-token>"
      }
    }
  }
}
```

### Pipeline allow-list (`?pipelines=`)

The `/mcp` endpoint accepts an optional `pipelines` query parameter with a comma-separated list of pipeline names:

```
http://localhost:8080/mcp?pipelines=my-pipeline,other-pipeline
```

When set, `list_unstructured_data_pipelines_for_user` only returns the listed pipelines for that connection. When omitted, all pipelines the user can access are returned. The allow-list applies per connection, so different clients can be scoped to different pipelines.

## OAuth Flow

The full authorization flow follows the [MCP Authorization specification (2025-11-25)](https://modelcontextprotocol.io/specification/2025-11-25):

1. **Client → `GET /.well-known/oauth-protected-resource`** — discovers that auth is required and which authorization server to use (RFC 9728)
2. **Client → `GET /.well-known/oauth-authorization-server`** — discovers available endpoints: authorize, token, register (RFC 8414)
3. **Client → `POST /auth/register`** — dynamically registers itself, receives `client_id` and `client_secret` (RFC 7591)
4. **Client → `GET /auth/authorize`** — starts authorization code flow with PKCE (S256)
5. **Server → redirects to SSO** — user authenticates with the external identity provider
6. **SSO → `GET /auth/callback/oidc`** — server receives SSO callback, exchanges code for token
7. **Server → redirects browser to `GET /auth/complete/{token}`**: a clean completion URL (no OAuth parameters); the page then redirects to the client with the authorization code and state
8. **Client → `POST /auth/token`** — exchanges authorization code + PKCE verifier for access token
9. **Client → `POST /mcp`** — uses Bearer token for all subsequent MCP requests
10. **Token refresh** — client sends `grant_type=refresh_token` to `/auth/token` when the access token expires

## Endpoints

| Method                  | Path                                      | Auth         | Description                              |
| ----------------------- | ----------------------------------------- | ------------ | ---------------------------------------- |
| `POST`, `GET`, `DELETE` | `/mcp`                                    | Bearer token | MCP Streamable HTTP protocol             |
| `GET`                   | `/.well-known/oauth-protected-resource`   | None         | Protected resource metadata (RFC 9728)   |
| `GET`                   | `/.well-known/oauth-authorization-server` | None         | Authorization server metadata (RFC 8414) |
| `POST`                  | `/auth/register`                          | None         | Dynamic client registration (RFC 7591)   |
| `GET`                   | `/auth/authorize`                         | None         | Authorization endpoint (RFC 6749)        |
| `GET`                   | `/auth/callback/oidc`                     | None         | SSO callback handler                     |
| `GET`                   | `/auth/complete/{token}`                  | None         | Post-authentication completion page      |
| `POST`                  | `/auth/token`                             | None         | Token endpoint (code exchange, refresh)  |
| `GET`                   | `/healthz`                                | None         | Liveness probe                           |
| `GET`                   | `/readyz`                                 | None         | Readiness probe                          |

## Tools

All five tools are reached over `POST <MCP_PUBLIC_URL>/mcp` with a Bearer token. Access is row-level: every tool runs its Snowflake queries as the authenticated user, so a caller only ever sees the rows their Snowflake grants and row access policies allow.

### Call order

```
list_unstructured_data_pipelines_for_user   <- get pipeline name
         |
get_chunks_for_embeddings                   <- search by query
         |  (optional)
get_processed_document                      <- full markdown document
         |  (optional)
get_citation_image                          <- highlighted or cropped page image for a chunk
```

### Result shape and errors

Every tool returns a normal MCP `CallToolResult`. The first content block is text: a one-line human summary followed by the JSON payload described under each tool below. `get_citation_image` appends one or more image blocks after that text.

Failures are returned as a tool result with `isError: true` and a plain-text message, not as a JSON-RPC protocol error. The tool descriptions instruct agents to report the message verbatim and stop, rather than retrying against other pipelines or chunk IDs. Conditions common to every tool:

| Condition                                        | Message                                                      |
| ------------------------------------------------ | ------------------------------------------------------------ |
| Required argument missing or empty               | `Error: ... are required. ...`                               |
| Pipeline not in the connection's `?pipelines=` allow-list | `Error: pipeline "<name>" is not accessible in this session.` |
| No OAuth token on the request context            | `Error: oauth token not found in context`                    |
| Pipeline cannot be resolved, or its Snowflake database / schema / table is unset | `Error: ... pipeline "<name>" ...`               |

### Glob patterns

`file_path_filter` takes a glob where `*` matches any sequence of characters, including `/`. It is translated to a SQL `LIKE` pattern against `FILE_ID`, with literal `%` and `_` escaped first. Examples: `*/README.md`, `repo-name/docs/*`, `*/aemv2/*`.

### `list_unstructured_data_pipelines_for_user`

Lists the UnstructuredDataPipeline custom resources the authenticated user can access (pipelines whose Snowflake database the user can query).

**Request:** no parameters (the OAuth token from the request context identifies the user).

**Response:** a JSON array of pipeline objects.

| Field         | Type              | Description                                                        |
| ------------- | ----------------- | ------------------------------------------------------------------ |
| `name`        | string            | Pipeline CR name. The value to pass as `pipeline_name` everywhere else. |
| `description` | string            | Human-readable summary of what the pipeline contains.              |
| `guidance`    | string (optional) | Usage guidance for agents. Omitted when not set; follow it when present. |

```json
[
  {
    "name": "product-docs",
    "description": "Public product documentation, PDFs and markdown.",
    "guidance": "Prefer the latest release notes when versions conflict."
  }
]
```

Pipelines are filtered server-side by the user's Snowflake database access, then by the connection's `?pipelines=` allow-list (see [Client Configuration](#client-configuration)). Two non-array outcomes:

- No pipelines exist in the cluster: `No pipelines are configured in this cluster.` (not an error).
- Pipelines exist but none are accessible: `Found N pipeline(s) but you do not have access to any of them. ...` with `isError: true`.

### `get_chunks_for_embeddings`

Searches for relevant text chunks in a data product using vector cosine similarity. Returns the top matching chunks for the given query (default 10, configurable via `limit`), ordered best first.

Each chunk carries a `score` field holding its cosine similarity. When [reranking](#reranking) is enabled, candidates are re-scored by a cross-encoder and each chunk also carries a `reranker_score`; the cosine `score` is kept rather than overwritten. Consume the returned order directly, since neither score is guaranteed to decrease monotonically down the list.

**Request:**

| Parameter          | Type    | Required | Description                                                                                                                                  |
| ------------------ | ------- | -------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `pipeline_name`    | string  | Yes      | Name of the UnstructuredDataPipeline. If not known, call `list_unstructured_data_pipelines_for_user` first and pick the matching pipeline.   |
| `query`            | string  | Yes      | The search query to find relevant chunks (vector similarity; natural-language queries work best).                                           |
| `file_path_filter` | string  | No       | Glob pattern scoping the search to matching files.                                                                                          |
| `limit`            | integer | No       | Number of chunks to return. Defaults to 10, capped at 100. A larger value returns 100 chunks and a note saying so, rather than an error.      |

**Response:** a JSON array of chunk objects, best first.

| Field            | Type              | Description                                                                                       |
| ---------------- | ----------------- | --------------------------------------------------------------------------------------------------- |
| `file_id`        | string            | Source document identifier. Pass to `get_processed_document`.                                      |
| `chunk_id`       | string            | Chunk identifier, `<file path>#<index>`. Pass to `get_citation_image`. The only identifier that survives the file being embedded again. |
| `chunk_index`    | string            | The chunk's position in the flattened view. Not an identifier; do not pass it as `chunk_id`.       |
| `chunk_text`     | string            | Text content of the chunk, including its heading trail.                                            |
| `raw_text`       | string (optional) | Chunk body without the heading trail, for display.                                                 |
| `score`          | string            | Cosine similarity, rendered as a string.                                                           |
| `reranker_score` | number (optional) | Cross-encoder score. Present only when reranking ran and scored that chunk.                        |
| `block_ids`      | array (optional)  | Document element identifiers the chunk came from, e.g. `["#/texts/59"]`.                           |
| `regions`        | array (optional)  | Where the chunk sits on the source pages. See below.                                               |

Each entry in `regions` is `{"pageNo": <int>, "bbox": [left, top, right, bottom], "image": "<key>"}`. Coordinates are normalised 0 to 1 from the top-left of the page. `image` locates the rendered page in the destination bucket and is empty for inputs that render no pages.

```json
[
  {
    "file_id": "product-docs/guide.pdf",
    "chunk_id": "product-docs/guide.pdf#12",
    "chunk_index": "12",
    "chunk_text": "## Install\nRun the operator with ...",
    "raw_text": "Run the operator with ...",
    "score": "0.8213",
    "reranker_score": 0.964,
    "block_ids": ["#/texts/59"],
    "regions": [{ "pageNo": 4, "bbox": [0.12, 0.31, 0.88, 0.47], "image": "pages/guide.pdf/4.png" }]
  }
]
```

The text block wrapping the array carries two notes when they apply: a `NOTE:` that `limit` was clamped to 100, and a `TIP:` naming `get_processed_document` as the follow-up call.

**Typical agent flow:**

1. User asks a question (no pipeline specified).
2. Agent calls `list_unstructured_data_pipelines_for_user` to get pipeline names and descriptions.
3. Agent matches the user's question to a pipeline based on its description.
4. If confident, agent calls `get_chunks_for_embeddings` with the resolved `pipeline_name`; the Snowflake database, schema, and table are resolved server-side from the pipeline.
5. If ambiguous (multiple pipelines could match), agent asks the user to clarify.

### `get_processed_document`

Retrieves the processed document output (full Markdown from the pipeline's DocumentProcessor stage). Returns up to 20 documents per call.

Use this when chunks from `get_chunks_for_embeddings` don't provide enough context (for example, to follow up with the `file_id` of the top matching chunk).

**Request:** `file_id` or `file_path_filter` is required; `file_id` wins when both are given.

| Parameter          | Type   | Required                              | Description                                                                                                 |
| ------------------ | ------ | ------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `pipeline_name`    | string | Yes                                   | Name of the UnstructuredDataPipeline. If not known, call `list_unstructured_data_pipelines_for_user` first. |
| `file_id`          | string | One of `file_id` / `file_path_filter` | The exact file identifier, taken from the `file_id` of a chunk. Takes priority over `file_path_filter`.     |
| `file_path_filter` | string | One of `file_id` / `file_path_filter` | Glob pattern matching the files to retrieve.                                                                |

**Response:** one document object for a `file_id` lookup, an array of them for a pattern.

| Field              | Type   | Description                      |
| ------------------ | ------ | ---------------------------------- |
| `file_id`          | string | Document identifier.             |
| `markdown_content` | string | Full markdown text of the document. |

```json
{ "file_id": "product-docs/guide.pdf", "markdown_content": "# Guide\n..." }
```

Pattern outcomes:

- No match: `No processed documents found matching "<pattern>" in pipeline "<name>".` (not an error).
- More than 20 matches: a JSON array of `file_id` strings only, with no content, and a message to narrow the pattern or switch to `file_id`.
- 1 to 20 matches: an array of full document objects.

### `get_citation_image`

Returns the source page imagery a chunk was taken from. `mode` picks between two shapes, and a response is one shape or the other, never both:

- `mode: "highlight"` (the default) returns the **full page with the cited regions highlighted**, so the chunk is readable in its surrounding context. One image per cited page, capped at 10 pages.
- `mode: "crop"` returns **only the cited regions, cropped** flush to each bounding box. One image per region, so a chunk covering several blocks on one page yields several images.

An unrecognised `mode` is rejected rather than treated as the default.

The highlight is a translucent yellow wash over each cited region inside a dotted black outline, with a thin white rule outside the outline so the box stays visible on a dark or coloured page. Everything is composited, not painted over, so the text under the wash stays legible.

**Request:**

| Parameter       | Type   | Required | Description                                                                                     |
| --------------- | ------ | -------- | ------------------------------------------------------------------------------------------------ |
| `pipeline_name` | string | Yes      | Name of the UnstructuredDataPipeline the chunk came from. The same value passed to `get_chunks_for_embeddings`. |
| `chunk_id`      | string | Yes      | The `chunk_id` of a chunk returned by `get_chunks_for_embeddings`. Must be `<file path>#<index>`; anything else is rejected as malformed. |
| `mode`          | string | No       | `"highlight"` or `"crop"`. Defaults to `"highlight"`, the full page with the regions marked. Use `"crop"` for the cited regions on their own. |

**Response:** a text block followed by the images.

| Content block | Type           | Description                                                                 |
| ------------- | -------------- | ----------------------------------------------------------------------------- |
| `summary`     | text           | Chunk ID, source `file_id`, and the page numbers covered. Notes any pages dropped by the 10-page cap or missing from the destination. |
| images        | `image/png`    | With `mode: "highlight"`, one full page per cited page with its regions marked, in page order. With `mode: "crop"`, one crop per cited region. |

Not every chunk has an image, and these cases return the summary text alone rather than an error:

- The source format renders no pages (spreadsheets, web pages), or the chunk predates page image storage.
- The page images are not in the pipeline's destination bucket.

Two pipeline settings have to be in place for a citation image to exist:

- The DocumentProcessor stage must have `include_page_images` on, which is the default for every `image_export_mode` other than `placeholder`.
- The DestinationSyncer stage must list the DocumentProcessor stage in its `dependsOn`. The image is read from the destination bucket with the pipeline's own credentials, so a syncer that only syncs `embed` leaves nothing to read.

A chunk that is absent and a chunk hidden from the caller by a row access policy both return `Error: no chunk with that chunk_id is available in this pipeline.` The Snowflake lookup is the authorization check and the only one, so the two are deliberately indistinguishable. A failed lookup (the warehouse being briefly unreachable) is reported differently and says it is worth retrying.

### `search_google_drive`

Finds files by name or exact wording in a pipeline's Google Drive folders, using Drive's own search. Returns file metadata and a link, never file contents.

Use it to locate a document the user named, or to find an exact term such as an error code. Use `get_chunks_for_embeddings` instead to answer a question from document content: that tool searches by meaning, this one matches words.

There is nothing to switch on. Any pipeline with a Google Drive crawler stage can be searched, provided that stage records its files in Snowflake and the server is configured for the tool (see below):

```yaml
- name: crawl
  type: SourceCrawler
  sourceCrawlerConfig:
    type: googleDrive
    googleDriveConfig:
      folders:
        - url: https://drive.google.com/drive/folders/<id>
  queryConfig:
    type: snowflake
    snowflake:
      account: my-account
      database: my_db
      schema: marts
      table: crawl_mv
```

The `snowflake` block is what makes the stage searchable rather than any marker. Drive says which files match, and the row access policy over that table decides which of them the caller may see, so without it the tool has no way to filter and refuses to answer.

Calling the tool on a pipeline with no Google Drive crawler returns an error naming that, rather than an empty result.

Only files the crawler has already indexed can be returned. Access is decided from the crawl table, so a file uploaded since the last crawl is matched by Drive, has no row yet, and is filtered out. It becomes searchable once the crawl picks it up.

**Request:**

| Parameter       | Type   | Required | Description                                                                                 |
| --------------- | ------ | -------- | --------------------------------------------------------------------------------------------- |
| `pipeline_name` | string | Yes      | Name of the UnstructuredDataPipeline. If not known, call `list_unstructured_data_pipelines_for_user` first. |
| `query`         | string | Yes      | Words to match. Whole word and case insensitive, so `report` also matches `Reports`. Wrap in double quotes for an exact phrase. No partial word matching: `repo` does not match `report`. |
| `limit`         | int    | No       | Files to return. Defaults to 10, capped at 100.                                             |

**Response:** an array of file objects.

| Field           | Type   | Description                                                      |
| --------------- | ------ | ------------------------------------------------------------------ |
| `file_id`       | string | Drive file identifier of the matched file.                       |
| `name`          | string | File name.                                                       |
| `mime_type`     | string | Drive MIME type.                                                 |
| `modified_time` | string | RFC 3339 timestamp of the last change.                           |
| `web_view_link` | string | Link to open the file in Drive.                                  |
| `download_url`  | string | Presigned link to the copy the crawler stored in S3, valid for 5 minutes and usable with no Google credential. Omitted if the link could not be signed. The link is not checked against S3 first, so a file removed from the bucket since the last crawl gives a URL that 404s. |

An empty result can mean the caller has no access rather than that nothing matched. The two are deliberately indistinguishable, since reporting how many files were hidden would confirm that matching files exist.

`file_id` is the same identifier `get_processed_document` and `get_citation_image` take, but only for files the pipeline has already crawled. Drive also matches files added since the last crawl, and those tools will not find them.

#### How results are filtered

The search runs as the pipeline's own Google service account, which can read the whole corpus. Drive is asked what matches; Snowflake is asked who may see it. The caller never authenticates to Google.

Access is decided by querying the crawl table as the caller, with their own Red Hat SSO token, so the row access policy over that table drops the files they may not read. That is the same gate every other tool in this server answers to, which is why search cannot be offered for a pipeline whose crawler stage has no `queryConfig.snowflake`. An error from that query is never read as "no results": a caller is shown nothing rather than something unchecked.

Because the decision comes from rows written at crawl time, a permission revoked in Drive keeps working until the next crawl refreshes them. The reverse holds too: a file uploaded since the last crawl is found by Drive, has no row yet, and so does not appear.

Results are confined to the folders the pipeline declares. Drive's `'<id>' in parents` matches direct children only, with no recursive operator, so the server expands each declared folder to its full subtree and names every folder in the query. Without that, a file one level down is invisible to search while still being indexed by the crawler. The expansion honours `skipFolders`, so search and crawl agree on the corpus, and it is cached per pipeline for ten minutes, which means a newly added folder is not searchable until the entry expires. A folder that was never shared with the service account is reported as an error rather than returning nothing, because listing such a folder answers with an empty list and would otherwise look like an empty corpus.

A query naming too many folders is refused rather than searched partially. Drive sends the query as a GET parameter and its frontend rejects a URL past roughly 16KB with an HTML 400 carrying no JSON error, which lands around 310 folder clauses. The folder count is capped below that, and the built query is checked against a byte limit as well, because the search term shares the same budget and a long term can overflow a legal tree.

Each result carries a short-lived presigned URL to the copy the crawler stored in the destination bucket, so the caller can fetch the file without any Google credential. The object key comes from the warehouse row's `FILE_PATH` rather than being reconstructed, and the link is signed only after the access check, so one is never minted for a file the caller may not read. Signing is offline, so the object is not checked for existence first.

Shortcuts are excluded from results entirely. A shortcut's ACL is inherited from the folder it sits in rather than from the file it points at, so approving a shortcut says nothing about whether the caller may read its target. Drive still finds the target directly when the caller can read it, so nothing is lost.

Paging gathers more candidates than the caller asked for, because access is decided afterwards and some will be dropped. Paging stops once enough are held, the pages run out, or a budget of 5 pages is reached. The response carries a note saying coverage was partial when the budget stops a short result or when Drive sets `incompleteSearch`. It never says how many files were withheld, because that count would confirm matching files exist.

#### Configuration

The tool registers only when introspection is on, and logs the reason when it is not:

| Setting                                    | Why it is required                                                                 |
| ------------------------------------------ | ------------------------------------------------------------------------------------ |
| `SSO_DISABLE_INTROSPECTION` unset or false | The caller's verified identity is what the Snowflake query runs as, so an unverified one would mean an unfiltered search. |

## Package Structure

```
cmd/unstructured-data-mcp-server/
  main.go                  ← entry point, wiring

internal/mcp/tools/
  list_pipelines.go         ← list_unstructured_data_pipelines_for_user tool
  get_chunks.go             ← get_chunks_for_embeddings tool
  get_processed_document.go ← get_processed_document tool
  get_citation_image.go     ← get_citation_image tool

pkg/auth/
  provider.go               ← Provider interface (extensible)
  provider_generic.go       ← Generic OIDC provider (Keycloak, Okta, Azure AD, etc.)
  oauth.go                  ← HTTP middleware (token validation + caching), metadata handlers
  server.go                 ← OAuth AS proxy endpoints (register, authorize, callback, complete, token)
  store.go                  ← In-memory OAuth client and authorization code storage
  pkce.go                   ← PKCE S256 validation (RFC 7636)
  pages.go                  ← Interactive HTML pages (redirects, authorization completion)
```

## Adding a New OAuth Provider

The auth package is extensible via the `Provider` interface. To add a new provider (e.g., Google):

1. Create `pkg/auth/provider_google.go`
2. Implement the four methods:

```go
type GoogleProvider struct { ... }

func (p *GoogleProvider) BuildAuthURL(callbackURL, state string) (string, error) { ... }
func (p *GoogleProvider) ExchangeCode(ctx context.Context, code, callbackURL string) (*ExternalToken, error) { ... }
func (p *GoogleProvider) RefreshToken(ctx context.Context, refreshToken string) (*ExternalToken, error) { ... }
func (p *GoogleProvider) IntrospectToken(ctx context.Context, token string) (*IntrospectionResponse, error) { ... }
```

3. Wire the provider in `cmd/unstructured-data-mcp-server/main.go`. The entry point currently instantiates `auth.NewGenericProvider` directly, so switch or parameterize the constructor call there

No changes needed to the middleware, OAuth server endpoints, or MCP layer.

## Skills

A pre-built skill file is available at [`skills/unstructured-data-mcp-skill.md`](../skills/unstructured-data-mcp-skill.md) for use with LLM coding assistants (Claude Code, Cursor, etc.). It describes how to connect to and use the MCP server tools.

## SSO Provider Setup

When registering the MCP server as a client in your SSO provider, configure:

- **Valid redirect URI**: Your `SSO_CALLBACK_URL` value (e.g., `http://localhost:8080/auth/callback/oidc`)
- **Client authentication**: Client ID and Secret (confidential client)
- **Grant types**: Authorization Code, Refresh Token
- **Scopes**: `openid` (minimum)
