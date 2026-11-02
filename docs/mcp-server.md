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
| `SSO_INTROSPECTION_URL` | SSO token introspection endpoint (RFC 7662). Required unless `SSO_DISABLE_INTROSPECTION=true` (development only) |
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
| `SSO_DISABLE_INTROSPECTION`              | `false`                             | **Development only.** When `true`, skips SSO introspection and validates tokens locally without signature verification. Never enable in production. |

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

### `list_unstructured_data_pipelines_for_user`

Lists the UnstructuredDataPipeline custom resources the authenticated user can access (pipelines whose Snowflake database the user can query).

**Parameters:** None (uses OAuth token from context).

**Returns:** A JSON array of pipelines the user can access, each with:

- `name`: pipeline CR name
- `description`: human-readable summary of what the pipeline does
- `guidance`: optional usage guidance for agents (omitted when not set)

Pipelines are filtered server-side by the user's Snowflake database access; a connection can be further restricted with the `?pipelines=` query parameter (see [Client Configuration](#client-configuration)).

### `get_chunks_for_embeddings`

Searches for relevant text chunks in a data product using vector cosine similarity. Returns the top matching chunks for the given query (default 10, configurable via `limit` parameter).

**Parameters:**

| Parameter          | Required | Description                                                                                                                                  |
| ------------------ | -------- | -------------------------------------------------------------------------------------------------------------------------------------------- |
| `pipeline_name`    | Yes      | Name of the UnstructuredDataPipeline. If not known, call `list_unstructured_data_pipelines_for_user` first and pick the matching pipeline.   |
| `query`            | Yes      | The search query to find relevant chunks (vector similarity; natural-language queries work best).                                           |
| `file_path_filter` | No       | Glob pattern to scope the search to files matching this path (e.g. `*/README.md`, `repo-name/docs/*`).                                        |
| `limit`            | No       | Number of chunks to return. Defaults to 10.                                                                                                  |

**Typical agent flow:**

1. User asks a question (no pipeline specified).
2. Agent calls `list_unstructured_data_pipelines_for_user` to get pipeline names and descriptions.
3. Agent matches the user's question to a pipeline based on its description.
4. If confident, agent calls `get_chunks_for_embeddings` with the resolved `pipeline_name`; the Snowflake database, schema, and table are resolved server-side from the pipeline.
5. If ambiguous (multiple pipelines could match), agent asks the user to clarify.

### `get_processed_document`

Retrieves the processed document output (full Markdown from the pipeline's DocumentProcessor stage). Returns up to 20 documents per call.

**Parameters:**

| Parameter          | Required                              | Description                                                                                                 |
| ------------------ | ------------------------------------- | ----------------------------------------------------------------------------------------------------------- |
| `pipeline_name`    | Yes                                   | Name of the UnstructuredDataPipeline. If not known, call `list_unstructured_data_pipelines_for_user` first. |
| `file_id`          | One of `file_id` / `file_path_filter` | The exact file identifier to look up. Takes priority over `file_path_filter` when both are provided.        |
| `file_path_filter` | One of `file_id` / `file_path_filter` | Glob pattern to retrieve processed documents from matching files (e.g. `*/README.md`).                      |

Use this when chunks from `get_chunks_for_embeddings` don't provide enough context (for example, to follow up with the `file_id` of the top matching chunk).

## Package Structure

```
cmd/unstructured-data-mcp-server/
  main.go                  ← entry point, wiring

internal/mcp/tools/
  list_pipelines.go         ← list_unstructured_data_pipelines_for_user tool
  get_chunks.go             ← get_chunks_for_embeddings tool
  get_processed_document.go ← get_processed_document tool

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
