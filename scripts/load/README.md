# MCP Server Load Tests

[k6](https://grafana.com/docs/k6/) load test for the Unstructured Data MCP server (Streamable HTTP transport).

## Prerequisites

- **k6** v0.50+ — [install guide](https://grafana.com/docs/k6/latest/set-up/install-k6/)
- **python3** — used by `get-token.sh` for OAuth PKCE flow
- A running MCP server (local or remote)
- A valid bearer token (see [Getting a Token](#getting-a-token) below)

## Getting a Token

The `get-token.sh` script handles the full MCP OAuth flow (dynamic client registration + PKCE + browser SSO):

```bash
# Against local server (default: http://localhost:8080)
./scripts/load/get-token.sh

# Against a remote server
MCP_BASE_URL=https://your-mcp-server.example.com ./scripts/load/get-token.sh
```

It opens your browser for SSO login and prints the access token. Export it:

```bash
export MCP_BASE_URL=http://localhost:8080
export MCP_TOKEN='<token from get-token.sh>'
```

## `mcp-load-test.js`

Simulates a real agent's tool-chaining behavior: initialize session, list pipelines (or use a fixed one), query chunks, optionally fetch a document — using real data from each prior step.

### Quick Start

```bash
# Default: 50 VUs, 1m hold, discovers pipelines dynamically
k6 run scripts/load/mcp-load-test.js

# Stress test a specific pipeline (skips list_pipelines)
VUS=250 DURATION=2m MCP_PIPELINE_NAME=my-pipeline \
  k6 run scripts/load/mcp-load-test.js

# Smoke test
VUS=5 DURATION=30s RAMP_UP=5s k6 run scripts/load/mcp-load-test.js

# Instant ramp (all VUs active immediately)
RAMP_UP=0 k6 run scripts/load/mcp-load-test.js

# Explore capacity without failing on thresholds
VUS=200 DURATION=3m SKIP_THRESHOLDS=1 k6 run scripts/load/mcp-load-test.js
```

### Environment Variables

| Env Var | Default | Description |
|---------|---------|-------------|
| `MCP_BASE_URL` | `http://localhost:8080` | MCP server URL |
| `MCP_TOKEN` | - | Bearer access token (required) |
| `MCP_PIPELINE_NAME` | - | Skip pipeline discovery and target this pipeline directly |
| `VUS` | `50` | Number of virtual users |
| `DURATION` | `1m` | Hold duration at peak VUs |
| `RAMP_UP` | `30s` | Ramp-up time (`0` for instant) |
| `RAMP_DOWN` | `15s` | Ramp-down time |
| `CHASE_DOCUMENT_PROB` | `0.5` | Probability of also fetching the full document |
| `THINK_TIME_MS` | `200` | Sleep between steps (ms) |
| `SKIP_THRESHOLDS` | - | Set to `1` to skip pass/fail thresholds |
| `DEBUG_TOOL_ERRORS` | - | Set to `1` to log tool error details |
| `SEND_LIMIT_ARG` | `1` | Set to `0` to omit the limit param on get_chunks |

## Fixtures

Edit `fixtures.json` to configure test queries:

```json
{
  "queries": [
    { "pipeline_name": "pipeline-a", "text": "your search query" }
  ]
}
```

- `pipeline_name` — the pipeline this query is meant for; in auto-discover mode the script matches it against the user's accessible pipelines
- `text` — the search query sent to `get_chunks_for_embeddings`

## Reports

k6 generates HTML and JSON reports in `scripts/load/reports/` after each run. These are gitignored — only `.gitkeep` is tracked.
