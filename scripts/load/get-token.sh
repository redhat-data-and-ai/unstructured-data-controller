#!/usr/bin/env bash
# Local helper: complete MCP OAuth (browser SSO login) and print access_token.
#
# Works against a LOCAL server or a REMOTE one (e.g. the sandbox route) —
# just set MCP_BASE_URL. Pass either the root URL or the /mcp/ URL; the
# trailing /mcp (if any) is stripped automatically since /healthz,
# /auth/register, /auth/authorize, /auth/token are root-level endpoints,
# not under /mcp.
#
# Prerequisites:
#   - The MCP server (local or remote) must be reachable from this machine.
#   - python3 available (serves the localhost redirect catcher on :8765).
#
# Usage:
#   ./scripts/load/get-token.sh                                  # local default
#   MCP_BASE_URL=http://localhost:8080 ./scripts/load/get-token.sh
#   MCP_BASE_URL=https://unstructured-data-mcp-server-sandbox.apps.int.spoke.prod.us-east-1.aws.paas.redhat.com \
#     ./scripts/load/get-token.sh
#
# Then:
#   export MCP_TOKEN='...'   # paste printed token
#   k6 run scripts/load/mcp-load-test.js

set -euo pipefail

MCP_BASE_URL="${MCP_BASE_URL:-http://localhost:8080}"
MCP_BASE_URL="${MCP_BASE_URL%/}"
MCP_BASE_URL="${MCP_BASE_URL%/mcp}"
REDIRECT_PORT="${REDIRECT_PORT:-8765}"
REDIRECT_URI="http://127.0.0.1:${REDIRECT_PORT}/callback"
CODE_FILE="$(mktemp)"
trap 'rm -f "$CODE_FILE"' EXIT

echo "==> Checking MCP server at ${MCP_BASE_URL}"
if ! curl -sf "${MCP_BASE_URL}/healthz" >/dev/null; then
  echo "ERROR: ${MCP_BASE_URL}/healthz is not reachable. Start the MCP server first." >&2
  exit 1
fi

echo "==> Registering OAuth client"
REG=$(curl -sf -X POST "${MCP_BASE_URL}/auth/register" \
  -H 'Content-Type: application/json' \
  -d "{\"redirect_uris\":[\"${REDIRECT_URI}\"],\"grant_types\":[\"authorization_code\",\"refresh_token\"],\"response_types\":[\"code\"],\"token_endpoint_auth_method\":\"none\"}")

CLIENT_ID=$(echo "$REG" | python3 -c 'import json,sys; print(json.load(sys.stdin)["client_id"])')
echo "    client_id=${CLIENT_ID}"

# PKCE
CODE_VERIFIER=$(python3 - <<'PY'
import secrets, base64
print(base64.urlsafe_b64encode(secrets.token_bytes(32)).rstrip(b"=").decode())
PY
)
CODE_CHALLENGE=$(python3 - <<PY
import hashlib, base64
v = "${CODE_VERIFIER}".encode()
print(base64.urlsafe_b64encode(hashlib.sha256(v).digest()).rstrip(b"=").decode())
PY
)
STATE=$(python3 -c 'import secrets; print(secrets.token_urlsafe(16))')

AUTH_URL="${MCP_BASE_URL}/auth/authorize?response_type=code&client_id=$(python3 -c "import urllib.parse; print(urllib.parse.quote('${CLIENT_ID}'))")&redirect_uri=$(python3 -c "import urllib.parse; print(urllib.parse.quote('${REDIRECT_URI}'))")&code_challenge=${CODE_CHALLENGE}&code_challenge_method=S256&state=${STATE}"

echo "==> Starting local redirect catcher on ${REDIRECT_URI}"
python3 - "$REDIRECT_PORT" "$CODE_FILE" "$STATE" <<'PY' &
import http.server, urllib.parse, sys

port = int(sys.argv[1])
code_file = sys.argv[2]
expected_state = sys.argv[3]

class Handler(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        u = urllib.parse.urlparse(self.path)
        if u.path != "/callback":
            self.send_response(404); self.end_headers(); return
        q = urllib.parse.parse_qs(u.query)
        if q.get("state", [None])[0] != expected_state:
            self.send_response(400); self.end_headers()
            self.wfile.write(b"state mismatch"); return
        code = q.get("code", [None])[0]
        if not code:
            self.send_response(400); self.end_headers()
            self.wfile.write(b"missing code"); return
        open(code_file, "w").write(code)
        self.send_response(200)
        self.send_header("Content-Type", "text/html")
        self.end_headers()
        self.wfile.write(b"<h3>Login OK. You can close this tab and return to the terminal.</h3>")
    def log_message(self, *args):
        pass

httpd = http.server.HTTPServer(("127.0.0.1", port), Handler)
httpd.handle_request()
PY
CATCHER_PID=$!

cleanup_catcher() {
  kill "$CATCHER_PID" 2>/dev/null || true
}
trap 'cleanup_catcher; rm -f "$CODE_FILE"' EXIT

echo ""
echo "Open this URL in your browser and complete SSO login:"
echo ""
echo "  ${AUTH_URL}"
echo ""
if command -v xdg-open >/dev/null 2>&1; then
  xdg-open "${AUTH_URL}" >/dev/null 2>&1 || true
elif command -v open >/dev/null 2>&1; then
  open "${AUTH_URL}" >/dev/null 2>&1 || true
fi

echo "==> Waiting for browser callback..."
wait "$CATCHER_PID"
CODE=$(cat "$CODE_FILE")
if [[ -z "$CODE" ]]; then
  echo "ERROR: no authorization code received" >&2
  exit 1
fi

echo "==> Exchanging code for tokens"
TOKEN_JSON=$(curl -sf -X POST "${MCP_BASE_URL}/auth/token" \
  -H 'Content-Type: application/x-www-form-urlencoded' \
  -d "grant_type=authorization_code" \
  -d "code=${CODE}" \
  -d "redirect_uri=${REDIRECT_URI}" \
  -d "client_id=${CLIENT_ID}" \
  -d "code_verifier=${CODE_VERIFIER}")

ACCESS_TOKEN=$(echo "$TOKEN_JSON" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("access_token",""))')
if [[ -z "$ACCESS_TOKEN" ]]; then
  echo "ERROR: no access_token in response:" >&2
  echo "$TOKEN_JSON" >&2
  exit 1
fi

echo ""
echo "SUCCESS. Export and run k6:"
echo ""
echo "  export MCP_BASE_URL=${MCP_BASE_URL}"
echo "  export MCP_TOKEN='${ACCESS_TOKEN}'"
echo "  k6 run scripts/load/mcp-load-test.js"
echo ""
echo "Access token (also above in MCP_TOKEN):"
echo "$ACCESS_TOKEN"
