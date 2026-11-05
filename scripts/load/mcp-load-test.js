/**
 * k6 load test that simulates a REAL agent's tool-chaining behavior,
 * instead of calling each tool independently with static/fixture args.
 *
 * The MCP tools do NOT chain each other server-side (verified in the Go
 * source: each tool in internal/mcp/tools/*.go only talks to k8s/Snowflake
 * directly). The "call X first" hints only live in each tool's description,
 * meant for an LLM agent to follow. This script follows that same chain by
 * parsing each tool's real response and feeding it into the next call:
 *
 *   1. initialize                              → Mcp-Session-Id
 *   2. notifications/initialized
 *   3. tools/call list_unstructured_data_pipelines_for_user
 *        → parse the returned [{name, description}, ...] JSON
 *   4. tools/call get_chunks_for_embeddings(pipeline_name=<real name from #3>)
 *        → parse the returned chunk list, extract a real file_id
 *   5. (probabilistically) tools/call get_processed_document(
 *        pipeline_name=<same>, file_id=<real file_id from #4>)
 *   6. DELETE session
 *
 * If step 3 returns zero accessible pipelines for this user/token, steps
 * 4-5 are skipped for that iteration (counted via mcp_no_pipelines_available).
 *
 * Usage:
 *   export MCP_BASE_URL=http://localhost:8080
 *   export MCP_TOKEN='<bearer access token>'
 *   k6 run scripts/load/mcp-load-test.js
 *
 * Optional env:
 *   VUS=50  DURATION=1m  RAMP_UP=30s  RAMP_DOWN=15s
 *   RAMP_UP=0                # instant ramp: all VUS active from ~t=0 instead of
 *                             # gradually over RAMP_UP (switches to k6's
 *                             # constant-vus executor; RAMP_DOWN becomes the
 *                             # gracefulStop tail so in-flight iterations finish)
 *   CHASE_DOCUMENT_PROB=0.5   # chance of also calling get_processed_document
 *   THINK_TIME_MS=200
 *   SKIP_THRESHOLDS=1         # explore capacity without failing the run
 *   DEBUG_TOOL_ERRORS=1       # log the actual tool error text (use with low VUS!)
 *   MCP_PIPELINE_NAME=foo      # skip list_pipelines discovery and target this
 *                             # pipeline directly (stress-test mode)
 *   SEND_LIMIT_ARG=0          # omit the "limit" arg on get_chunks_for_embeddings
 *                             # (set this if the server predates the limit param
 *                             # and rejects it as an unexpected additional property)
 */

import http from 'k6/http';
import exec from 'k6/execution';
import { check, sleep, fail } from 'k6';
import { Rate, Trend, Counter } from 'k6/metrics';
import { SharedArray } from 'k6/data';
import { randomItem } from 'https://jslib.k6.io/k6-utils/1.4.0/index.js';
import { buildReportOutputs } from './lib/handle-summary.js';

const BASE_URL = (__ENV.MCP_BASE_URL || 'http://localhost:8080').replace(/\/$/, '');
const MCP_URL = `${BASE_URL}/mcp`;
const TOKEN = __ENV.MCP_TOKEN || '';

const VUS = Number(__ENV.VUS || 50);
const DURATION = __ENV.DURATION || '1m';
const RAMP_UP = __ENV.RAMP_UP || '30s';
const RAMP_DOWN = __ENV.RAMP_DOWN || '15s';
const INSTANT_RAMP = RAMP_UP === '0' || RAMP_UP === '0s';
const CHASE_DOCUMENT_PROB = Number(__ENV.CHASE_DOCUMENT_PROB || 0.5);
const THINK_TIME_MS = Number(__ENV.THINK_TIME_MS || 200);
const DEBUG_TOOL_ERRORS = __ENV.DEBUG_TOOL_ERRORS === '1';
const PIPELINE_NAME = __ENV.MCP_PIPELINE_NAME || '';
const SEND_LIMIT_ARG = __ENV.SEND_LIMIT_ARG !== '0';

const fixtures = new SharedArray('fixtures', () => {
  try {
    return [JSON.parse(open('./fixtures.json'))];
  } catch (_) {
    return [{}];
  }
});

const jsonrpcErrors = new Rate('mcp_jsonrpc_errors');
const toolErrors = new Rate('mcp_tool_errors');
const toolLatency = new Trend('mcp_tool_latency_ms', true);
const sessionsCreated = new Counter('mcp_sessions_created');
const chainCompleted = new Counter('mcp_chain_completed'); // list -> chunks -> doc, all real
const noPipelinesAvailable = new Counter('mcp_no_pipelines_available');
const noChunksFound = new Counter('mcp_no_chunks_found');
const queryPipelineFallback = new Counter('mcp_query_pipeline_fallback');

// Per-tool call counters — check these after a run to confirm every tool
// was actually exercised (each should be > 0).
const toolCallsByName = {
  list_unstructured_data_pipelines_for_user: new Counter('mcp_calls_list_pipelines'),
  get_chunks_for_embeddings: new Counter('mcp_calls_get_chunks'),
  get_processed_document: new Counter('mcp_calls_get_doc'),
};

const thresholds = {
  http_req_failed: ['rate<0.10'],
  'http_req_duration{step:tools_call}': ['p(95)<15000'],
  checks: ['rate>0.90'],
  mcp_tool_errors: ['rate<0.10'],
  mcp_jsonrpc_errors: ['rate<0.05'],
};

export const options = {
  scenarios: {
    mcp_agent_flow: INSTANT_RAMP
      ? {
          // All VUS start together (k6 spins them up as fast as it can, in
          // practice well under 1s for a few hundred VUs) instead of ramping
          // in over time. RAMP_DOWN is reused as gracefulStop so any
          // iteration still in flight when DURATION ends can finish cleanly.
          executor: 'constant-vus',
          vus: VUS,
          duration: DURATION,
          gracefulStop: RAMP_DOWN,
        }
      : {
          executor: 'ramping-vus',
          startVUs: 0,
          stages: [
            { duration: RAMP_UP, target: VUS },
            { duration: DURATION, target: VUS },
            { duration: RAMP_DOWN, target: 0 },
          ],
          gracefulRampDown: '30s',
        },
  },
  thresholds: __ENV.SKIP_THRESHOLDS === '1' ? {} : thresholds,
};

function commonHeaders(sessionId) {
  const headers = {
    Authorization: `Bearer ${TOKEN}`,
    'Content-Type': 'application/json',
    Accept: 'application/json, text/event-stream',
    'MCP-Protocol-Version': '2025-03-26',
  };
  if (sessionId) headers['Mcp-Session-Id'] = sessionId;
  return headers;
}

function rpc(id, method, params) {
  return JSON.stringify({ jsonrpc: '2.0', id, method, params: params || {} });
}

/** Parse MCP Streamable HTTP body (plain JSON or SSE data: lines) into the JSON-RPC message. */
function parseMcpBody(body) {
  if (!body) return null;
  const trimmed = String(body).trim();
  if (trimmed.startsWith('{') || trimmed.startsWith('[')) {
    try {
      return JSON.parse(trimmed);
    } catch (_) {
      /* fall through to SSE */
    }
  }
  let last = null;
  for (const line of trimmed.split('\n')) {
    const m = line.match(/^data:\s*(.+)$/);
    if (!m) continue;
    try {
      last = JSON.parse(m[1]);
    } catch (_) {
      /* ignore */
    }
  }
  return last;
}

/** Extract the tool's plain-text content out of a tools/call JSON-RPC result. */
function toolResultText(msg) {
  const content = msg && msg.result && msg.result.content;
  if (!Array.isArray(content) || !content.length) return '';
  return content.map((c) => c.text || '').join('\n');
}

/**
 * Both tool responses embed a JSON array inside human-readable text
 * (list_pipelines is pure JSON; get_chunks prefixes/suffixes text around
 * the JSON array). This extracts the array robustly either way.
 */
function extractJsonArray(text) {
  if (!text) return null;
  const trimmed = text.trim();
  try {
    const parsed = JSON.parse(trimmed);
    if (Array.isArray(parsed)) return parsed;
  } catch (_) {
    /* fall through to bracket extraction */
  }
  const start = trimmed.indexOf('[');
  const end = trimmed.lastIndexOf(']');
  if (start === -1 || end === -1 || end < start) return null;
  try {
    return JSON.parse(trimmed.slice(start, end + 1));
  } catch (_) {
    return null;
  }
}

/**
 * Each fixture query carries the pipeline it's actually about (see
 * fixtures.json), so the tool call can pair them correctly instead of
 * picking pipeline and query independently at random.
 */
function pickQueryEntry() {
  const queries = (fixtures[0] || {}).queries || ['What are the main topics?'];
  const entry = randomItem(queries);
  if (typeof entry === 'string') return { pipelineName: null, text: entry };
  return { pipelineName: entry.pipeline_name || null, text: entry.text || '' };
}

function callTool(sessionId, id, toolName, args) {
  const started = Date.now();
  const res = http.post(
    MCP_URL,
    rpc(id, 'tools/call', { name: toolName, arguments: args || {} }),
    {
      headers: commonHeaders(sessionId),
      tags: { step: 'tools_call', tool: toolName },
      timeout: '60s',
    },
  );
  toolLatency.add(Date.now() - started);
  if (toolCallsByName[toolName]) toolCallsByName[toolName].add(1);

  const msg = parseMcpBody(res.body);
  const hasJsonRpcError = !!(msg && msg.error);
  const isToolError = !!(msg && msg.result && msg.result.isError);

  jsonrpcErrors.add(hasJsonRpcError ? 1 : 0);
  toolErrors.add(isToolError || hasJsonRpcError ? 1 : 0);

  if (DEBUG_TOOL_ERRORS && (isToolError || hasJsonRpcError)) {
    const detail = hasJsonRpcError ? JSON.stringify(msg.error) : toolResultText(msg);
    console.log(`[${toolName}] error: ${String(detail).slice(0, 500)}`);
  }

  check(res, {
    [`${toolName}: status 200`]: (r) => r.status === 200,
    [`${toolName}: jsonrpc result`]: () => !!msg && !!msg.result && !msg.error,
    [`${toolName}: tool not isError`]: () =>
      !!msg && !!msg.result && !msg.result.isError,
  });

  return { res, msg, ok: !hasJsonRpcError && !isToolError, text: toolResultText(msg) };
}

export function setup() {
  if (!TOKEN) exec.test.abort('MCP_TOKEN env var is required');
  const health = http.get(`${BASE_URL}/healthz`);
  if (!check(health, { 'setup: /healthz is 200': (r) => r.status === 200 })) {
    exec.test.abort(`/healthz returned ${health.status} — is the MCP server running at ${BASE_URL}?`);
  }
  const pipelineInfo = PIPELINE_NAME ? ` pipeline=${PIPELINE_NAME} (fixed)` : ' pipeline=auto-discover';
  console.log(
    INSTANT_RAMP
      ? `Agent-flow profile: VUs=${VUS} (instant ramp) hold=${DURATION} gracefulStop=${RAMP_DOWN} chaseDocProb=${CHASE_DOCUMENT_PROB}${pipelineInfo}`
      : `Agent-flow profile: VUs=${VUS} ramp=${RAMP_UP} hold=${DURATION} down=${RAMP_DOWN} chaseDocProb=${CHASE_DOCUMENT_PROB}${pipelineInfo}`,
  );
}

export default function () {
  // --- 1. initialize ---
  const initRes = http.post(
    MCP_URL,
    rpc(1, 'initialize', {
      protocolVersion: '2025-03-26',
      capabilities: {},
      clientInfo: { name: 'k6-mcp-load-test', version: '1.0.0' },
    }),
    { headers: commonHeaders(), tags: { step: 'initialize' } },
  );

  const sessionId = initRes.headers['Mcp-Session-Id'] || initRes.headers['mcp-session-id'];
  const initOk = check(initRes, {
    'initialize: status 200': (r) => r.status === 200,
    'initialize: has session id': () => !!sessionId,
    'initialize: has result': (r) => {
      const msg = parseMcpBody(r.body);
      return msg && msg.result && !msg.error;
    },
  });
  if (!initOk || !sessionId) {
    jsonrpcErrors.add(1);
    sleep(THINK_TIME_MS / 1000);
    return;
  }
  sessionsCreated.add(1);

  http.post(
    MCP_URL,
    JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized', params: {} }),
    { headers: commonHeaders(sessionId), tags: { step: 'initialized' } },
  );

  // --- 2. resolve pipeline ---
  const queryEntry = pickQueryEntry();
  let pipeline;
  if (PIPELINE_NAME) {
    pipeline = { name: PIPELINE_NAME };
  } else {
    const listCall = callTool(sessionId, 2, 'list_unstructured_data_pipelines_for_user', {});
    const pipelines = listCall.ok ? extractJsonArray(listCall.text) : null;

    if (!pipelines || !pipelines.length) {
      noPipelinesAvailable.add(1);
      http.del(MCP_URL, null, {
        headers: commonHeaders(sessionId),
        tags: { step: 'session_delete' },
      });
      sleep(THINK_TIME_MS / 1000);
      return;
    }

    const matchedPipeline = queryEntry.pipelineName
      ? pipelines.find(
          (p) => p.name && p.name.toLowerCase() === queryEntry.pipelineName.toLowerCase(),
        )
      : null;
    if (queryEntry.pipelineName && !matchedPipeline) queryPipelineFallback.add(1);
    pipeline = matchedPipeline || randomItem(pipelines);
  }
  sleep(THINK_TIME_MS / 1000);

  // --- 3. get_chunks_for_embeddings using the pipeline matched to this query ---
  const chunksArgs = { pipeline_name: pipeline.name, query: queryEntry.text };
  if (SEND_LIMIT_ARG) chunksArgs.limit = 5;
  const chunksCall = callTool(sessionId, 3, 'get_chunks_for_embeddings', chunksArgs);

  const chunks = chunksCall.ok ? extractJsonArray(chunksCall.text) : null;
  if (!chunks || !chunks.length) {
    noChunksFound.add(1);
    http.del(MCP_URL, null, {
      headers: commonHeaders(sessionId),
      tags: { step: 'session_delete' },
    });
    sleep(THINK_TIME_MS / 1000);
    return;
  }

  // --- 4. (probabilistically) get_processed_document using the REAL file_id from step 3 ---
  if (Math.random() < CHASE_DOCUMENT_PROB) {
    const topChunk = chunks[0];
    const fileId = topChunk && topChunk.file_id;
    if (fileId) {
      sleep(THINK_TIME_MS / 1000);
      callTool(sessionId, 4, 'get_processed_document', {
        pipeline_name: pipeline.name,
        file_id: fileId,
      });
      chainCompleted.add(1);
    }
  } else {
    chainCompleted.add(1);
  }

  // --- 5. cleanup ---
  http.del(MCP_URL, null, {
    headers: commonHeaders(sessionId),
    tags: { step: 'session_delete' },
  });

  sleep(THINK_TIME_MS / 1000);
}

export function handleSummary(data) {
  return buildReportOutputs(data, 'mcp-load-test', 'MCP Load Test');
}
