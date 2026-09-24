/**
 * Shared k6 end-of-test report builder.
 *
 * Produces:
 *   - colored text summary on stdout (same as k6's default)
 *   - an HTML report (charts + tables) under scripts/load/reports/
 *   - the raw JSON summary under scripts/load/reports/
 *
 * Assumes k6 is invoked from the repo root, e.g.:
 *   k6 run scripts/load/mcp-load-test.js
 * If you run k6 from a different working directory, the report path below
 * will be relative to *that* directory instead.
 */
import { htmlReport } from 'https://raw.githubusercontent.com/benc-uk/k6-reporter/3.0.4/dist/bundle.js';
import { textSummary } from 'https://jslib.k6.io/k6-summary/0.1.0/index.js';

export function buildReportOutputs(data, name, title) {
  const ts = new Date().toISOString().replace(/[:.]/g, '-');
  const base = `scripts/load/reports/${name}-${ts}`;

  return {
    stdout: textSummary(data, { indent: ' ', enableColors: true }),
    [`${base}.html`]: htmlReport(data, { title: title || `MCP Load Test — ${name}` }),
    [`${base}.json`]: JSON.stringify(data, null, 2),
  };
}
