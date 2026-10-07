# Observability

The Unstructured Data Controller and MCP Server include built-in observability via [OpenTelemetry](https://opentelemetry.io/). Both binaries expose metrics through a Prometheus-compatible `/metrics` endpoint and can optionally push telemetry to an OTel Collector via OTLP gRPC.

## How It Works

Both binaries use the shared `pkg/metrics` package which initializes the OTel SDK with dual export:

- **Prometheus export (always on)** -- custom metrics are served on a `/metrics` HTTP endpoint in Prometheus format. For the controller, they appear alongside controller-runtime's built-in metrics (work queue depth, reconcile counts, etc.). For the MCP server, a dedicated `/metrics` endpoint is exposed on a separate port (8000) to isolate it from the MCP protocol traffic on port 8080.

- **OTLP push (opt-in)** -- when `OTEL_EXPORTER_OTLP_ENDPOINT` is set, metrics and traces are pushed to an OTel Collector via gRPC. The collector can then forward to any backend (Datadog, Langfuse, Jaeger, etc.).

```
Controller ──┐                              ┌── Datadog
             ├── OTLP gRPC ──> OTel Collector ──> Langfuse
MCP Server ──┘                              └── (any OTLP backend)
             │
             └── /metrics ──> Prometheus / any scraper
```

## Configuration

All configuration uses standard [OTel environment variables](https://opentelemetry.io/docs/specs/otel/configuration/sdk-environment-variables/) -- no custom flags needed.

| Variable | Description | Default |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | OTel Collector endpoint (e.g. `http://otel-collector:4317`). Enables OTLP push when set. | unset (disabled) |
| `OTEL_EXPORTER_OTLP_INSECURE` | Disable TLS for the collector connection (plaintext gRPC). An `http://` endpoint scheme also disables TLS. | `false` |
| `OTEL_SERVICE_NAME` | Override the default service name in telemetry data. | set by the binary |

### Enabling OTLP push

**Controller** -- uncomment the env vars in `config/manager/manager.yaml`:

```yaml
- name: OTEL_EXPORTER_OTLP_ENDPOINT
  value: "http://otel-collector.observability.svc:4317"
- name: OTEL_EXPORTER_OTLP_INSECURE
  value: "true"
```

**MCP Server** -- uncomment in `config/mcp/configmap.yaml`:

```yaml
OTEL_EXPORTER_OTLP_ENDPOINT: "http://otel-collector.observability.svc:4317"
OTEL_EXPORTER_OTLP_INSECURE: "true"
```

### Prometheus scraping

The controller's metrics endpoint is managed by controller-runtime (port 8443, HTTPS with authn/authz). See the [kubebuilder metrics reference](https://book.kubebuilder.io/reference/metrics) for configuration.

The MCP server's `/metrics` endpoint is on a dedicated port 8000 (HTTP, unauthenticated), isolated from the MCP protocol on port 8080. The deployment includes standard Prometheus scrape annotations:

```yaml
annotations:
  prometheus.io/scrape: "true"
  prometheus.io/port: "8000"
  prometheus.io/path: "/metrics"
```

## Metrics Reference

### Controller metrics

Recorded per reconcile loop. All metrics include a `controller` attribute identifying which controller emitted them.

| Metric | Type | Unit | Description |
|---|---|---|---|
| `udc.reconcile.count` | Counter | reconcile | Total reconcile invocations |
| `udc.reconcile.duration` | Histogram | seconds | Wall-clock duration of a reconcile loop |
| `udc.reconcile.errors` | Counter | error | Reconcile invocations that returned an error |
| `udc.files.processed` | Counter | file | Cumulative files processed across reconciles |

Prometheus names follow OTel conventions (dots become underscores, counters get `_total` suffix):

```
udc_reconcile_count_total{controller="DocumentProcessor"} 42
udc_reconcile_duration_seconds_bucket{controller="SourceCrawler",le="1"} 15
udc_files_processed_total{controller="ChunksGenerator"} 1200
```

### MCP Server metrics

Recorded per tool invocation.

| Metric | Type | Unit | Description |
|---|---|---|---|
| `udc.mcp.tool.calls` | Counter | call | Total MCP tool invocations |
| `udc.mcp.tool.duration` | Histogram | seconds | Tool invocation wall-clock duration |
| `udc.mcp.tool.errors` | Counter | error | Tool invocations that returned an error |
| `udc.mcp.external.duration` | Histogram | seconds | Duration of external service calls (K8s, Snowflake, embedding API) |

Tool metrics include a `tool` attribute. External call metrics include `service` and `operation` attributes.

### MCP Server traces

When OTLP is enabled, the MCP server emits traces with spans for each tool invocation and child spans for external service calls. Child spans are created in `pkg/` packages via the global `otel.Tracer()` API and propagate automatically through context.

**get_chunks_for_embeddings:**
```
mcp.tool/get_chunks_for_embeddings       [root, attrs: mcp.pipeline_name]
  |-- k8s.get_pipeline_query_config      [attrs: pipeline.name, stage.type]
  |-- embedding.generate                 [attrs: embedding.model, embedding.input_count]
  |-- snowflake.search_chunks            [attrs: db.system, db.name]
```

**list_pipelines:**
```
mcp.tool/list_pipelines                  [root]
  |-- k8s.list_pipelines
  |-- snowflake.show_databases           [attrs: db.system]
```

**get_processed_document:**
```
mcp.tool/get_processed_document          [root, attrs: mcp.pipeline_name, mcp.file_id]
  |-- k8s.get_pipeline_query_config      [attrs: pipeline.name, stage.type]
  |-- snowflake.get_processed_document   [attrs: db.system, db.name]
```

Traces are useful for Langfuse integration (LLM observability) and debugging latency in the tool call chain.

## Adding Instrumentation to New Code

### New controller

Add named returns and a `ReconcileObserver` defer at the top of the `Reconcile` method:

```go
func (r *MyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (result ctrl.Result, retErr error) {
    if metricsProvider != nil {
        obs := metricsProvider.ReconcileObserver("MyController")
        defer obs.End(ctx, &retErr)
    }
    // ... existing reconcile logic ...
}
```

To track files processed, call `RecordFilesProcessed` next to the status update:

```go
cr.Status.FilesProcessed += count
if metricsProvider != nil {
    metricsProvider.RecordFilesProcessed(ctx, "MyController", count)
}
```

The `metricsProvider` package-level variable is set at startup via `SetMetricsProvider()` in `internal/controller/telemetry.go`. The nil guard ensures controllers work without metrics during tests.

### New MCP tool

Add the `*udcmetrics.Provider` parameter to the Register function and use `ToolObserver` + `RecordExternalCall`:

```go
func RegisterMyTool(s *mcp.Server, mp *udcmetrics.Provider) {
    mcp.AddTool(s, &mcp.Tool{...},
        func(ctx context.Context, ...) (res *mcp.CallToolResult, _ any, _ error) {
        if mp != nil {
            var endTool func(bool)
            ctx, endTool = mp.ToolObserver(ctx, "my_tool",
                attribute.String("mcp.pipeline_name", args.PipelineName))
            // Derive error flag from the returned result's IsError field.
            defer func() { endTool(res != nil && res.IsError) }()
        }

        // Time external calls:
        start := time.Now()
        result, err := externalService.Call(ctx, ...)
        if mp != nil {
            mp.RecordExternalCall(ctx, "service_name", "operation", time.Since(start), err)
        }
        if err != nil {
            return errorResult, nil, nil
        }
        // ...
    })
}
```

### New pkg/ function with external calls

Add a trace span using the global `otel.Tracer()` API. No import of `internal/` or `pkg/metrics` needed -- the span propagates automatically when the caller has an active span:

```go
func MyExternalCall(ctx context.Context, ...) (result T, err error) {
    ctx, span := otel.Tracer("pkg/mypackage").Start(ctx, "mypackage.operation",
        trace.WithSpanKind(trace.SpanKindClient),
        trace.WithAttributes(attribute.String("key", "value")))
    defer func() {
        if err != nil {
            span.RecordError(err)
            span.SetStatus(codes.Error, err.Error())
        }
        span.End()
    }()
    // ... existing logic ...
}
```

## Testing Locally

### Verify Prometheus metrics (no collector needed)

```bash
# Build and run the MCP server
go build -o /tmp/mcp-server ./cmd/unstructured-data-mcp-server

SSO_AUTHORIZATION_URL=http://localhost \
SSO_TOKEN_URL=http://localhost \
SSO_CALLBACK_URL=http://localhost \
SSO_DISABLE_INTROSPECTION=true \
/tmp/mcp-server &

# Check for custom metrics
curl -s http://localhost:8000/metrics | grep udc_
```

### Verify OTLP push with a local collector

```bash
# Start a local OTel Collector
docker run --rm -p 4317:4317 -p 55679:55679 \
  otel/opentelemetry-collector-contrib:latest

# Run the MCP server with OTLP enabled
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
OTEL_EXPORTER_OTLP_INSECURE=true \
SSO_AUTHORIZATION_URL=http://localhost \
SSO_TOKEN_URL=http://localhost \
SSO_CALLBACK_URL=http://localhost \
SSO_DISABLE_INTROSPECTION=true \
/tmp/mcp-server
```

The collector logs will show received metric data points.

### Unit tests

```bash
go test -v ./pkg/metrics/
```

## Architecture Decisions

**Why OTel SDK instead of Prometheus client?** The project needs to support both pull (Prometheus scraping for OSS users) and push (OTLP to OTel Collector for Datadog/Langfuse). The OTel SDK supports both via its Prometheus exporter and OTLP exporter. This follows the same pattern as [Tekton](https://tekton.dev/) and [Knative](https://knative.dev/), which are the only major Kubernetes projects that use the OTel SDK natively.

**Why not the Prometheus-to-OTel bridge?** The bridge adds app-side overhead and silently drops Summary metrics. Since this is a new instrumentation effort (no existing Prometheus metrics to migrate), writing directly against the OTel SDK avoids the translation layer entirely. Controller-runtime's built-in Prometheus metrics remain on the `/metrics` endpoint as-is.

**Why pin OTel exporters to v1.44.0?** The existing controller-runtime dependency pulls in `otel v1.44.0`. Pinning the exporters to matching versions avoids cascading transitive dependency upgrades (newer OTel versions require Go 1.26, while the project targets Go 1.25).
