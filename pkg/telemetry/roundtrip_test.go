/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package telemetry_test

import (
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/go-logr/logr/testr"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	prometheusbridge "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel"
	collectormetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	collectortracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	"google.golang.org/grpc"

	// Blank import for its side effect: controller-runtime registers its
	// reconcile, workqueue and Go runtime collectors into metrics.Registry
	// from package inits reached through the root package, not from
	// pkg/metrics itself. Without this the registry under test is empty and
	// the assertions below would pass vacuously.
	_ "sigs.k8s.io/controller-runtime"
	ctrlmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/telemetry"
)

// TestExportReachesCollector drives the whole export path against a real OTLP
// gRPC server: exporter construction, endpoint resolution from the
// environment, the resource, the Prometheus bridge producer, and the
// flush-on-shutdown. A constructor returning a non-nil provider proves none of
// that; only bytes arriving on a socket does.
func TestExportReachesCollector(t *testing.T) {
	collector := startCollector(t)

	// The bridged registry stands in for controller-runtime's. If a counter
	// registered here comes out the other end, the controller's
	// controller_runtime_* metrics will too.
	registry := prometheus.NewRegistry()
	bridged := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "udc_roundtrip_bridged_total",
		Help: "Counter registered on a bridged Prometheus registry.",
	})
	registry.MustRegister(bridged)
	bridged.Inc()

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.addr)
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	// Flush on shutdown rather than waiting out the default 60s interval.
	t.Setenv("OTEL_METRIC_EXPORT_INTERVAL", "100")
	t.Setenv("OTEL_TRACES_SAMPLER", "always_on")

	ctx := context.Background()
	provider, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName:    "roundtrip-test",
		ServiceVersion: "v9.9.9",
		MetricProducer: prometheusbridge.NewMetricProducer(prometheusbridge.WithGatherer(registry)),
		Logger:         testr.New(t),
	})
	require.NoError(t, err)
	require.True(t, provider.Enabled(), "an endpoint is configured, so telemetry must be enabled")

	_, span := otel.Tracer("roundtrip").Start(ctx, "roundtrip-span")
	span.End()

	counter, err := otel.Meter("roundtrip").Int64Counter("udc.roundtrip.native")
	require.NoError(t, err)
	counter.Add(ctx, 1)

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, provider.Shutdown(shutdownCtx))

	traces := collector.traceRequests()
	require.NotEmpty(t, traces, "no trace export reached the collector")

	spanNames, traceResources := flattenTraces(traces)
	assert.Contains(t, spanNames, "roundtrip-span")
	assert.Equal(t, "roundtrip-test", traceResources["service.name"])
	assert.Equal(t, "v9.9.9", traceResources["service.version"])

	metrics := collector.metricRequests()
	require.NotEmpty(t, metrics, "no metric export reached the collector")

	metricNames, metricResources := flattenMetrics(metrics)
	assert.Contains(t, metricNames, "udc.roundtrip.native", "native instrument did not reach the collector")
	assert.Contains(t, metricNames, "udc_roundtrip_bridged_total",
		"Prometheus bridge producer did not reach the collector")
	assert.Equal(t, "roundtrip-test", metricResources["service.name"])
}

// TestControllerRuntimeMetricsReachCollector guards the claim the controller's
// wiring rests on: that bridging controller-runtime's own Prometheus registry
// is enough to get its metrics to a Collector, so the operator needs no
// /metrics endpoint and no scrape RBAC.
//
// The registry is populated by package inits inside controller-runtime, not by
// anything this repo writes, so a dependency bump could empty it without a
// single line of our code changing. Nothing else in the suite would notice.
func TestControllerRuntimeMetricsReachCollector(t *testing.T) {
	collector := startCollector(t)

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.addr)
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")
	t.Setenv("OTEL_TRACES_EXPORTER", "none")

	ctx := context.Background()
	provider, err := telemetry.Setup(ctx, telemetry.Config{
		ServiceName:    "controller-runtime-bridge-test",
		MetricProducer: prometheusbridge.NewMetricProducer(prometheusbridge.WithGatherer(ctrlmetrics.Registry)),
		Logger:         testr.New(t),
	})
	require.NoError(t, err)
	require.True(t, provider.Enabled())

	shutdownCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	require.NoError(t, provider.Shutdown(shutdownCtx))

	exported := collector.metricRequests()
	require.NotEmpty(t, exported, "no metric export reached the collector")

	names, _ := flattenMetrics(exported)

	// The reconcile and workqueue families are Vecs with no children until a
	// controller actually runs, so they gather nothing in a unit test and
	// cannot be asserted here. The Go collector is unconditional, and it is
	// registered by the same init on the same registry, so its arrival is
	// what demonstrates the bridge is wired to controller-runtime's registry
	// rather than to an empty one.
	for _, want := range []string{"go_goroutines", "go_threads"} {
		assert.Contains(t, names, want, "expected %q to arrive via the Prometheus bridge", want)
	}
}

// TestShutdownWithExpiredContextDoesNotPanic covers the documented hazard that
// a cancelled context makes the flush a no-op. The contract under test is only
// that it fails safely, which is what lets the binaries treat a flush error as
// loggable rather than fatal.
func TestShutdownWithExpiredContextDoesNotPanic(t *testing.T) {
	collector := startCollector(t)

	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.addr)
	t.Setenv("OTEL_EXPORTER_OTLP_INSECURE", "true")

	provider, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName: "expired-context-test",
		Logger:      testr.New(t),
	})
	require.NoError(t, err)

	expired, cancel := context.WithCancel(context.Background())
	cancel()

	assert.NotPanics(t, func() {
		_ = provider.Shutdown(expired)
	})
}

// collector is an in-process OTLP/gRPC server that records what it receives.
type collector struct {
	collectortracepb.UnimplementedTraceServiceServer
	collectormetricspb.UnimplementedMetricsServiceServer

	addr string

	mu      sync.Mutex
	traces  []*collectortracepb.ExportTraceServiceRequest
	metrics []*collectormetricspb.ExportMetricsServiceRequest
}

func (c *collector) Export(
	_ context.Context,
	req *collectortracepb.ExportTraceServiceRequest,
) (*collectortracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.traces = append(c.traces, req)
	return &collectortracepb.ExportTraceServiceResponse{}, nil
}

// exportMetrics has to be adapted rather than named Export because the trace
// and metrics services declare the same method name with different signatures,
// so one type cannot satisfy both directly.
func (c *collector) exportMetrics(
	_ context.Context,
	req *collectormetricspb.ExportMetricsServiceRequest,
) (*collectormetricspb.ExportMetricsServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics = append(c.metrics, req)
	return &collectormetricspb.ExportMetricsServiceResponse{}, nil
}

func (c *collector) traceRequests() []*collectortracepb.ExportTraceServiceRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*collectortracepb.ExportTraceServiceRequest(nil), c.traces...)
}

func (c *collector) metricRequests() []*collectormetricspb.ExportMetricsServiceRequest {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]*collectormetricspb.ExportMetricsServiceRequest(nil), c.metrics...)
}

// metricsAdapter lets one collector back both gRPC services.
type metricsAdapter struct {
	collectormetricspb.UnimplementedMetricsServiceServer
	parent *collector
}

func (m metricsAdapter) Export(
	ctx context.Context,
	req *collectormetricspb.ExportMetricsServiceRequest,
) (*collectormetricspb.ExportMetricsServiceResponse, error) {
	return m.parent.exportMetrics(ctx, req)
}

// startCollector listens on an ephemeral loopback port and stops with the test.
func startCollector(t *testing.T) *collector {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// The scheme is required: the exporters silently ignore a bare host:port.
	recorder := &collector{addr: "http://" + listener.Addr().String()}

	server := grpc.NewServer()
	collectortracepb.RegisterTraceServiceServer(server, recorder)
	collectormetricspb.RegisterMetricsServiceServer(server, metricsAdapter{parent: recorder})

	go func() {
		// Serve returns an error once Stop is called; the test is over by then.
		_ = server.Serve(listener)
	}()
	t.Cleanup(server.Stop)

	return recorder
}

func flattenTraces(reqs []*collectortracepb.ExportTraceServiceRequest) ([]string, map[string]string) {
	var names []string
	resources := map[string]string{}

	for _, req := range reqs {
		for _, resourceSpans := range req.GetResourceSpans() {
			collectAttributes(resourceSpans.GetResource().GetAttributes(), resources)
			for _, scopeSpans := range resourceSpans.GetScopeSpans() {
				for _, span := range scopeSpans.GetSpans() {
					names = append(names, span.GetName())
				}
			}
		}
	}
	return names, resources
}

func flattenMetrics(reqs []*collectormetricspb.ExportMetricsServiceRequest) ([]string, map[string]string) {
	var names []string
	resources := map[string]string{}

	for _, req := range reqs {
		for _, resourceMetrics := range req.GetResourceMetrics() {
			collectAttributes(resourceMetrics.GetResource().GetAttributes(), resources)
			for _, scopeMetrics := range resourceMetrics.GetScopeMetrics() {
				for _, metric := range scopeMetrics.GetMetrics() {
					names = append(names, metric.GetName())
				}
			}
		}
	}
	return names, resources
}

func collectAttributes(attrs []*commonpb.KeyValue, into map[string]string) {
	for _, attr := range attrs {
		into[attr.GetKey()] = attr.GetValue().GetStringValue()
	}
}
