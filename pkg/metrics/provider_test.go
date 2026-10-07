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

package metrics

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInitWithDefaults(t *testing.T) {
	// Without OTEL_EXPORTER_OTLP_ENDPOINT, only Prometheus export should be active.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	p, err := Init(context.Background(), Config{
		ServiceName:    "test-service",
		ServiceVersion: "0.0.1",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	assert.NotNil(t, p.meterProvider, "MeterProvider should be initialized")
	assert.NotNil(t, p.tracerProvider, "TracerProvider should be initialized")
	assert.NotNil(t, p.Instruments, "Instruments should be initialized")
}

func TestInitWithExternalRegisterer(t *testing.T) {
	// Simulates the controller case: pass an existing Prometheus registry.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	reg := prometheus.NewRegistry()

	p, err := Init(context.Background(), Config{
		ServiceName:          "test-controller",
		ServiceVersion:       "0.0.1",
		PrometheusRegisterer: reg,
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	// When an external registerer is provided, PrometheusHandler() returns nil
	// because the controller-runtime endpoint already serves the metrics.
	assert.Nil(t, p.PrometheusHandler(), "PrometheusHandler should be nil when using an external registerer")
}

func TestPrometheusHandlerServesMetrics(t *testing.T) {
	// Simulates the MCP server case: no external registerer, so we create our own.
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	p, err := Init(context.Background(), Config{
		ServiceName:    "test-mcp-server",
		ServiceVersion: "0.0.1",
	})
	require.NoError(t, err)
	defer func() { require.NoError(t, p.Shutdown(context.Background())) }()

	handler := p.PrometheusHandler()
	require.NotNil(t, handler, "PrometheusHandler should be non-nil when no external registerer is provided")

	// Record a metric so the /metrics endpoint has something to serve.
	p.RecordFilesProcessed(context.Background(), "test-controller", 5)

	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	assert.Equal(t, http.StatusOK, rec.Code)

	body, err := io.ReadAll(rec.Body)
	require.NoError(t, err)
	bodyStr := string(body)

	// The OTel Prometheus exporter translates OTel metric names to Prometheus conventions:
	// "udc.files.processed" -> "udc_files_processed" (dots to underscores, counter suffix added).
	assert.True(t, strings.Contains(bodyStr, "udc_files_processed"),
		"expected udc_files_processed in metrics output, got: %s", bodyStr)
}

func TestShutdownIsIdempotent(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	p, err := Init(context.Background(), Config{
		ServiceName:    "test-service",
		ServiceVersion: "0.0.1",
	})
	require.NoError(t, err)

	require.NoError(t, p.Shutdown(context.Background()))
	// Second shutdown should not panic or return an unexpected error.
	// The OTel SDK may return an error on double shutdown, but it should not panic.
	_ = p.Shutdown(context.Background())
}

func TestStartBlocksUntilContextCancelled(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")

	p, err := Init(context.Background(), Config{
		ServiceName:    "test-service",
		ServiceVersion: "0.0.1",
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- p.Start(ctx)
	}()

	// Start should be blocking.
	select {
	case <-done:
		t.Fatal("Start returned before context was cancelled")
	case <-time.After(50 * time.Millisecond):
		// Expected: Start is still blocking.
	}

	cancel()

	select {
	case err := <-done:
		// Start should return after context cancellation. The SDK may return
		// "already shut down" if Shutdown was already called, which is acceptable.
		if err != nil {
			t.Logf("Start returned error after cancellation (acceptable): %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after context cancellation")
	}
}
