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
	"testing"

	"github.com/go-logr/logr/testr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/telemetry"
)

// clearTelemetryEnv isolates a test from whatever the developer happens to
// have exported, and from tests that ran before it.
func clearTelemetryEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		"OTEL_SDK_DISABLED",
		"OTEL_TRACES_EXPORTER",
		"OTEL_METRICS_EXPORTER",
		"OTEL_EXPORTER_OTLP_ENDPOINT",
		"OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
		"OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_SERVICE_NAME",
		"OTEL_RESOURCE_ATTRIBUTES",
		"OTEL_TRACES_SAMPLER",
		"OTEL_TRACES_SAMPLER_ARG",
	} {
		t.Setenv(key, "")
	}
}

func TestSetupRequiresServiceName(t *testing.T) {
	clearTelemetryEnv(t)

	provider, err := telemetry.Setup(context.Background(), telemetry.Config{Logger: testr.New(t)})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "ServiceName is required")
	// A usable Provider even on the error path, so a caller that logs and
	// continues cannot nil-panic in its shutdown handler.
	assert.NotNil(t, provider)
	assert.False(t, provider.Enabled())
	assert.NoError(t, provider.Shutdown(context.Background()))
}

func TestSetupDisabled(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			// The default for a developer running the binary locally and for
			// any cluster without a Collector.
			name: "nothing configured",
			env:  map[string]string{},
		},
		{
			name: "both exporters explicitly none",
			env: map[string]string{
				"OTEL_TRACES_EXPORTER":  "none",
				"OTEL_METRICS_EXPORTER": "none",
			},
		},
		{
			// The spec-defined kill switch must win even when an endpoint is
			// configured, because that is the state an operator reaches for
			// when telemetry is causing a problem in production.
			name: "OTEL_SDK_DISABLED overrides a configured endpoint",
			env: map[string]string{
				"OTEL_SDK_DISABLED":           "true",
				"OTEL_EXPORTER_OTLP_ENDPOINT": "http://collector:4317",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearTelemetryEnv(t)
			for key, value := range tt.env {
				t.Setenv(key, value)
			}

			provider, err := telemetry.Setup(context.Background(), telemetry.Config{
				ServiceName: "disabled-test",
				Logger:      testr.New(t),
			})
			require.NoError(t, err)
			assert.False(t, provider.Enabled())

			// The globals must be the API's no-op implementations, not an SDK
			// provider with nowhere to send data. A real provider would still
			// allocate a recording span on every call and throw it away.
			assert.IsType(t, tracenoop.NewTracerProvider(), otel.GetTracerProvider())
			assert.IsType(t, metricnoop.NewMeterProvider(), otel.GetMeterProvider())

			assert.NoError(t, provider.Shutdown(context.Background()))
		})
	}
}

func TestSetupConsoleExporters(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "console")
	t.Setenv("OTEL_METRICS_EXPORTER", "console")

	provider, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName: "console-test",
		Logger:      testr.New(t),
	})
	require.NoError(t, err)

	// Console needs no endpoint, which is what makes it the zero-setup way to
	// see telemetry working locally.
	assert.True(t, provider.Enabled())
	assert.NotEqual(t, tracenoop.NewTracerProvider(), otel.GetTracerProvider())

	require.NoError(t, provider.Shutdown(context.Background()))
}

func TestSetupRejectsUnsupportedExporter(t *testing.T) {
	tests := []struct {
		name    string
		envKey  string
		wantErr string
	}{
		{
			name:    "traces",
			envKey:  "OTEL_TRACES_EXPORTER",
			wantErr: `unsupported OTEL_TRACES_EXPORTER value "jaeger"`,
		},
		{
			name:    "metrics",
			envKey:  "OTEL_METRICS_EXPORTER",
			wantErr: `unsupported OTEL_METRICS_EXPORTER value "jaeger"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			clearTelemetryEnv(t)
			t.Setenv(tt.envKey, "jaeger")

			provider, err := telemetry.Setup(context.Background(), telemetry.Config{
				ServiceName: "bad-exporter-test",
				Logger:      testr.New(t),
			})

			require.Error(t, err)
			assert.Contains(t, err.Error(), tt.wantErr)
			// Setup must not leave half an SDK installed behind an error.
			assert.False(t, provider.Enabled())
			assert.NoError(t, provider.Shutdown(context.Background()))
		})
	}
}

func TestSetupInstallsPropagatorsEvenWhenDisabled(t *testing.T) {
	clearTelemetryEnv(t)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator())

	_, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName: "propagator-test",
		Logger:      testr.New(t),
	})
	require.NoError(t, err)

	// Propagation costs nothing and keeps an inbound trace context flowing
	// through the no-op tracer, so downstream services still join the trace
	// even when this process exports nothing.
	fields := otel.GetTextMapPropagator().Fields()
	assert.Contains(t, fields, "traceparent")
	assert.Contains(t, fields, "baggage")
}

func TestShutdownIsIdempotent(t *testing.T) {
	clearTelemetryEnv(t)
	t.Setenv("OTEL_TRACES_EXPORTER", "console")

	provider, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName: "shutdown-test",
		Logger:      testr.New(t),
	})
	require.NoError(t, err)

	// Both binaries can reach their flush more than once on tangled exit
	// paths, so a second call must be harmless rather than an error or a
	// panic on an already-stopped provider.
	require.NoError(t, provider.Shutdown(context.Background()))
	require.NoError(t, provider.Shutdown(context.Background()))
}

func TestShutdownOnNilProvider(t *testing.T) {
	var provider *telemetry.Provider
	assert.NoError(t, provider.Shutdown(context.Background()))
}
