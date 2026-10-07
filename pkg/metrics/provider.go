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

// Package metrics provides OpenTelemetry SDK initialization with dual export:
// OTLP push (for collectors like Datadog/Langfuse) and Prometheus pull (for OSS scraping).
package metrics

import (
	"context"
	"errors"
	"net/http"
	"os"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.41.0"
)

// Config holds configuration for the telemetry provider.
type Config struct {
	// ServiceName identifies this service in telemetry backends (e.g. "unstructured-data-controller").
	ServiceName string

	// ServiceVersion is the version of this service (e.g. "0.1.0").
	ServiceVersion string

	// PrometheusRegisterer is the Prometheus registerer where OTel metrics are published.
	// When non-nil (controller case), custom OTel metrics appear on controller-runtime's
	// existing /metrics endpoint alongside its built-in metrics.
	// When nil (MCP server case), a new registry is created and served via PrometheusHandler().
	PrometheusRegisterer prometheus.Registerer
}

// Provider manages OTel MeterProvider and TracerProvider lifecycle.
// It supports dual export: Prometheus (always on) and OTLP gRPC (when OTEL_EXPORTER_OTLP_ENDPOINT is set).
type Provider struct {
	meterProvider  *sdkmetric.MeterProvider
	tracerProvider *sdktrace.TracerProvider
	Instruments    *Instruments

	// promGatherer is set only when PrometheusRegisterer was nil (MCP server case),
	// so PrometheusHandler() can serve the metrics.
	promGatherer prometheus.Gatherer
}

// Init creates and configures OTel providers with dual export.
// Prometheus export is always active. OTLP push activates when OTEL_EXPORTER_OTLP_ENDPOINT
// is set - the SDK reads that env var automatically, so no manual endpoint parsing is needed.
func Init(ctx context.Context, cfg Config) (*Provider, error) {
	res, err := resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceName(cfg.ServiceName),
			semconv.ServiceVersion(cfg.ServiceVersion),
		),
		// WithFromEnv reads OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES.
		// Later detectors override earlier ones, so env vars take precedence
		// over the hardcoded defaults above.
		resource.WithFromEnv(),
	)
	if err != nil {
		return nil, err
	}

	p := &Provider{}

	readers, err := p.setupMetricReaders(ctx, cfg)
	if err != nil {
		return nil, err
	}

	mpOpts := []sdkmetric.Option{sdkmetric.WithResource(res)}
	for _, r := range readers {
		mpOpts = append(mpOpts, sdkmetric.WithReader(r))
	}
	p.meterProvider = sdkmetric.NewMeterProvider(mpOpts...)

	// If later steps fail, shut down the MeterProvider to stop its background
	// periodic reader. The deferred cleanup is cancelled on success.
	meterProviderReady := false
	defer func() {
		if !meterProviderReady {
			_ = p.meterProvider.Shutdown(context.Background())
		}
	}()

	tp, err := setupTracerProvider(ctx, res)
	if err != nil {
		return nil, err
	}
	p.tracerProvider = tp

	instruments, err := newInstruments(p.meterProvider)
	if err != nil {
		// Clean up the TracerProvider we just created.
		_ = tp.Shutdown(context.Background())
		return nil, err
	}
	p.Instruments = instruments

	// Set globals only after all components are ready, so a partial failure
	// never leaves the process with half-initialized global providers.
	otel.SetMeterProvider(p.meterProvider)
	otel.SetTracerProvider(p.tracerProvider)

	meterProviderReady = true
	return p, nil
}

// setupMetricReaders creates the Prometheus reader (always) and optional OTLP periodic reader.
func (p *Provider) setupMetricReaders(ctx context.Context, cfg Config) ([]sdkmetric.Reader, error) {
	var readers []sdkmetric.Reader

	promReader, err := p.setupPrometheusReader(cfg)
	if err != nil {
		return nil, err
	}
	readers = append(readers, promReader)

	// OTLP metric exporter activates only when OTEL_EXPORTER_OTLP_ENDPOINT is set.
	// This keeps the default OSS experience zero-config (Prometheus only).
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		otlpExporter, otlpErr := otlpmetricgrpc.New(ctx)
		if otlpErr != nil {
			return nil, otlpErr
		}
		readers = append(readers, sdkmetric.NewPeriodicReader(otlpExporter))
	}

	return readers, nil
}

// setupPrometheusReader creates the OTel Prometheus exporter.
// When a registerer is provided (controller), it registers on the existing Prometheus registry.
// When nil (MCP server), it creates a new registry whose handler is accessible via PrometheusHandler().
func (p *Provider) setupPrometheusReader(cfg Config) (sdkmetric.Reader, error) {
	var opts []promexporter.Option

	if cfg.PrometheusRegisterer != nil {
		opts = append(opts, promexporter.WithRegisterer(cfg.PrometheusRegisterer))
	} else {
		// MCP server case: create a dedicated registry so the metrics endpoint
		// only exposes this application's metrics, not the default process collector.
		reg := prometheus.NewRegistry()
		opts = append(opts, promexporter.WithRegisterer(reg))
		p.promGatherer = reg
	}

	return promexporter.New(opts...)
}

// setupTracerProvider creates a TracerProvider with optional OTLP export.
// When OTEL_EXPORTER_OTLP_ENDPOINT is not set, the provider is created without
// an exporter - traces are no-ops (zero overhead, no data sent).
func setupTracerProvider(ctx context.Context, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	tpOpts := []sdktrace.TracerProviderOption{sdktrace.WithResource(res)}

	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		traceExporter, err := otlptracegrpc.New(ctx)
		if err != nil {
			return nil, err
		}
		tpOpts = append(tpOpts, sdktrace.WithBatcher(traceExporter))
	}

	return sdktrace.NewTracerProvider(tpOpts...), nil
}

// PrometheusHandler returns an http.Handler serving Prometheus metrics.
// Returns nil when the provider was initialized with an external registerer
// (controller case, where metrics are served by controller-runtime's built-in endpoint).
func (p *Provider) PrometheusHandler() http.Handler {
	if p.promGatherer == nil {
		return nil
	}
	return promhttp.HandlerFor(p.promGatherer, promhttp.HandlerOpts{})
}

// Shutdown flushes pending telemetry and releases resources.
// Safe to call multiple times.
func (p *Provider) Shutdown(ctx context.Context) error {
	var errs []error
	if p.meterProvider != nil {
		if err := p.meterProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	if p.tracerProvider != nil {
		if err := p.tracerProvider.Shutdown(ctx); err != nil {
			errs = append(errs, err)
		}
	}
	return errors.Join(errs...)
}

// Start implements manager.Runnable for controller-runtime lifecycle integration.
// It blocks until the context is cancelled, then flushes and shuts down the providers.
func (p *Provider) Start(ctx context.Context) error {
	<-ctx.Done()
	// Use a bounded timeout so a slow OTLP flush doesn't stall the manager
	// shutdown past the pod's terminationGracePeriodSeconds.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return p.Shutdown(shutdownCtx)
}

// NeedLeaderElection returns false so telemetry runs on every replica,
// not just the leader. Without this, non-leader replicas never call Start
// and their providers are never shut down.
func (*Provider) NeedLeaderElection() bool { return false }
