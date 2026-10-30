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

// Package telemetry bootstraps the OpenTelemetry SDK for the controller and
// the MCP server.
//
// Both binaries push OTLP to an OpenTelemetry Collector, which fans the data
// out to the observability backends. Nothing in this package knows which
// backends those are: switching from one to another is a Collector config
// change, not a code change.
//
// Telemetry is off unless it is configured. With no OTEL_* environment
// variables set, Setup installs no-op providers and the process behaves
// exactly as it did before telemetry existed.
package telemetry

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	metricnoop "go.opentelemetry.io/otel/metric/noop"
	"go.opentelemetry.io/otel/propagation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"
)

// sdkDisabledEnvKey is the specification-defined kill switch. The Go SDK does
// not read it itself, so we do.
const sdkDisabledEnvKey = "OTEL_SDK_DISABLED"

// Config configures the SDK. Only ServiceName is required; everything else
// either has a sensible default or is supplied through standard OTEL_*
// environment variables so that operators can retune a running deployment
// without a rebuild.
type Config struct {
	// ServiceName is the default for the service.name resource attribute.
	// OTEL_SERVICE_NAME overrides it.
	ServiceName string

	// ServiceVersion is the default for the service.version resource
	// attribute. Empty is allowed but makes it impossible to tell releases
	// apart in a backend, so callers should stamp a real value.
	ServiceVersion string

	// TraceSampleRatio is the head sampling ratio used when
	// OTEL_TRACES_SAMPLER is not set. Zero means "sample nothing", so callers
	// that want every span must pass 1.
	//
	// The two binaries deliberately differ: reconcile loops run continuously
	// and are sampled down, while MCP tool calls are user-driven and low
	// volume, so they are not.
	TraceSampleRatio float64

	// MetricProducer is an extra SDK producer attached to the metric reader.
	// The controller uses this to push controller-runtime's existing
	// Prometheus registry through the same OTLP pipeline, which is why it
	// needs no scrape endpoint.
	MetricProducer sdkmetric.Producer

	// ResourceAttributes are merged into the resource. OTEL_RESOURCE_ATTRIBUTES
	// takes precedence over these.
	ResourceAttributes []attribute.KeyValue

	// Logger receives startup decisions and asynchronous exporter errors.
	// Without it, exporter failures would go to the SDK's default stderr
	// handler and never reach the structured log pipeline.
	Logger logr.Logger
}

// Provider owns the SDK providers that Setup installed as process globals.
// The zero value is a valid disabled Provider, so a caller that ignores a
// Setup error can still call Shutdown safely.
type Provider struct {
	tracerProvider *sdktrace.TracerProvider
	meterProvider  *sdkmetric.MeterProvider
	shutdownOnce   sync.Once
	shutdownErr    error
}

// Setup builds the SDK providers from cfg and the OTEL_* environment and
// installs them as the OpenTelemetry globals. It returns a usable Provider
// even when telemetry is disabled, so the caller never has to nil-check.
//
// Pass a context that outlives the process, not a signal-cancelled one: the
// OTLP exporters retain it for their gRPC dialer.
func Setup(ctx context.Context, cfg Config) (*Provider, error) {
	if cfg.ServiceName == "" {
		return &Provider{}, errors.New("telemetry: ServiceName is required")
	}

	// The propagators are installed even when telemetry is disabled. They cost
	// nothing, and they keep an incoming trace context flowing through the
	// no-op tracer so that downstream services still join the same trace.
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	))

	provider := &Provider{}

	if sdkDisabled(cfg.Logger) {
		cfg.Logger.Info("OpenTelemetry disabled", "reason", sdkDisabledEnvKey)
		installNoop()
		return provider, nil
	}

	warnOnSchemelessEndpoint(cfg.Logger)

	res, err := newResource(ctx, cfg)
	if err != nil {
		return provider, fmt.Errorf("building telemetry resource: %w", err)
	}

	if err := provider.setupTracing(ctx, cfg, res); err != nil {
		return provider, err
	}
	if err := provider.setupMetrics(ctx, cfg, res); err != nil {
		// Tracing may already be installed. Shut it back down so a partial
		// failure does not leave a provider nobody holds a handle to.
		_ = provider.Shutdown(ctx)
		return &Provider{}, err
	}

	// Route asynchronous exporter failures into the structured log pipeline.
	// The SDK default writes them to stderr, where they are invisible to the
	// cluster's log collection.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		cfg.Logger.Error(err, "OpenTelemetry exporter error")
	}))

	// Distinguish the two outcomes explicitly. "Configured" when nothing is
	// exporting reads as success and sends anyone debugging missing
	// telemetry looking in the wrong place; the first question is always
	// whether an endpoint was set at all.
	if !provider.Enabled() {
		cfg.Logger.Info("OpenTelemetry inactive: no exporter configured",
			"service", cfg.ServiceName,
			"hint", "set "+otlpEndpointEnvKey+", or "+tracesExporterEnvKey+"/"+metricsExporterEnvKey+"=console",
		)
		return provider, nil
	}

	cfg.Logger.Info("OpenTelemetry configured",
		"service", cfg.ServiceName,
		"version", cfg.ServiceVersion,
		"traces", provider.tracerProvider != nil,
		"metrics", provider.meterProvider != nil,
	)

	return provider, nil
}

func (p *Provider) setupTracing(ctx context.Context, cfg Config, res *resource.Resource) error {
	spanExporter, err := newSpanExporter(ctx)
	if err != nil {
		return fmt.Errorf("creating span exporter: %w", err)
	}

	// No exporter means no TracerProvider at all, rather than a provider whose
	// spans are dropped. Recording is decided by the sampler, not the
	// exporter, so an exporter-less provider would still allocate a full
	// recording span and evaluate every attribute on every call before
	// discarding it. The API's no-op tracer does none of that.
	if spanExporter == nil {
		otel.SetTracerProvider(tracenoop.NewTracerProvider())
		return nil
	}

	sampler := samplerFromEnv(cfg.TraceSampleRatio, cfg.Logger)
	cfg.Logger.Info("OpenTelemetry tracing enabled", "sampler", sampler.Description())

	p.tracerProvider = sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(spanExporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sampler),
	)
	otel.SetTracerProvider(p.tracerProvider)
	return nil
}

func (p *Provider) setupMetrics(ctx context.Context, cfg Config, res *resource.Resource) error {
	reader, err := newMetricReader(ctx, cfg.MetricProducer)
	if err != nil {
		return fmt.Errorf("creating metric reader: %w", err)
	}

	if reader == nil {
		otel.SetMeterProvider(metricnoop.NewMeterProvider())
		return nil
	}

	p.meterProvider = sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(res),
	)
	otel.SetMeterProvider(p.meterProvider)
	return nil
}

// Enabled reports whether any real exporter is installed. Callers can use it
// to skip work that only exists to feed telemetry.
func (p *Provider) Enabled() bool {
	return p != nil && (p.tracerProvider != nil || p.meterProvider != nil)
}

// Shutdown flushes buffered telemetry and stops the exporters. It is
// idempotent and safe on a disabled Provider.
//
// Pass a live context with its own deadline. A cancelled context makes the
// SDK's flush return immediately and silently drop whatever is buffered,
// which is exactly the data an operator most wants after a crash or an
// eviction.
func (p *Provider) Shutdown(ctx context.Context) error {
	if p == nil {
		return nil
	}

	p.shutdownOnce.Do(func() {
		var errs []error
		if p.tracerProvider != nil {
			if err := p.tracerProvider.Shutdown(ctx); err != nil {
				errs = append(errs, fmt.Errorf("shutting down tracer provider: %w", err))
			}
		}
		if p.meterProvider != nil {
			if err := p.meterProvider.Shutdown(ctx); err != nil {
				errs = append(errs, fmt.Errorf("shutting down meter provider: %w", err))
			}
		}
		p.shutdownErr = errors.Join(errs...)
	})

	return p.shutdownErr
}

// installNoop pins the globals to the API's no-op implementations so that an
// explicitly disabled process cannot accidentally pick up a provider
// installed by some other package's init.
func installNoop() {
	otel.SetTracerProvider(tracenoop.NewTracerProvider())
	otel.SetMeterProvider(metricnoop.NewMeterProvider())
}

// sdkDisabled reads OTEL_SDK_DISABLED. An unparseable value is treated as "not
// disabled" and logged: silently switching telemetry off because of a typo is
// worse than leaving it on.
func sdkDisabled(log logr.Logger) bool {
	raw := os.Getenv(sdkDisabledEnvKey)
	if raw == "" {
		return false
	}

	disabled, err := strconv.ParseBool(raw)
	if err != nil {
		log.Info("Ignoring unparseable "+sdkDisabledEnvKey, "value", raw)
		return false
	}
	return disabled
}
