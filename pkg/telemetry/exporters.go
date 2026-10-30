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

package telemetry

import (
	"context"
	"fmt"
	"net/url"
	"os"

	"github.com/go-logr/logr"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// Exporter selection follows the OpenTelemetry environment specification, so
// operators configure this the same way they configure any other OTel service.
//
// These are hand-rolled rather than delegated to contrib's autoexport for two
// reasons. First, autoexport defaults OTLP to http/protobuf, while the
// deployment topology here is OTLP/gRPC to a Collector. Second, autoexport
// attaches metric producers through package-level global state keyed on
// OTEL_METRICS_PRODUCERS, so an unrelated environment variable could silently
// drop the controller-runtime bridge, which is the main thing this package
// exists to deliver.
const (
	tracesExporterEnvKey  = "OTEL_TRACES_EXPORTER"
	metricsExporterEnvKey = "OTEL_METRICS_EXPORTER"

	otlpEndpointEnvKey        = "OTEL_EXPORTER_OTLP_ENDPOINT"
	otlpTracesEndpointEnvKey  = "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT"
	otlpMetricsEndpointEnvKey = "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT"

	exporterOTLP    = "otlp"
	exporterConsole = "console"
	exporterNone    = "none"
)

// newSpanExporter returns nil when tracing is not configured, which tells the
// caller to install no TracerProvider at all.
func newSpanExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	switch kind := resolveExporter(tracesExporterEnvKey, otlpTracesEndpointEnvKey); kind {
	case exporterNone:
		return nil, nil
	case exporterConsole:
		exporter, err := stdouttrace.New()
		if err != nil {
			return nil, fmt.Errorf("creating stdout trace exporter: %w", err)
		}
		return exporter, nil
	case exporterOTLP:
		exporter, err := otlptracegrpc.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating OTLP/gRPC trace exporter: %w", err)
		}
		return exporter, nil
	default:
		return nil, fmt.Errorf("unsupported %s value %q: want otlp, console or none", tracesExporterEnvKey, kind)
	}
}

// newMetricReader returns nil when metrics are not configured. The producer,
// when non-nil, is attached to whichever reader is built so that bridged
// metrics follow the same path as native ones.
func newMetricReader(ctx context.Context, producer sdkmetric.Producer) (sdkmetric.Reader, error) {
	var opts []sdkmetric.PeriodicReaderOption
	if producer != nil {
		opts = append(opts, sdkmetric.WithProducer(producer))
	}

	switch kind := resolveExporter(metricsExporterEnvKey, otlpMetricsEndpointEnvKey); kind {
	case exporterNone:
		return nil, nil
	case exporterConsole:
		exporter, err := stdoutmetric.New()
		if err != nil {
			return nil, fmt.Errorf("creating stdout metric exporter: %w", err)
		}
		return sdkmetric.NewPeriodicReader(exporter, opts...), nil
	case exporterOTLP:
		exporter, err := otlpmetricgrpc.New(ctx)
		if err != nil {
			return nil, fmt.Errorf("creating OTLP/gRPC metric exporter: %w", err)
		}
		return sdkmetric.NewPeriodicReader(exporter, opts...), nil
	default:
		return nil, fmt.Errorf("unsupported %s value %q: want otlp, console or none", metricsExporterEnvKey, kind)
	}
}

// otlpEndpointEnvKeys are the endpoint variables checked by warnOnSchemelessEndpoint.
var otlpEndpointEnvKeys = []string{
	otlpEndpointEnvKey,
	otlpTracesEndpointEnvKey,
	otlpMetricsEndpointEnvKey,
}

// warnOnSchemelessEndpoint catches the most common way to misconfigure OTLP.
//
// The specification requires these variables to hold a full URL. Given a bare
// "collector.svc:4317" the exporter cannot parse it, reports the failure only
// through the SDK's own internal debug logger, and falls back to
// localhost:4317. The result is the worst kind of failure: telemetry looks
// configured, nothing obviously errors, and no data ever arrives.
//
// This only warns. Which scheme was intended is not knowable from here, and
// refusing to start a healthy operator over a telemetry setting would be a bad
// trade.
func warnOnSchemelessEndpoint(log logr.Logger) {
	for _, key := range otlpEndpointEnvKeys {
		value := os.Getenv(key)
		if value == "" {
			continue
		}

		// Checking for a non-empty scheme is not enough: url.Parse reads
		// "collector.svc:4317" as scheme "collector.svc" with opaque "4317",
		// because every character in it is scheme-legal. Only http and https
		// are actually accepted by the exporters.
		parsed, err := url.Parse(value)
		if err == nil && (parsed.Scheme == "http" || parsed.Scheme == "https") {
			continue
		}

		log.Info("OTLP endpoint has no http:// or https:// scheme and will be ignored, "+
			"falling back to localhost:4317",
			"variable", key, "value", value)
	}
}

// resolveExporter decides which exporter to build for one signal.
//
// With nothing configured at all the answer is "none". Defaulting to OTLP
// instead would point the exporter at localhost:4317 and fill the logs of
// every developer and every cluster without a Collector with connection
// errors, so telemetry stays off until an endpoint says otherwise.
func resolveExporter(exporterEnvKey, signalEndpointEnvKey string) string {
	if kind := os.Getenv(exporterEnvKey); kind != "" {
		return kind
	}

	if os.Getenv(signalEndpointEnvKey) != "" || os.Getenv(otlpEndpointEnvKey) != "" {
		return exporterOTLP
	}
	return exporterNone
}
