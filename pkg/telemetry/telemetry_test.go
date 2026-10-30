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
	"strings"
	"testing"

	"github.com/go-logr/logr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
)

// recordingSink captures log output so tests can assert that a
// misconfiguration was actually reported. Every fallback in this package is
// silent in its effect, so "did it warn" is the only observable difference
// between a deliberate default and a swallowed typo.
type recordingSink struct {
	messages []string
}

func (*recordingSink) Init(logr.RuntimeInfo)                 {}
func (*recordingSink) Enabled(int) bool                      { return true }
func (s *recordingSink) Info(_ int, msg string, _ ...any)    { s.messages = append(s.messages, msg) }
func (s *recordingSink) Error(_ error, msg string, _ ...any) { s.messages = append(s.messages, msg) }
func (s *recordingSink) WithValues(_ ...any) logr.LogSink    { return s }
func (s *recordingSink) WithName(_ string) logr.LogSink      { return s }

func (s *recordingSink) logged(substr string) bool {
	for _, msg := range s.messages {
		if strings.Contains(msg, substr) {
			return true
		}
	}
	return false
}

func newRecordingLogger() (logr.Logger, *recordingSink) {
	sink := &recordingSink{}
	return logr.New(sink), sink
}

func TestSamplerFromEnv(t *testing.T) {
	tests := []struct {
		name            string
		sampler         string
		samplerArg      string
		defaultRatio    float64
		wantDescription string
		wantWarning     string
	}{
		{
			name:            "unset falls back to the caller's ratio",
			defaultRatio:    0.1,
			wantDescription: "ParentBased{root:TraceIDRatioBased{0.1}",
		},
		{
			name:            "always_on",
			sampler:         "always_on",
			defaultRatio:    0.1,
			wantDescription: "AlwaysOnSampler",
		},
		{
			name:            "always_off",
			sampler:         "always_off",
			defaultRatio:    1,
			wantDescription: "AlwaysOffSampler",
		},
		{
			name:            "traceidratio honours its argument",
			sampler:         "traceidratio",
			samplerArg:      "0.25",
			defaultRatio:    1,
			wantDescription: "TraceIDRatioBased{0.25}",
		},
		{
			name:            "parentbased_always_on",
			sampler:         "parentbased_always_on",
			defaultRatio:    0.1,
			wantDescription: "ParentBased{root:AlwaysOnSampler",
		},
		{
			name:            "parentbased_always_off",
			sampler:         "parentbased_always_off",
			defaultRatio:    1,
			wantDescription: "ParentBased{root:AlwaysOffSampler",
		},
		{
			name:            "parentbased_traceidratio honours its argument",
			sampler:         "parentbased_traceidratio",
			samplerArg:      "0.5",
			defaultRatio:    0.1,
			wantDescription: "ParentBased{root:TraceIDRatioBased{0.5}",
		},
		{
			// The regression that matters: the SDK's own behaviour on an
			// unrecognised value is to sample everything, which is the exact
			// opposite of what someone typing "always-off" intends.
			name:            "unrecognised sampler does not become always-on",
			sampler:         "always-off",
			defaultRatio:    0.1,
			wantDescription: "ParentBased{root:TraceIDRatioBased{0.1}",
			wantWarning:     "Unrecognised OTEL_TRACES_SAMPLER",
		},
		{
			name:            "unparseable argument falls back to the default ratio",
			sampler:         "traceidratio",
			samplerArg:      "not-a-number",
			defaultRatio:    0.3,
			wantDescription: "TraceIDRatioBased{0.3}",
			wantWarning:     "Unparseable OTEL_TRACES_SAMPLER_ARG",
		},
		{
			// ParseFloat accepts "NaN" with a nil error, and NaN compares
			// false against every relational operator, so a range check alone
			// would let it through.
			name:            "NaN argument samples nothing",
			sampler:         "traceidratio",
			samplerArg:      "NaN",
			defaultRatio:    1,
			wantDescription: "TraceIDRatioBased{0}",
			wantWarning:     "not a number",
		},
		{
			name:            "argument above 1 is clamped",
			sampler:         "traceidratio",
			samplerArg:      "2",
			defaultRatio:    0.1,
			wantDescription: "TraceIDRatioBased{1}",
			wantWarning:     "clamping",
		},
		{
			name:            "negative argument is clamped",
			sampler:         "traceidratio",
			samplerArg:      "-1",
			defaultRatio:    0.1,
			wantDescription: "TraceIDRatioBased{0}",
			wantWarning:     "clamping",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(samplerEnvKey, tt.sampler)
			t.Setenv(samplerArgEnvKey, tt.samplerArg)

			log, sink := newRecordingLogger()
			sampler := samplerFromEnv(tt.defaultRatio, log)

			assert.Contains(t, sampler.Description(), tt.wantDescription)

			if tt.wantWarning == "" {
				assert.Empty(t, sink.messages, "a valid configuration should not warn")
				return
			}
			assert.True(t, sink.logged(tt.wantWarning),
				"expected a warning containing %q, got %v", tt.wantWarning, sink.messages)
		})
	}
}

func TestResolveExporter(t *testing.T) {
	tests := []struct {
		name            string
		exporter        string
		signalEndpoint  string
		genericEndpoint string
		want            string
	}{
		{
			name: "nothing configured means telemetry stays off",
			want: exporterNone,
		},
		{
			name:            "a generic endpoint implies OTLP",
			genericEndpoint: "http://collector:4317",
			want:            exporterOTLP,
		},
		{
			name:           "a signal-specific endpoint implies OTLP",
			signalEndpoint: "http://collector:4317",
			want:           exporterOTLP,
		},
		{
			name:     "an explicit exporter wins over no endpoint",
			exporter: exporterConsole,
			want:     exporterConsole,
		},
		{
			name:            "an explicit none wins over a configured endpoint",
			exporter:        exporterNone,
			genericEndpoint: "http://collector:4317",
			want:            exporterNone,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(tracesExporterEnvKey, tt.exporter)
			t.Setenv(otlpTracesEndpointEnvKey, tt.signalEndpoint)
			t.Setenv(otlpEndpointEnvKey, tt.genericEndpoint)

			assert.Equal(t, tt.want, resolveExporter(tracesExporterEnvKey, otlpTracesEndpointEnvKey))
		})
	}
}

func TestSDKDisabled(t *testing.T) {
	tests := []struct {
		name        string
		value       string
		want        bool
		wantWarning bool
	}{
		{name: "unset", value: "", want: false},
		{name: "true", value: "true", want: true},
		{name: "false", value: "false", want: false},
		{name: "1", value: "1", want: true},
		{name: "0", value: "0", want: false},
		{
			// Failing open is deliberate: silently switching telemetry off
			// because of a typo is worse than leaving it on.
			name:        "unparseable leaves telemetry enabled and warns",
			value:       "yes-please",
			want:        false,
			wantWarning: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(sdkDisabledEnvKey, tt.value)

			log, sink := newRecordingLogger()
			assert.Equal(t, tt.want, sdkDisabled(log))
			assert.Equal(t, tt.wantWarning, sink.logged("Ignoring unparseable"))
		})
	}
}

func TestWarnOnSchemelessEndpoint(t *testing.T) {
	tests := []struct {
		name        string
		endpoint    string
		wantWarning bool
	}{
		{name: "unset", endpoint: "", wantWarning: false},
		{name: "http URL", endpoint: "http://collector.observability.svc:4317", wantWarning: false},
		{name: "https URL", endpoint: "https://collector.observability.svc:4317", wantWarning: false},
		{
			// url.Parse reads this as scheme "collector.observability.svc"
			// with opaque "4317" and returns no error, so a non-empty-scheme
			// check would wave it through. The exporter rejects it and
			// silently falls back to localhost.
			name:        "bare host:port is reported",
			endpoint:    "collector.observability.svc:4317",
			wantWarning: true,
		},
		{name: "bare IP:port is reported", endpoint: "127.0.0.1:4317", wantWarning: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv(otlpEndpointEnvKey, tt.endpoint)
			t.Setenv(otlpTracesEndpointEnvKey, "")
			t.Setenv(otlpMetricsEndpointEnvKey, "")

			log, sink := newRecordingLogger()
			warnOnSchemelessEndpoint(log)

			assert.Equal(t, tt.wantWarning, sink.logged("no http:// or https:// scheme"))
		})
	}
}

func TestNewResource(t *testing.T) {
	tests := []struct {
		name               string
		serviceName        string
		serviceVersion     string
		envServiceName     string
		envAttributes      string
		extraAttributes    []attribute.KeyValue
		wantServiceName    string
		wantServiceVersion string
		wantExtra          map[string]string
	}{
		{
			name:               "config supplies service identity",
			serviceName:        "from-config",
			serviceVersion:     "v1.2.3",
			wantServiceName:    "from-config",
			wantServiceVersion: "v1.2.3",
		},
		{
			// Operators must be able to retag a running deployment without a
			// rebuild, so the environment is applied last and wins.
			name:            "OTEL_SERVICE_NAME overrides the compiled-in name",
			serviceName:     "from-config",
			envServiceName:  "from-env",
			wantServiceName: "from-env",
		},
		{
			name:            "OTEL_RESOURCE_ATTRIBUTES is merged in",
			serviceName:     "from-config",
			envAttributes:   "deployment.environment.name=preprod",
			wantServiceName: "from-config",
			wantExtra:       map[string]string{"deployment.environment.name": "preprod"},
		},
		{
			name:            "caller-supplied attributes are kept",
			serviceName:     "from-config",
			extraAttributes: []attribute.KeyValue{attribute.String("udc.custom", "value")},
			wantServiceName: "from-config",
			wantExtra:       map[string]string{"udc.custom": "value"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Setenv("OTEL_SERVICE_NAME", tt.envServiceName)
			t.Setenv("OTEL_RESOURCE_ATTRIBUTES", tt.envAttributes)

			log, _ := newRecordingLogger()
			res, err := newResource(context.Background(), Config{
				ServiceName:        tt.serviceName,
				ServiceVersion:     tt.serviceVersion,
				ResourceAttributes: tt.extraAttributes,
				Logger:             log,
			})
			require.NoError(t, err)

			attrs := attributeMap(res.Attributes())
			assert.Equal(t, tt.wantServiceName, attrs["service.name"])

			if tt.wantServiceVersion != "" {
				assert.Equal(t, tt.wantServiceVersion, attrs["service.version"])
			}
			for key, want := range tt.wantExtra {
				assert.Equal(t, want, attrs[key])
			}

			// Always present, and the only per-replica identifier the backends
			// get from the process itself.
			assert.NotEmpty(t, attrs["service.instance.id"])
		})
	}
}

func attributeMap(attrs []attribute.KeyValue) map[string]string {
	out := make(map[string]string, len(attrs))
	for _, attr := range attrs {
		out[string(attr.Key)] = attr.Value.String()
	}
	return out
}
