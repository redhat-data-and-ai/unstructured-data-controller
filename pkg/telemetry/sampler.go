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
	"math"
	"os"
	"strconv"

	"github.com/go-logr/logr"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const (
	samplerEnvKey    = "OTEL_TRACES_SAMPLER"
	samplerArgEnvKey = "OTEL_TRACES_SAMPLER_ARG"
)

// samplerFromEnv resolves OTEL_TRACES_SAMPLER, falling back to a parent-based
// ratio sampler at defaultRatio.
//
// The Go SDK does not read these variables itself, so we parse them. Every
// unrecognised or unparseable input is logged rather than silently accepted:
// the failure mode that matters is a typo such as "always-off" quietly
// turning into 100% sampling and flooding the Collector.
func samplerFromEnv(defaultRatio float64, log logr.Logger) sdktrace.Sampler {
	name := os.Getenv(samplerEnvKey)
	if name == "" {
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(clampRatio(defaultRatio, log)))
	}

	switch name {
	case "always_on":
		return sdktrace.AlwaysSample()
	case "always_off":
		return sdktrace.NeverSample()
	case "traceidratio":
		return sdktrace.TraceIDRatioBased(ratioFromEnv(defaultRatio, log))
	case "parentbased_always_on":
		return sdktrace.ParentBased(sdktrace.AlwaysSample())
	case "parentbased_always_off":
		return sdktrace.ParentBased(sdktrace.NeverSample())
	case "parentbased_traceidratio":
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(ratioFromEnv(defaultRatio, log)))
	default:
		log.Info("Unrecognised "+samplerEnvKey+", using the configured default",
			"value", name, "defaultRatio", defaultRatio)
		return sdktrace.ParentBased(sdktrace.TraceIDRatioBased(clampRatio(defaultRatio, log)))
	}
}

// ratioFromEnv reads OTEL_TRACES_SAMPLER_ARG, falling back to defaultRatio.
func ratioFromEnv(defaultRatio float64, log logr.Logger) float64 {
	raw := os.Getenv(samplerArgEnvKey)
	if raw == "" {
		return clampRatio(defaultRatio, log)
	}

	ratio, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		log.Info("Unparseable "+samplerArgEnvKey+", using the configured default",
			"value", raw, "defaultRatio", defaultRatio)
		return clampRatio(defaultRatio, log)
	}

	return clampRatio(ratio, log)
}

// clampRatio bounds a sampling ratio to [0,1].
//
// ParseFloat accepts "NaN" and "Inf" with a nil error, and NaN compares false
// against every relational operator, so it is checked explicitly rather than
// by range alone. An out-of-range ratio is clamped instead of rejected: the
// intent of "2" is obviously "sample everything".
func clampRatio(ratio float64, log logr.Logger) float64 {
	if math.IsNaN(ratio) {
		log.Info("Sampling ratio is not a number, sampling nothing")
		return 0
	}
	if ratio < 0 {
		log.Info("Sampling ratio below 0, clamping", "ratio", ratio)
		return 0
	}
	if ratio > 1 {
		log.Info("Sampling ratio above 1, clamping", "ratio", ratio)
		return 1
	}
	return ratio
}
