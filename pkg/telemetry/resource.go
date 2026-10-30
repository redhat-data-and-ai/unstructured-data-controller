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
	"errors"
	"fmt"
	"os"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.43.0"
)

// newResource describes this process to the backends.
//
// Kubernetes attributes (k8s.pod.name, k8s.namespace.name, k8s.deployment.name
// and friends) are deliberately absent. The Collector's k8sattributes
// processor derives them from the connection, where it can also see labels and
// annotations the pod cannot report about itself. Duplicating that here would
// only create two sources of truth that disagree.
func newResource(ctx context.Context, cfg Config) (*resource.Resource, error) {
	attrs := make([]attribute.KeyValue, 0, len(cfg.ResourceAttributes)+3)
	attrs = append(attrs, semconv.ServiceName(cfg.ServiceName))

	if cfg.ServiceVersion != "" {
		attrs = append(attrs, semconv.ServiceVersion(cfg.ServiceVersion))
	}

	// In Kubernetes the hostname is the pod name, which is the natural
	// per-replica identity and needs no downward-API plumbing to obtain.
	if hostname, err := os.Hostname(); err == nil && hostname != "" {
		attrs = append(attrs, semconv.ServiceInstanceID(hostname))
	}

	attrs = append(attrs, cfg.ResourceAttributes...)

	// Order matters: resource.New merges detectors left to right and later
	// values win, so WithFromEnv goes last. That lets an operator override
	// anything here (service.name, deployment.environment.name, ...) through
	// OTEL_SERVICE_NAME and OTEL_RESOURCE_ATTRIBUTES without a rebuild.
	res, err := resource.New(ctx,
		resource.WithAttributes(attrs...),
		resource.WithTelemetrySDK(),
		resource.WithProcessRuntimeName(),
		resource.WithProcessRuntimeVersion(),
		resource.WithFromEnv(),
	)

	// A schema URL conflict means some detector reported a different semconv
	// version. The merged resource is still usable and losing all telemetry
	// over a version skew would be a bad trade, so treat it as non-fatal.
	if errors.Is(err, resource.ErrSchemaURLConflict) {
		cfg.Logger.Info("Telemetry resource has conflicting schema URLs, continuing", "error", err.Error())
		return res, nil
	}
	if err != nil {
		return nil, fmt.Errorf("detecting resource attributes: %w", err)
	}

	return res, nil
}
