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

package controller

import udcmetrics "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/metrics"

// metricsProvider is set at startup via SetMetricsProvider and used by all
// reconcilers to record reconcile duration, error counts, and files processed.
var metricsProvider *udcmetrics.Provider

// SetMetricsProvider configures the metrics provider for all controllers.
// Must be called before the manager starts.
func SetMetricsProvider(p *udcmetrics.Provider) {
	metricsProvider = p
}
