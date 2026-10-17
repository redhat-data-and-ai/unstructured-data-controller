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

package controllerutils

import (
	"os"
	"strconv"
)

const (
	// Default number of concurrent reconcile workers per controller.
	// Set to 5 based on production patterns from Crossplane (10), Flux (2-4),
	// and sizing heuristic: (expected CRs) / (avg reconcile latency in seconds).
	DefaultReconcilerConcurrency = 5

	apiGroup = "operator.dataverse.redhat.com"
)

// Environment variable names for per-controller concurrency overrides.
const (
	EnvConcurrencyUnstructuredDataPipeline  = "CONCURRENCY_UNSTRUCTURED_DATA_PIPELINE"
	EnvConcurrencyDocumentProcessor         = "CONCURRENCY_DOCUMENT_PROCESSOR"
	EnvConcurrencyChunksGenerator           = "CONCURRENCY_CHUNKS_GENERATOR"
	EnvConcurrencyVectorEmbeddingsGenerator = "CONCURRENCY_VECTOR_EMBEDDINGS_GENERATOR"
	EnvConcurrencySourceCrawler             = "CONCURRENCY_SOURCE_CRAWLER"
	EnvConcurrencyDestinationSyncer         = "CONCURRENCY_DESTINATION_SYNCER"
)

// concurrencyFromEnv reads a concurrency value from an environment variable,
// falling back to DefaultReconcilerConcurrency if the variable is unset or invalid.
func concurrencyFromEnv(envVar string) int {
	if val := os.Getenv(envVar); val != "" {
		if n, err := strconv.Atoi(val); err == nil && n > 0 {
			return n
		}
	}
	return DefaultReconcilerConcurrency
}

// BuildGroupKindConcurrency builds a GroupKindConcurrency map for controller-runtime's
// config.Controller. Each controller defaults to DefaultReconcilerConcurrency (5) and
// can be overridden via environment variables (e.g., CONCURRENCY_DOCUMENT_PROCESSOR=10).
func BuildGroupKindConcurrency() map[string]int {
	return map[string]int{
		"UnstructuredDataPipeline." + apiGroup:  concurrencyFromEnv(EnvConcurrencyUnstructuredDataPipeline),
		"DocumentProcessor." + apiGroup:         concurrencyFromEnv(EnvConcurrencyDocumentProcessor),
		"ChunksGenerator." + apiGroup:           concurrencyFromEnv(EnvConcurrencyChunksGenerator),
		"VectorEmbeddingsGenerator." + apiGroup: concurrencyFromEnv(EnvConcurrencyVectorEmbeddingsGenerator),
		"SourceCrawler." + apiGroup:             concurrencyFromEnv(EnvConcurrencySourceCrawler),
		"DestinationSyncer." + apiGroup:         concurrencyFromEnv(EnvConcurrencyDestinationSyncer),
	}
}
