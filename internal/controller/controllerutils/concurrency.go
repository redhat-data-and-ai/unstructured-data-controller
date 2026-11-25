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

	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// Default number of concurrent reconcile workers per controller.
	DefaultReconcilerConcurrency = 5
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

// ConcurrencyOverride returns the worker count set in envVar, or 0 when it is
// unset or not a positive integer. Zero makes controller-runtime use the
// manager-wide DefaultReconcilerConcurrency.
func ConcurrencyOverride(envVar string) int {
	value := os.Getenv(envVar)
	if value == "" {
		return 0
	}
	workers, err := strconv.Atoi(value)
	if err != nil || workers <= 0 {
		log.Log.Info("ignoring invalid concurrency env var, using default",
			"envVar", envVar, "value", value, "default", DefaultReconcilerConcurrency)
		return 0
	}
	return workers
}
