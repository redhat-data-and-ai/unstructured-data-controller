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
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

func TestConcurrencyFromEnv(t *testing.T) {
	tests := []struct {
		name     string
		envVar   string
		envValue string
		setEnv   bool
		expected int
	}{
		{
			name:     "env not set returns default",
			envVar:   "TEST_CONCURRENCY_UNSET",
			setEnv:   false,
			expected: DefaultReconcilerConcurrency,
		},
		{
			name:     "valid positive integer",
			envVar:   "TEST_CONCURRENCY_VALID",
			envValue: "10",
			setEnv:   true,
			expected: 10,
		},
		{
			name:     "value of 1 is accepted",
			envVar:   "TEST_CONCURRENCY_ONE",
			envValue: "1",
			setEnv:   true,
			expected: 1,
		},
		{
			name:     "zero falls back to default",
			envVar:   "TEST_CONCURRENCY_ZERO",
			envValue: "0",
			setEnv:   true,
			expected: DefaultReconcilerConcurrency,
		},
		{
			name:     "negative falls back to default",
			envVar:   "TEST_CONCURRENCY_NEGATIVE",
			envValue: "-1",
			setEnv:   true,
			expected: DefaultReconcilerConcurrency,
		},
		{
			name:     "non-numeric string falls back to default",
			envVar:   "TEST_CONCURRENCY_INVALID",
			envValue: "ten",
			setEnv:   true,
			expected: DefaultReconcilerConcurrency,
		},
		{
			name:     "empty string falls back to default",
			envVar:   "TEST_CONCURRENCY_EMPTY",
			envValue: "",
			setEnv:   true,
			expected: DefaultReconcilerConcurrency,
		},
		{
			name:     "surrounding whitespace falls back to default",
			envVar:   "TEST_CONCURRENCY_WHITESPACE",
			envValue: " 10",
			setEnv:   true,
			expected: DefaultReconcilerConcurrency,
		},
		{
			name:     "float value falls back to default",
			envVar:   "TEST_CONCURRENCY_FLOAT",
			envValue: "3.5",
			setEnv:   true,
			expected: DefaultReconcilerConcurrency,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.setEnv {
				t.Setenv(tt.envVar, tt.envValue)
			}
			result := concurrencyFromEnv(tt.envVar)
			if result != tt.expected {
				t.Errorf("concurrencyFromEnv(%q) = %d, want %d", tt.envVar, result, tt.expected)
			}
		})
	}
}

// controllerKeys returns the GroupKindConcurrency key controller-runtime looks
// up for each stage controller's For() type, keyed by its override env var.
func controllerKeys(t *testing.T) map[string]string {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := operatorv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("building scheme: %v", err)
	}

	objects := map[string]client.Object{
		EnvConcurrencyUnstructuredDataPipeline:  &operatorv1alpha1.UnstructuredDataPipeline{},
		EnvConcurrencyDocumentProcessor:         &operatorv1alpha1.DocumentProcessor{},
		EnvConcurrencyChunksGenerator:           &operatorv1alpha1.ChunksGenerator{},
		EnvConcurrencyVectorEmbeddingsGenerator: &operatorv1alpha1.VectorEmbeddingsGenerator{},
		EnvConcurrencySourceCrawler:             &operatorv1alpha1.SourceCrawler{},
		EnvConcurrencyDestinationSyncer:         &operatorv1alpha1.DestinationSyncer{},
	}
	keys := make(map[string]string, len(objects))
	for envVar, object := range objects {
		gvk, err := apiutil.GVKForObject(object, scheme)
		if err != nil {
			t.Fatalf("resolving GVK for %T: %v", object, err)
		}
		keys[envVar] = gvk.GroupKind().String()
	}
	return keys
}

// clearOverrides empties every override, so an override set in the test
// process's own environment cannot leak into an assertion about defaults.
func clearOverrides(t *testing.T, keys map[string]string) {
	t.Helper()
	for envVar := range keys {
		t.Setenv(envVar, "")
	}
}

func TestBuildGroupKindConcurrencyDefaults(t *testing.T) {
	keys := controllerKeys(t)
	clearOverrides(t, keys)
	result := BuildGroupKindConcurrency()

	if len(result) != len(keys) {
		t.Errorf("got %d entries, want %d: %v", len(result), len(keys), result)
	}
	for envVar, key := range keys {
		got, ok := result[key]
		if !ok {
			t.Errorf("no entry under %q, so controller-runtime would run it with 1 worker (env %s)", key, envVar)
			continue
		}
		if got != DefaultReconcilerConcurrency {
			t.Errorf("%s = %d, want default %d", key, got, DefaultReconcilerConcurrency)
		}
	}
}

func TestBuildGroupKindConcurrencyOverridesOnlyItsOwnController(t *testing.T) {
	keys := controllerKeys(t)
	for envVar := range keys {
		t.Run(envVar, func(t *testing.T) {
			clearOverrides(t, keys)
			t.Setenv(envVar, "17")
			result := BuildGroupKindConcurrency()

			for otherEnvVar, otherKey := range keys {
				want := DefaultReconcilerConcurrency
				if otherEnvVar == envVar {
					want = 17
				}
				if result[otherKey] != want {
					t.Errorf("with %s=17, %s = %d, want %d", envVar, otherKey, result[otherKey], want)
				}
			}
		})
	}
}
