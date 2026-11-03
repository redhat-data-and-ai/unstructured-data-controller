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

func TestBuildGroupKindConcurrency(t *testing.T) {
	t.Run("returns all 6 controllers with defaults when no env vars set", func(t *testing.T) {
		result := BuildGroupKindConcurrency()

		expectedKeys := []string{
			"UnstructuredDataPipeline." + apiGroup,
			"DocumentProcessor." + apiGroup,
			"ChunksGenerator." + apiGroup,
			"VectorEmbeddingsGenerator." + apiGroup,
			"SourceCrawler." + apiGroup,
			"DestinationSyncer." + apiGroup,
		}

		if len(result) != len(expectedKeys) {
			t.Errorf("expected %d entries, got %d", len(expectedKeys), len(result))
		}

		for _, key := range expectedKeys {
			val, ok := result[key]
			if !ok {
				t.Errorf("missing key %q", key)
				continue
			}
			if val != DefaultReconcilerConcurrency {
				t.Errorf("key %q = %d, want default %d", key, val, DefaultReconcilerConcurrency)
			}
		}
	})

	t.Run("uses correct GroupKind format (Kind.group)", func(t *testing.T) {
		result := BuildGroupKindConcurrency()

		// controller-runtime expects "Kind.group" format
		expectedGroup := "operator.dataverse.redhat.com"
		for key := range result {
			if len(key) < len(expectedGroup) {
				t.Errorf("key %q too short to contain group", key)
				continue
			}
			suffix := key[len(key)-len(expectedGroup):]
			if suffix != expectedGroup {
				t.Errorf("key %q does not end with %q", key, expectedGroup)
			}
		}
	})

	t.Run("env var overrides default for a single controller", func(t *testing.T) {
		t.Setenv(EnvConcurrencyDocumentProcessor, "20")

		result := BuildGroupKindConcurrency()

		dpKey := "DocumentProcessor." + apiGroup
		if result[dpKey] != 20 {
			t.Errorf("DocumentProcessor concurrency = %d, want 20", result[dpKey])
		}

		// Other controllers should still use the default
		scKey := "SourceCrawler." + apiGroup
		if result[scKey] != DefaultReconcilerConcurrency {
			t.Errorf("SourceCrawler concurrency = %d, want default %d", result[scKey], DefaultReconcilerConcurrency)
		}
	})
}
