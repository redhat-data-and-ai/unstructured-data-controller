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

package v1alpha1

import (
	"testing"
)

func TestVectorEmbeddingsGeneratorConfig_SetDefaults_EmptyConfig(t *testing.T) {
	c := VectorEmbeddingsGeneratorConfig{}
	c.SetDefaults()

	if c.ModelName != DefaultEmbeddingModelName {
		t.Errorf("expected modelName %q, got %q", DefaultEmbeddingModelName, c.ModelName)
	}
	if c.BatchSize != DefaultEmbeddingBatchSize {
		t.Errorf("expected batchSize %d, got %d", DefaultEmbeddingBatchSize, c.BatchSize)
	}
}

func TestVectorEmbeddingsGeneratorConfig_SetDefaults_PreservesExplicitValues(t *testing.T) {
	c := VectorEmbeddingsGeneratorConfig{
		ModelName: "custom-model",
		BatchSize: 500,
	}
	c.SetDefaults()

	if c.ModelName != "custom-model" {
		t.Errorf("expected modelName %q, got %q", "custom-model", c.ModelName)
	}
	if c.BatchSize != 500 {
		t.Errorf("expected batchSize %d, got %d", 500, c.BatchSize)
	}
}
