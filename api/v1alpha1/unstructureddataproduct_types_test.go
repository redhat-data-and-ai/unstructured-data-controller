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

func TestValidateSpec(t *testing.T) {
	tests := []struct {
		name    string
		spec    UnstructuredDataProductSpec
		wantErr bool
	}{
		{
			name: "docling hierarchical with DocumentProcessorConfig set returns error",
			spec: UnstructuredDataProductSpec{
				ChunksGeneratorConfig: ChunksGeneratorConfig{
					Strategy: ChunkingStrategyDoclingHierarchical,
				},
				DocumentProcessorConfig: &DocumentProcessorConfig{
					Type: "docling",
				},
			},
			wantErr: true,
		},
		{
			name: "docling hybrid with DocumentProcessorConfig set returns error",
			spec: UnstructuredDataProductSpec{
				ChunksGeneratorConfig: ChunksGeneratorConfig{
					Strategy: ChunkingStrategyDoclingHybrid,
				},
				DocumentProcessorConfig: &DocumentProcessorConfig{
					Type: "docling",
				},
			},
			wantErr: true,
		},
		{
			name: "docling hierarchical without DocumentProcessorConfig returns no error",
			spec: UnstructuredDataProductSpec{
				ChunksGeneratorConfig: ChunksGeneratorConfig{
					Strategy: ChunkingStrategyDoclingHierarchical,
				},
			},
			wantErr: false,
		},
		{
			name: "docling hybrid without DocumentProcessorConfig returns no error",
			spec: UnstructuredDataProductSpec{
				ChunksGeneratorConfig: ChunksGeneratorConfig{
					Strategy: ChunkingStrategyDoclingHybrid,
				},
			},
			wantErr: false,
		},
		{
			name: "non-docling strategy with DocumentProcessorConfig set returns no error",
			spec: UnstructuredDataProductSpec{
				ChunksGeneratorConfig: ChunksGeneratorConfig{
					Strategy: ChunkingStrategyRecursiveCharacter,
				},
				DocumentProcessorConfig: &DocumentProcessorConfig{
					Type: "docling",
				},
			},
			wantErr: false,
		},
		{
			name: "non-docling strategy without DocumentProcessorConfig returns no error",
			spec: UnstructuredDataProductSpec{
				ChunksGeneratorConfig: ChunksGeneratorConfig{
					Strategy: ChunkingStrategyMarkdown,
				},
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.spec.ValidateSpec()
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSpec() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestIsDoclingChunkingStrategy(t *testing.T) {
	tests := []struct {
		name     string
		strategy ChunkingStrategy
		want     bool
	}{
		{
			name:     "docling hierarchical",
			strategy: ChunkingStrategyDoclingHierarchical,
			want:     true,
		},
		{
			name:     "docling hybrid",
			strategy: ChunkingStrategyDoclingHybrid,
			want:     true,
		},
		{
			name:     "recursive character",
			strategy: ChunkingStrategyRecursiveCharacter,
			want:     false,
		},
		{
			name:     "markdown",
			strategy: ChunkingStrategyMarkdown,
			want:     false,
		},
		{
			name:     "token",
			strategy: ChunkingStrategyToken,
			want:     false,
		},
		{
			name:     "empty string",
			strategy: "",
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsDoclingChunkingStrategy(tt.strategy)
			if got != tt.want {
				t.Errorf("IsDoclingChunkingStrategy(%q) = %v, want %v", tt.strategy, got, tt.want)
			}
		})
	}
}
