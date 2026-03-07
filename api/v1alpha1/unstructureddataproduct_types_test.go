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

import "testing"

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
