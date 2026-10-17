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

package main

import (
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	mcptools "github.com/redhat-data-and-ai/unstructured-data-controller/internal/mcp/tools"
)

func TestResolveRerankerPoolDepth(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want int
	}{
		{"unset uses the default", "", mcptools.DefaultRerankerPool},
		{"a valid value is honoured", "35", 35},
		{"the maximum is honoured", strconv.Itoa(mcptools.MaxRerankerPool), mcptools.MaxRerankerPool},
		{"above the maximum is clamped", "5000", mcptools.MaxRerankerPool},
		{"zero falls back to the default", "0", mcptools.DefaultRerankerPool},
		{"negative falls back to the default", "-10", mcptools.DefaultRerankerPool},
		{"non-numeric falls back to the default", "fifty", mcptools.DefaultRerankerPool},
		{"empty-ish garbage falls back to the default", "  ", mcptools.DefaultRerankerPool},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, resolveRerankerPoolDepth(tt.raw))
		})
	}
}

// A bad value must never fail startup: reranking is best-effort and degrades
// to cosine ordering, so a misconfigured pool should not take the server down.
func TestResolveRerankerPoolDepthAlwaysReturnsUsableDepth(t *testing.T) {
	for _, raw := range []string{"", "0", "-1", "abc", "99999", "1"} {
		got := resolveRerankerPoolDepth(raw)
		assert.Positive(t, got, "input %q produced a non-positive pool", raw)
		assert.LessOrEqual(t, got, mcptools.MaxRerankerPool, "input %q exceeded the maximum", raw)
	}
}
