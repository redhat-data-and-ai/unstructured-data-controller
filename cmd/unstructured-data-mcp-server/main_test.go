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
	"context"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	mcptools "github.com/redhat-data-and-ai/unstructured-data-controller/internal/mcp/tools"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
)

// Drive search is the one tool with no second gate behind it: it runs as a
// service account that can read the whole corpus, so the caller's identity is
// the only thing narrowing results. Each case below is a reason that identity
// or the group lookup cannot be trusted, and each must switch the tool off
// rather than weaken it.
// stubIdentifier stands in for the middleware, which these cases do not
// exercise: they only check whether the tool registers at all.
type stubIdentifier struct{}

func (stubIdentifier) Identify(_ context.Context, _ string) (*auth.IntrospectionResponse, error) {
	return &auth.IntrospectionResponse{Active: true, Sub: "someone"}, nil
}

// Drive search runs as a service account that can read the whole corpus and
// narrows results by querying Snowflake as the caller, so an unverified
// identity would be an unfiltered search.
func TestDriveSearchOptions(t *testing.T) {
	tests := []struct {
		name                 string
		disableIntrospection bool
		wantEnabled          bool
		wantReasonContains   string
	}{
		{
			name:        "introspection on, so the caller is who they claim",
			wantEnabled: true,
		},
		{
			name:                 "introspection disabled makes the identity unverified",
			disableIntrospection: true,
			wantEnabled:          false,
			wantReasonContains:   "SSO_DISABLE_INTROSPECTION",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, reason := driveSearchOptions(&auth.OAuthConfig{
				DisableIntrospection: tt.disableIntrospection,
			}, stubIdentifier{})

			if !tt.wantEnabled {
				assert.Nil(t, opts, "tool must stay switched off")
				assert.Contains(t, reason, tt.wantReasonContains)
				return
			}

			require.NotNil(t, opts, "tool should be enabled, got reason: %s", reason)
			assert.Empty(t, reason)
			assert.NotNil(t, opts.Identifier, "the session check cannot be optional")
		})
	}
}

func TestResolveRerankerPoolDepth(t *testing.T) {
	tests := []struct {
		name            string
		raw             string
		want            int
		wantErrContains string
	}{
		{name: "unset uses the default", raw: "", want: mcptools.DefaultRerankerPool},
		{name: "whitespace only uses the default", raw: "   ", want: mcptools.DefaultRerankerPool},
		{name: "a valid value is honoured", raw: "35", want: 35},
		{name: "surrounding whitespace is tolerated", raw: " 35 ", want: 35},
		{
			name: "the maximum is honoured",
			raw:  strconv.Itoa(mcptools.MaxRerankerPool),
			want: mcptools.MaxRerankerPool,
		},
		{name: "above the maximum is rejected", raw: "5000", wantErrContains: "exceeds the maximum"},
		{name: "zero is rejected", raw: "0", wantErrContains: "must be positive"},
		{name: "negative is rejected", raw: "-10", wantErrContains: "must be positive"},
		{name: "non-numeric is rejected", raw: "fifty", wantErrContains: "must be a number"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := resolveRerankerPoolDepth(tt.raw)

			if tt.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

// A misconfigured pool must fail startup rather than silently running at the
// default, which would be invisible once the server is up.
func TestResolveRerankerPoolDepthRejectsRatherThanFallingBack(t *testing.T) {
	for _, raw := range []string{"0", "-1", "abc", "99999"} {
		_, err := resolveRerankerPoolDepth(raw)
		assert.Error(t, err, "input %q should be rejected, not silently defaulted", raw)
	}
}

func TestRerankerEnabled(t *testing.T) {
	tests := []struct {
		raw  string
		want bool
	}{
		{"true", true},
		{"TRUE", true},
		{"True", true},
		{" true ", true},
		{"", false},
		{"false", false},
		{"1", false},
		{"yes", false},
	}

	for _, tt := range tests {
		t.Run("ENABLE_RERANKER="+tt.raw, func(t *testing.T) {
			t.Setenv("ENABLE_RERANKER", tt.raw)
			assert.Equal(t, tt.want, rerankerEnabled())
		})
	}
}

func TestBuildRerankerClient(t *testing.T) {
	tests := []struct {
		name            string
		env             map[string]string
		wantClient      bool
		wantPool        int
		wantErrContains string
	}{
		{
			name:       "disabled returns no client and no error",
			env:        map[string]string{"ENABLE_RERANKER": ""},
			wantClient: false,
			wantPool:   mcptools.DefaultRerankerPool,
		},
		{
			// Enablement must come from the flag alone, so a fully configured
			// reranker stays off until it is switched on explicitly.
			name: "config present but flag off stays disabled",
			env: map[string]string{
				"ENABLE_RERANKER":     "",
				"RERANKER_ENDPOINT":   "https://example.invalid/v1/rerank",
				"RERANKER_MODEL_NAME": "a-model",
			},
			wantClient: false,
			wantPool:   mcptools.DefaultRerankerPool,
		},
		{
			name: "enabled and fully configured",
			env: map[string]string{
				"ENABLE_RERANKER":     "true",
				"RERANKER_ENDPOINT":   "https://example.invalid/v1/rerank",
				"RERANKER_MODEL_NAME": "a-model",
			},
			wantClient: true,
			wantPool:   mcptools.DefaultRerankerPool,
		},
		{
			name: "enabled with an explicit pool depth",
			env: map[string]string{
				"ENABLE_RERANKER":         "true",
				"RERANKER_ENDPOINT":       "https://example.invalid/v1/rerank",
				"RERANKER_MODEL_NAME":     "a-model",
				"RERANKER_CANDIDATE_POOL": "80",
			},
			wantClient: true,
			wantPool:   80,
		},
		{
			name:            "enabled without an endpoint fails startup",
			env:             map[string]string{"ENABLE_RERANKER": "true", "RERANKER_MODEL_NAME": "a-model"},
			wantErrContains: "RERANKER_ENDPOINT is empty",
		},
		{
			name: "enabled without a model fails startup",
			env: map[string]string{
				"ENABLE_RERANKER":   "true",
				"RERANKER_ENDPOINT": "https://example.invalid/v1/rerank",
			},
			wantErrContains: "RERANKER_MODEL_NAME is empty",
		},
		{
			name: "enabled with a bad pool depth fails startup",
			env: map[string]string{
				"ENABLE_RERANKER":         "true",
				"RERANKER_ENDPOINT":       "https://example.invalid/v1/rerank",
				"RERANKER_MODEL_NAME":     "a-model",
				"RERANKER_CANDIDATE_POOL": "not-a-number",
			},
			wantErrContains: "must be a number",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, key := range []string{
				"ENABLE_RERANKER", "RERANKER_ENDPOINT", "RERANKER_MODEL_NAME",
				"RERANKER_API_KEY", "RERANKER_CANDIDATE_POOL",
			} {
				t.Setenv(key, tt.env[key])
			}

			client, poolDepth, err := buildRerankerClient()

			if tt.wantErrContains != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErrContains)
				assert.Nil(t, client)
				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantPool, poolDepth)
			if tt.wantClient {
				assert.NotNil(t, client)
			} else {
				assert.Nil(t, client)
			}
		})
	}
}
