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

package docling

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// optionsKey is the request key the convert endpoints read; the chunk
// endpoints read "convert_options". Docling answers 200 while silently
// discarding options sent under the wrong key.
const optionsKey = "options"

// TestDoclingRequestPayloadSendsToFormats checks that to_formats reaches the
// wire under "options". Docling drops unknown keys without complaining, so a
// payload under the wrong key returns 200 with json_content null and nothing
// reports a problem.
func TestDoclingRequestPayloadSendsToFormats(t *testing.T) {
	payload, err := json.Marshal(DoclingRequestPayload{
		Options: &DoclingConfig{ToFormats: []string{"md", "json"}},
		Sources: []DoclingSource{{URL: "https://example.invalid/a.pdf", Kind: "http"}},
	})
	require.NoError(t, err)

	var wire map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(payload, &wire))

	require.Contains(t, wire, optionsKey, "the convert endpoints only read options under %q", optionsKey)
	require.NotContains(t, wire, "convert_options")

	var opts struct {
		ToFormats []string `json:"to_formats"`
	}
	require.NoError(t, json.Unmarshal(wire[optionsKey], &opts))
	assert.Equal(t, []string{"md", "json"}, opts.ToFormats)
}

// TestDoclingResponseDocumentJSONDocument covers the shapes docling actually
// sends back for json_content. It is an object, not a string like its
// siblings, and it is nullable, so the obvious len() test on the raw field is
// wrong: the JSON literal null is four bytes and would read as present.
func TestDoclingResponseDocumentJSONDocument(t *testing.T) {
	const doc = `{"schema_name":"DoclingDocument","texts":[{"self_ref":"#/texts/0","text":"hello"}]}`

	tests := []struct {
		name string
		body string
		want string // "" means JSONDocument() should return nil
	}{
		{
			name: "a document is returned verbatim",
			body: `{"document":{"filename":"a.pdf","md_content":"# a","json_content":` + doc + `},"status":"success"}`,
			want: doc,
		},
		{
			name: "an explicit null reads as no document",
			body: `{"document":{"filename":"a.pdf","md_content":"# a","json_content":null},"status":"success"}`,
			want: "",
		},
		{
			name: "an absent field reads as no document",
			body: `{"document":{"filename":"a.pdf","md_content":"# a"},"status":"success"}`,
			want: "",
		},
		{
			name: "an empty object is a document, not an absence",
			body: `{"document":{"filename":"a.pdf","md_content":"# a","json_content":{}},"status":"success"}`,
			want: "{}",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var resp DoclingResponse
			require.NoError(t, json.Unmarshal([]byte(tt.body), &resp))

			// Adding the field must not disturb the markdown path that every
			// current consumer reads.
			assert.Equal(t, "# a", resp.Document.MDContent)

			got := resp.Document.JSONDocument()
			if tt.want == "" {
				assert.Nil(t, got)
				return
			}
			assert.JSONEq(t, tt.want, string(got))
			assert.Equal(t, tt.want, string(got),
				"the bytes are kept verbatim because block addresses only resolve against them")
		})
	}
}

// TestGetConvertedFileJSONContent walks the whole response path rather than
// just the struct tags: a real HTTP body, through getTaskStatus and the
// result endpoint, out to the caller.
func TestGetConvertedFileJSONContent(t *testing.T) {
	const doc = `{"schema_name":"DoclingDocument","pages":{"1":{"size":{"width":612,"height":792}}}}`

	tests := []struct {
		name   string
		taskID string
		result string
		want   string // "" means JSONDocument() should return nil
	}{
		{
			name:   "docling asked for json returns the document",
			taskID: "task-123",
			result: `{"document":{"filename":"a.pdf","md_content":"# a","json_content":` + doc +
				`},"status":"success","processing_time":1.5,"errors":[]}`,
			want: doc,
		},
		{
			name:   "docling not asked for json still converts",
			taskID: "task-456",
			result: `{"document":{"filename":"a.pdf","md_content":"# a","json_content":null},"status":"success"}`,
			want:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Header().Set("Content-Type", "application/json")
				var body string
				switch {
				case strings.HasPrefix(r.URL.Path, "/v1/status/poll/"):
					body = `{"task_id":"` + tt.taskID + `","task_status":"success","task_position":0}`
				case strings.HasPrefix(r.URL.Path, "/v1/result/"):
					body = tt.result
				default:
					w.WriteHeader(http.StatusNotFound)
					return
				}
				_, _ = w.Write([]byte(body))
			}))
			defer srv.Close()

			client := NewClientFromURL(&ClientConfig{
				URL:                   srv.URL,
				MaxConcurrentRequests: 1,
				HTTPTimeout:           5 * time.Second,
			})

			status, resp, err := client.GetConvertedFile(context.Background(), tt.taskID)
			require.NoError(t, err)
			require.Equal(t, TaskStatusSuccess, status)
			require.NotNil(t, resp)

			assert.Equal(t, "# a", resp.Document.MDContent)
			if tt.want == "" {
				assert.Nil(t, resp.Document.JSONDocument())
				return
			}
			assert.JSONEq(t, tt.want, string(resp.Document.JSONDocument()))
		})
	}
}

// The server discards options it does not recognise and returns 200, so a
// wrong key here quietly runs the standard pipeline. These assertions pin the
// wire names, because nothing downstream will.
func TestDoclingConfigVLMWireNames(t *testing.T) {
	scale := 2.78
	doclingConfig := DoclingConfig{
		Pipeline: "vlm",
		VLMPipelineCustomConfig: &VLMPipelineCustomConfig{
			ModelSpec: VLMModelSpec{
				Name:           "nvidia/Nemotron-3.5-Parse",
				DefaultRepoID:  "nvidia/Nemotron-3.5-Parse",
				Prompt:         "prompt",
				ResponseFormat: "nemotron_parse_v3_5",
			},
			EngineOptions: VLMEngineOptions{
				EngineType: VLMEngineTypeAPI,
				URL:        "https://example.invalid/v1/chat/completions",
				Headers:    map[string]string{"Authorization": "Bearer token"},
			},
			Scale:   &scale,
			MaxSize: 4096,
		},
	}

	payload, err := json.Marshal(doclingConfig)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(payload, &decoded))

	customConfig, ok := decoded["vlm_pipeline_custom_config"].(map[string]any)
	require.True(t, ok, "the server knows this option as vlm_pipeline_custom_config; "+
		"the older vlm_pipeline_model_api is deprecated and takes a flat shape")

	modelSpec, ok := customConfig["model_spec"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "nvidia/Nemotron-3.5-Parse", modelSpec["default_repo_id"])
	assert.Equal(t, "nvidia/Nemotron-3.5-Parse", modelSpec["name"])
	assert.Equal(t, "prompt", modelSpec["prompt"])
	assert.Equal(t, "nemotron_parse_v3_5", modelSpec["response_format"])

	engineOptions, ok := customConfig["engine_options"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "api", engineOptions["engine_type"],
		"docling resolves engine_options by this discriminator; without it no engine matches")
	assert.Equal(t, "https://example.invalid/v1/chat/completions", engineOptions["url"])

	headers, ok := engineOptions["headers"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "Bearer token", headers["Authorization"])

	// Rasterisation sits beside model_spec, not inside it or inside the engine
	// options, which is where the deprecated option used to carry it.
	assert.Equal(t, 2.78, customConfig["scale"])
	assert.Equal(t, float64(4096), customConfig["max_size"])

	// Docling only derives the request's model parameter from default_repo_id
	// while engine_options carries no params of its own, so sending an empty
	// params object would be enough to drop the model name.
	assert.NotContains(t, engineOptions, "params")
}

// An unset scale must be absent rather than zero, or pages are rasterised at
// nothing instead of at the server's default.
func TestVLMPipelineCustomConfigOmitsUnsetScale(t *testing.T) {
	payload, err := json.Marshal(&VLMPipelineCustomConfig{})
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(payload, &decoded))

	assert.NotContains(t, decoded, "scale")
	assert.NotContains(t, decoded, "max_size")
}

// Every field docling requires must be emitted even when empty rather than
// silently omitted into a validation error that names no field.
func TestVLMPipelineCustomConfigAlwaysEmitsRequiredFields(t *testing.T) {
	payload, err := json.Marshal(&VLMPipelineCustomConfig{})
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(payload, &decoded))

	require.Contains(t, decoded, "model_spec")
	modelSpec, ok := decoded["model_spec"].(map[string]any)
	require.True(t, ok)
	for _, field := range []string{"name", "default_repo_id", "prompt", "response_format"} {
		assert.Contains(t, modelSpec, field)
	}

	require.Contains(t, decoded, "engine_options")
	engineOptions, ok := decoded["engine_options"].(map[string]any)
	require.True(t, ok)
	for _, field := range []string{"engine_type", "url"} {
		assert.Contains(t, engineOptions, field)
	}
}

// The option must disappear entirely when unset, so ordinary conversions are
// byte-identical to what the server received before this field existed.
func TestDoclingConfigOmitsVLMEngineWhenUnset(t *testing.T) {
	payload, err := json.Marshal(DoclingConfig{})
	require.NoError(t, err)

	assert.NotContains(t, string(payload), "vlm_pipeline_custom_config")
	// The deprecated name must not reappear either: docling drops unknown
	// options silently, so sending it would run the standard pipeline and
	// still report success.
	assert.NotContains(t, string(payload), "vlm_pipeline_model_api")
}
