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

// optionsKey is the request key the convert endpoints read. The chunk
// endpoints read "convert_options" instead, and docling answers 200 while
// silently discarding every option sent under the wrong key, so the two are
// worth naming rather than inlining.
const optionsKey = "options"

// TestDoclingRequestPayloadSendsToFormats checks that to_formats actually
// reaches the wire, and reaches it under "options".
//
// Docling drops unknown keys without complaining. A payload sent under
// "convert_options" returns 200 with a normal-looking document in which every
// option was ignored, which means json_content comes back null and nothing
// anywhere reports a problem. A unit test is the only cheap place to catch it.
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
// just the struct tags: a real HTTP body, through getTaskStatus and the result
// endpoint, out to the caller. That is the plumbing a caller depends on, and
// it is what would break if the field were hung off the wrong struct.
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
