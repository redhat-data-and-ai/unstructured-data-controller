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

package unstructured

import (
	"encoding/json"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConvertedFileValidate(t *testing.T) {
	tests := []struct {
		name    string
		file    *ConvertedFile
		wantErr bool
	}{
		{
			name:    "nil receiver",
			file:    nil,
			wantErr: true,
		},
		{
			name:    "zero value, every pointer nil",
			file:    &ConvertedFile{},
			wantErr: true,
		},
		{
			name:    "document present but metadata missing",
			file:    &ConvertedFile{ConvertedDocument: &ConvertedDocument{Content: &Content{}}},
			wantErr: true,
		},
		{
			name:    "document present but content missing",
			file:    &ConvertedFile{ConvertedDocument: &ConvertedDocument{Metadata: &ConvertedFileMetadata{}}},
			wantErr: true,
		},
		{
			name: "fully populated",
			file: &ConvertedFile{ConvertedDocument: &ConvertedDocument{
				Metadata: &ConvertedFileMetadata{RawFilePath: "a.pdf"},
				Content:  &Content{Markdown: "# a"},
			}},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.file.Validate()
			if !tt.wantErr {
				require.NoError(t, err)
				return
			}
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrNotAConvertedFile),
				"callers match on ErrNotAConvertedFile to skip the file, got %v", err)
		})
	}
}

// TestConvertedFileValidateRejectsForeignJSON covers the reason this exists.
// Every field of ConvertedFile is a pointer, so unrelated JSON unmarshals
// without error and leaves them nil. Before Validate, the next dereference
// panicked and wedged the reconciler.
func TestConvertedFileValidateRejectsForeignJSON(t *testing.T) {
	foreign := [][]byte{
		[]byte(`{}`),
		[]byte(`{"fileName":"a.pdf","sourceType":"google_drive"}`),
		[]byte(`{"convertedDocument":null}`),
		[]byte(`{"convertedDocument":{}}`),
		[]byte(`{"convertedDocument":{"metadata":{"rawFilePath":"a.pdf"}}}`),
	}

	for _, raw := range foreign {
		t.Run(string(raw), func(t *testing.T) {
			var cf ConvertedFile
			require.NoError(t, json.Unmarshal(raw, &cf),
				"foreign JSON must unmarshal cleanly, that is the trap")

			err := cf.Validate()
			require.Error(t, err)
			assert.True(t, errors.Is(err, ErrNotAConvertedFile))

			assert.NotPanics(t, func() {
				if err := cf.Validate(); err == nil {
					_ = cf.ConvertedDocument.Metadata
					_ = cf.ConvertedDocument.Content.Markdown
				}
			}, "guarding on Validate must make the dereference safe")
		})
	}
}

func TestConvertedFileValidateAcceptsRealFile(t *testing.T) {
	raw := []byte(`{"convertedDocument":{
		"metadata":{"rawFilePath":"a.pdf","fileIdentifier":"abc","documentConverter":"docling"},
		"content":{"markdown":"# heading"}}}`)

	var cf ConvertedFile
	require.NoError(t, json.Unmarshal(raw, &cf))
	require.NoError(t, cf.Validate())

	assert.Equal(t, "a.pdf", cf.ConvertedDocument.Metadata.RawFilePath)
	assert.Equal(t, "# heading", cf.ConvertedDocument.Content.Markdown)
}
