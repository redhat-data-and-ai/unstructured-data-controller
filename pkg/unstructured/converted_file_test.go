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

	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
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

// Every file already in storage predates this field, so its JSON has no key
// for it and it decodes as nil. A pipeline not using the vlm pipeline also
// leaves it nil. These must compare equal, or shipping the field reconverts
// everything ever converted.
func TestConvertedFileMetadataEqualIgnoresAbsentVLMConfig(t *testing.T) {
	// No vlm_pipeline_custom_config key, as a pre-existing file would have.
	stored := []byte(`{"rawFilePath":"a.pdf","fileIdentifier":"abc",
		"documentConverter":"docling","doclingConfig":{"to_formats":["md"]}}`)

	var old ConvertedFileMetadata
	require.NoError(t, json.Unmarshal(stored, &old))
	require.Nil(t, old.DoclingConfig.VLMPipelineCustomConfig)

	current := ConvertedFileMetadata{
		RawFilePath:       "a.pdf",
		FileIdentifier:    "abc",
		DocumentConverter: DocumentConverterDocling,
		DoclingConfig:     v1alpha1.DoclingConfig{ToFormats: []string{"md"}},
	}

	assert.True(t, old.Equal(&current),
		"a file stored before the field existed must not be reconverted by its addition")
}

// The other half. The field lives in this struct so that changing what the
// model is asked to do reconverts. Held anywhere else, switching model or
// prompt would leave every stored file looking current while new files came
// from a different parse.
func TestConvertedFileMetadataEqualDetectsVLMConfigChange(t *testing.T) {
	base := func(mutate func(*v1alpha1.VLMPipelineCustomConfig)) ConvertedFileMetadata {
		vlm := &v1alpha1.VLMPipelineCustomConfig{
			Model:          "example/model",
			Prompt:         "<prompt>",
			ResponseFormat: "example_format",
			Scale:          "2.78",
			MaxSize:        4096,
		}
		if mutate != nil {
			mutate(vlm)
		}
		return ConvertedFileMetadata{
			RawFilePath:       "a.pdf",
			FileIdentifier:    "abc",
			DocumentConverter: DocumentConverterDocling,
			DoclingConfig: v1alpha1.DoclingConfig{
				Pipeline:                "vlm",
				VLMPipelineCustomConfig: vlm,
			},
		}
	}

	tests := []struct {
		name  string
		right ConvertedFileMetadata
		equal bool
	}{
		{
			name:  "identical config",
			right: base(nil),
			equal: true,
		},
		{
			name:  "different model",
			right: base(func(v *v1alpha1.VLMPipelineCustomConfig) { v.Model = "example/other" }),
			equal: false,
		},
		{
			name:  "different prompt",
			right: base(func(v *v1alpha1.VLMPipelineCustomConfig) { v.Prompt = "<other>" }),
			equal: false,
		},
		{
			name:  "different response format",
			right: base(func(v *v1alpha1.VLMPipelineCustomConfig) { v.ResponseFormat = "other_format" }),
			equal: false,
		},
		{
			name:  "different scale",
			right: base(func(v *v1alpha1.VLMPipelineCustomConfig) { v.Scale = "2.0" }),
			equal: false,
		},
		{
			name:  "different max size",
			right: base(func(v *v1alpha1.VLMPipelineCustomConfig) { v.MaxSize = 2048 }),
			equal: false,
		},
		{
			name: "configured against not configured",
			right: ConvertedFileMetadata{
				RawFilePath:       "a.pdf",
				FileIdentifier:    "abc",
				DocumentConverter: DocumentConverterDocling,
			},
			equal: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			left := base(nil)
			assert.Equal(t, tt.equal, left.Equal(&tt.right))
		})
	}
}
