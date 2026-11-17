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
	"slices"
	"testing"
)

// mutated is the sentinel the aliasing test writes into a value it got
// back from SetDefaults. Finding it in a package-level default means the
// default was handed out by reference.
const mutated = "mutated"

func TestDocumentProcessorConfig_SetDefaults_EmptyConfig(t *testing.T) {
	c := DocumentProcessorConfig{}
	c.SetDefaults()

	if c.Type != DefaultDocumentProcessorType {
		t.Errorf("expected type %q, got %q", DefaultDocumentProcessorType, c.Type)
	}
}

func TestDocumentProcessorConfig_SetDefaults_PreservesExplicitType(t *testing.T) {
	c := DocumentProcessorConfig{Type: "custom"}
	c.SetDefaults()

	if c.Type != "custom" {
		t.Errorf("expected type %q, got %q", "custom", c.Type)
	}
}

func TestDoclingConfig_SetDefaults_EmptyConfig(t *testing.T) {
	c := DoclingConfig{}
	c.SetDefaults()

	// Written out rather than compared against defaultFromFormats and
	// defaultToFormats, which SetDefaults copies from: comparing against
	// them would pass whatever they happen to hold.
	inputFormats := []string{"docx", "pptx", "html", "image", "pdf", "asciidoc", "md", "csv", "xlsx"}
	if !slices.Equal(c.FromFormats, inputFormats) {
		t.Errorf("expected from_formats %v, got %v", inputFormats, c.FromFormats)
	}
	outputFormats := []string{"md", "json"}
	if !slices.Equal(c.ToFormats, outputFormats) {
		t.Errorf("expected to_formats %v, got %v", outputFormats, c.ToFormats)
	}
	if c.ImageExportMode != DefaultImageExportMode {
		t.Errorf("expected image_export_mode %q, got %q", DefaultImageExportMode, c.ImageExportMode)
	}
	if c.OCRPreset != DefaultOCRPreset {
		t.Errorf("expected ocr_preset %q, got %q", DefaultOCRPreset, c.OCRPreset)
	}
	if c.PDFBackend != DefaultPDFBackend {
		t.Errorf("expected pdf_backend %q, got %q", DefaultPDFBackend, c.PDFBackend)
	}
	if c.Pipeline != DefaultPipeline {
		t.Errorf("expected pipeline %q, got %q", DefaultPipeline, c.Pipeline)
	}
	if c.TableMode != DefaultTableMode {
		t.Errorf("expected table_mode %q, got %q", DefaultTableMode, c.TableMode)
	}
	if c.TableCellMatching == nil || !*c.TableCellMatching {
		t.Error("expected table_cell_matching to be true")
	}
	if c.DoTableStructure == nil || !*c.DoTableStructure {
		t.Error("expected do_table_structure to be true")
	}
	if c.IncludeImages == nil || *c.IncludeImages {
		t.Error("expected include_images to be false")
	}
	if c.IncludePageImages == nil || !*c.IncludePageImages {
		t.Error("expected include_page_images to be true")
	}
	if c.ImagesScale != DefaultImagesScale {
		t.Errorf("expected images_scale %q, got %q", DefaultImagesScale, c.ImagesScale)
	}
}

func TestDoclingConfig_SetDefaults_ExportModeKeepsBase64OutOfMarkdown(t *testing.T) {
	c := DoclingConfig{}
	c.SetDefaults()

	if c.ImageExportMode != "referenced" {
		t.Errorf("expected image_export_mode %q, got %q", "referenced", c.ImageExportMode)
	}
}

// Defaulting page images on under "placeholder" would reconvert the corpus
// and return nothing.
func TestDoclingConfig_SetDefaults_PlaceholderDoesNotAskForPageImages(t *testing.T) {
	c := DoclingConfig{ImageExportMode: "placeholder"}
	c.SetDefaults()

	if c.IncludePageImages == nil || *c.IncludePageImages {
		t.Error("expected include_page_images to stay off under placeholder")
	}
}

func TestDoclingConfig_SetDefaults_PreservesExplicitValues(t *testing.T) {
	f := false
	c := DoclingConfig{
		FromFormats:       []string{"pdf"},
		ToFormats:         []string{"html"},
		ImageExportMode:   "placeholder",
		OCRPreset:         "tesseract",
		PDFBackend:        "pypdfium2",
		Pipeline:          "vlm",
		TableMode:         "fast",
		TableCellMatching: &f,
		DoTableStructure:  &f,
		IncludeImages:     &f,
		ImagesScale:       "1.0",
	}
	c.SetDefaults()

	if len(c.FromFormats) != 1 || c.FromFormats[0] != "pdf" {
		t.Errorf("expected from_formats [pdf], got %v", c.FromFormats)
	}
	if c.ToFormats[0] != "html" {
		t.Errorf("expected to_formats [html], got %v", c.ToFormats)
	}
	if c.ImageExportMode != "placeholder" {
		t.Errorf("expected image_export_mode %q, got %q", "placeholder", c.ImageExportMode)
	}
	if c.OCRPreset != "tesseract" {
		t.Errorf("expected ocr_preset %q, got %q", "tesseract", c.OCRPreset)
	}
	if c.PDFBackend != "pypdfium2" {
		t.Errorf("expected pdf_backend %q, got %q", "pypdfium2", c.PDFBackend)
	}
	if c.Pipeline != "vlm" {
		t.Errorf("expected pipeline %q, got %q", "vlm", c.Pipeline)
	}
	if c.TableMode != "fast" {
		t.Errorf("expected table_mode %q, got %q", "fast", c.TableMode)
	}
	if *c.TableCellMatching {
		t.Error("expected table_cell_matching to be false")
	}
	if *c.DoTableStructure {
		t.Error("expected do_table_structure to be false")
	}
	if *c.IncludeImages {
		t.Error("expected include_images to be false")
	}
	if c.ImagesScale != "1.0" {
		t.Errorf("expected images_scale %q, got %q", "1.0", c.ImagesScale)
	}
}

func TestDoclingConfig_SetDefaults_DoesNotAliasGlobalSlice(t *testing.T) {
	c := DoclingConfig{}
	c.SetDefaults()
	c.FromFormats[0] = mutated

	if defaultFromFormats[0] == mutated {
		t.Error("SetDefaults aliased the global defaultFromFormats slice")
	}
}

// The standard pipeline must not grow this config. Every live processor is on
// the standard pipeline, and populating the field would change their stored
// metadata, reconverting every corpus for a feature none of them use.
func TestDoclingConfig_SetDefaults_StandardPipelineLeavesVLMConfigUnset(t *testing.T) {
	for _, pipeline := range []string{"", PipelineStandard} {
		t.Run("pipeline="+pipeline, func(t *testing.T) {
			c := DoclingConfig{Pipeline: pipeline}
			c.SetDefaults()

			if c.VLMPipelineCustomConfig != nil {
				t.Errorf("expected vlm_pipeline_custom_config to stay unset, got %+v", c.VLMPipelineCustomConfig)
			}
		})
	}
}

// Selecting the vlm pipeline and nothing else has to produce a request the
// model will answer. Prompt and response format in particular cannot be
// guessed by a user, and a wrong one fails silently rather than loudly.
func TestDoclingConfig_SetDefaults_VLMPipelineFillsTheModelsSettings(t *testing.T) {
	c := DoclingConfig{Pipeline: PipelineVLM}
	c.SetDefaults()

	got := c.VLMPipelineCustomConfig
	if got == nil {
		t.Fatal("expected vlm_pipeline_custom_config to be populated, got nil")
	}

	want := vlmModelDefaults[DefaultVLMPipelineModel]
	if got.Model != DefaultVLMPipelineModel {
		t.Errorf("expected model %q, got %q", DefaultVLMPipelineModel, got.Model)
	}
	if got.Prompt != want.Prompt {
		t.Errorf("expected prompt %q, got %q", want.Prompt, got.Prompt)
	}
	if got.ResponseFormat != want.ResponseFormat {
		t.Errorf("expected response_format %q, got %q", want.ResponseFormat, got.ResponseFormat)
	}
	if got.Scale != want.Scale {
		t.Errorf("expected scale %q, got %q", want.Scale, got.Scale)
	}
	if got.MaxSize != want.MaxSize {
		t.Errorf("expected max_size %d, got %d", want.MaxSize, got.MaxSize)
	}
}

func TestDoclingConfig_SetDefaults_VLMPipelinePreservesExplicitValues(t *testing.T) {
	c := DoclingConfig{
		Pipeline: PipelineVLM,
		VLMPipelineCustomConfig: &VLMPipelineCustomConfig{
			Model:          VLMModelNemotronParse,
			Prompt:         "<prompt>",
			ResponseFormat: "example_format",
			Scale:          "1.0",
			MaxSize:        2048,
		},
	}
	c.SetDefaults()

	got := c.VLMPipelineCustomConfig
	if got.Prompt != "<prompt>" {
		t.Errorf("expected prompt %q, got %q", "<prompt>", got.Prompt)
	}
	if got.ResponseFormat != "example_format" {
		t.Errorf("expected response_format %q, got %q", "example_format", got.ResponseFormat)
	}
	if got.Scale != "1.0" {
		t.Errorf("expected scale %q, got %q", "1.0", got.Scale)
	}
	if got.MaxSize != 2048 {
		t.Errorf("expected max_size %d, got %d", 2048, got.MaxSize)
	}
}

// A model we have no settings for is left alone rather than given another
// model's prompt, which would be answered and would return nonsense.
func TestDoclingConfig_SetDefaults_VLMPipelineUnknownModelFillsNothing(t *testing.T) {
	c := DoclingConfig{
		Pipeline:                PipelineVLM,
		VLMPipelineCustomConfig: &VLMPipelineCustomConfig{Model: "example/unknown"},
	}
	c.SetDefaults()

	got := c.VLMPipelineCustomConfig
	if got.Model != "example/unknown" {
		t.Errorf("expected model %q, got %q", "example/unknown", got.Model)
	}
	if got.Prompt != "" || got.ResponseFormat != "" || got.Scale != "" || got.MaxSize != 0 {
		t.Errorf("expected an unknown model to be left alone, got %+v", got)
	}
}

// The defaults are handed out by copy. A caller mutating what it got back must
// not reach the package-level map through it.
func TestVLMPipelineCustomConfig_SetDefaults_DoesNotAliasTheModelTable(t *testing.T) {
	c := VLMPipelineCustomConfig{}
	c.SetDefaults()
	c.Prompt = mutated

	if vlmModelDefaults[DefaultVLMPipelineModel].Prompt == mutated {
		t.Error("SetDefaults aliased the package-level vlmModelDefaults entry")
	}
}

// TestDoclingConfig_SetDefaults_ToFormats pins the exact list that reaches
// docling. Docling populates json_content only when "json" is in to_formats
// and reports no error when it is missing, so losing it fails silently.
func TestDoclingConfig_SetDefaults_ToFormats(t *testing.T) {
	tests := []struct {
		name  string
		given []string
		want  []string
	}{
		{
			name:  "unset asks for markdown and the docling document",
			given: nil,
			want:  []string{"md", "json"},
		},
		{
			name:  "an empty slice counts as unset",
			given: []string{},
			want:  []string{"md", "json"},
		},
		{
			name:  "an explicit list is kept even when it drops json",
			given: []string{"md"},
			want:  []string{"md"},
		},
		{
			name:  "an explicit list is kept even when it drops md",
			given: []string{"json"},
			want:  []string{"json"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := DoclingConfig{ToFormats: tt.given}
			c.SetDefaults()

			if !slices.Equal(c.ToFormats, tt.want) {
				t.Errorf("expected to_formats %v, got %v", tt.want, c.ToFormats)
			}
		})
	}
}

// TestDoclingConfig_SetDefaults_ToFormatsDoesNotAliasGlobalSlice guards the
// same thing the from_formats test does. A caller mutating what it got back
// must not reach the package-level default through it.
func TestDoclingConfig_SetDefaults_ToFormatsDoesNotAliasGlobalSlice(t *testing.T) {
	c := DoclingConfig{}
	c.SetDefaults()
	c.ToFormats[0] = mutated

	if slices.Contains(defaultToFormats, mutated) {
		t.Error("SetDefaults aliased the global defaultToFormats slice")
	}
}
