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

	if len(c.FromFormats) != len(defaultFromFormats) {
		t.Errorf("expected %d from_formats, got %d", len(defaultFromFormats), len(c.FromFormats))
	}
	if c.ToFormats[0] != "md" {
		t.Errorf("expected to_formats [md], got %v", c.ToFormats)
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
	if c.IncludeImages == nil || !*c.IncludeImages {
		t.Error("expected include_images to be true")
	}
	if c.ImagesScale != DefaultImagesScale {
		t.Errorf("expected images_scale %q, got %q", DefaultImagesScale, c.ImagesScale)
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
	c.FromFormats[0] = "mutated"

	if defaultFromFormats[0] == "mutated" {
		t.Error("SetDefaults aliased the global defaultFromFormats slice")
	}
}

func TestNemotronConfig_SetDefaults_EmptyConfig(t *testing.T) {
	c := NemotronConfig{}
	c.SetDefaults()

	if c.Mode != DefaultNemotronMode {
		t.Errorf("expected mode %q, got %q", DefaultNemotronMode, c.Mode)
	}
	if c.DPI != DefaultNemotronDPI {
		t.Errorf("expected dpi %d, got %d", DefaultNemotronDPI, c.DPI)
	}
	if c.IncludePageImages == nil || *c.IncludePageImages {
		t.Error("expected page images to default to off")
	}
	if len(c.CropSkipClasses) != 3 {
		t.Errorf("expected 3 default crop skip classes, got %v", c.CropSkipClasses)
	}
}

func TestNemotronConfig_SetDefaults_PreservesExplicitValues(t *testing.T) {
	includePageImages := true
	c := NemotronConfig{
		Mode:              "single",
		DPI:               300,
		IncludePageImages: &includePageImages,
		CropSkipClasses:   []string{"Picture"},
	}
	c.SetDefaults()

	if c.Mode != "single" {
		t.Errorf("expected mode to be preserved, got %q", c.Mode)
	}
	if c.DPI != 300 {
		t.Errorf("expected dpi to be preserved, got %d", c.DPI)
	}
	if c.IncludePageImages == nil || !*c.IncludePageImages {
		t.Error("expected explicit page images setting to be preserved")
	}
	if len(c.CropSkipClasses) != 1 || c.CropSkipClasses[0] != "Picture" {
		t.Errorf("expected explicit crop skip classes to be preserved, got %v", c.CropSkipClasses)
	}
}

func TestNemotronConfig_SetDefaults_DoesNotAliasGlobalSlice(t *testing.T) {
	first := NemotronConfig{}
	first.SetDefaults()
	first.CropSkipClasses[0] = "mutated"

	second := NemotronConfig{}
	second.SetDefaults()

	if second.CropSkipClasses[0] == "mutated" {
		t.Error("defaults alias the package-level slice; one config mutated another")
	}
}

func TestDocumentProcessorConfig_SetDefaults_NemotronDoesNotDefaultDocling(t *testing.T) {
	// A nemotron pipeline must not carry a fully populated docling config:
	// it would take part in the Equal() comparison that drives reprocessing.
	c := DocumentProcessorConfig{Type: DocumentProcessorTypeNemotron}
	c.SetDefaults()

	if c.NemotronConfig == nil {
		t.Fatal("expected a nemotron config to be created")
	}
	if c.NemotronConfig.Mode != DefaultNemotronMode {
		t.Errorf("expected nemotron defaults to be applied, got mode %q", c.NemotronConfig.Mode)
	}
	if len(c.DoclingConfig.FromFormats) != 0 {
		t.Errorf("expected docling config to be left alone, got %v", c.DoclingConfig.FromFormats)
	}
}

func TestDocumentProcessorConfig_SetDefaults_DoclingDoesNotCreateNemotronConfig(t *testing.T) {
	c := DocumentProcessorConfig{}
	c.SetDefaults()

	if c.NemotronConfig != nil {
		t.Error("expected no nemotron config on a docling pipeline")
	}
	if len(c.DoclingConfig.FromFormats) == 0 {
		t.Error("expected docling defaults to be applied")
	}
}
