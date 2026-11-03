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
	"github.com/google/go-cmp/cmp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

type DocumentConverter string

const (
	DocumentConverterDocling  DocumentConverter = "docling"
	DocumentConverterNemotron DocumentConverter = "nemotron"
)

type Content struct {
	// Markdown is the whole document. For nemotron this is the per-page
	// markdown concatenated, so consumers that only want a flat rendering
	// (the converted MV, the langchain chunker) keep working unchanged.
	Markdown string `json:"markdown"`
	// Pages is populated by block-aware parsers only. Absent for docling.
	Pages []Page `json:"pages,omitempty"`
}

// Page is one parsed page of a document.
type Page struct {
	PageNumber int     `json:"pageNumber"`
	Markdown   string  `json:"markdown"`
	Blocks     []Block `json:"blocks,omitempty"`
	// PageImageURI is set only when page images were requested.
	PageImageURI string `json:"pageImageURI,omitempty"`
	// Warnings are parser notes for this page, e.g. a table whose markup could
	// not be fully converted.
	Warnings []map[string]any `json:"warnings,omitempty"`
}

// Block is one semantic region on a page: a table, a paragraph, a heading.
type Block struct {
	Index int    `json:"index"`
	Class string `json:"class"`
	// Text is the parser's raw output. For tables this is LaTeX.
	Text string `json:"text"`
	// Content is postprocessed. For tables this is HTML. The chunker prefers
	// this over Text.
	Content    string    `json:"content"`
	BBox       []float64 `json:"bbox,omitempty"`
	BBoxPixels []float64 `json:"bboxPixels,omitempty"`
	// CropURI is the filestore path of this block's cropped image, empty when
	// the class is skipped or the bounding box was degenerate.
	CropURI string `json:"cropURI,omitempty"`
}

type ConvertedFileMetadata struct {
	RawFilePath       string                   `json:"rawFilePath"`
	FileIdentifier    string                   `json:"fileIdentifier"`
	FilePath          string                   `json:"filePath,omitempty"`
	FileName          string                   `json:"fileName,omitempty"`
	FileURL           string                   `json:"fileURL,omitempty"`
	SourceType        string                   `json:"sourceType,omitempty"`
	DocumentConverter DocumentConverter        `json:"documentConverter"`
	DoclingConfig     v1alpha1.DoclingConfig   `json:"doclingConfig"`
	NemotronConfig    *v1alpha1.NemotronConfig `json:"nemotronConfig,omitempty"`
	Error             string                   `json:"error,omitempty"`
}

type ConvertedDocument struct {
	Metadata *ConvertedFileMetadata `json:"metadata"`
	Content  *Content               `json:"content"`
}

type ConvertedFile struct {
	ConvertedDocument *ConvertedDocument `json:"convertedDocument"`
}

// Equal reports whether two documents were produced by the same input and the
// same parser configuration. It drives needsConversion, so any field that
// should trigger reprocessing when it changes must be compared here.
func (c *ConvertedFileMetadata) Equal(other *ConvertedFileMetadata) bool {
	if c.RawFilePath != other.RawFilePath {
		return false
	}
	if c.FileIdentifier != other.FileIdentifier {
		return false
	}
	if c.DocumentConverter != other.DocumentConverter {
		return false
	}
	if !cmp.Equal(c.DoclingConfig, other.DoclingConfig) {
		return false
	}
	if !cmp.Equal(c.NemotronConfig, other.NemotronConfig) {
		return false
	}

	return true
}
