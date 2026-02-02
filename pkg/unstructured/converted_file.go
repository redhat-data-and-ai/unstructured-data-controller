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
	DocumentConverterDocling DocumentConverter = "docling"
)

type Content struct {
	Markdown string `json:"markdown"`
}

type ConvertedFileMetadata struct {
	RawFilePath       string                 `json:"rawFilePath"`
	DocumentConverter DocumentConverter      `json:"documentConverter"`
	DoclingConfig     v1alpha1.DoclingConfig `json:"doclingConfig"`
}

type ConvertedDocument struct {
	Metadata *ConvertedFileMetadata `json:"metadata"`
	Content  *Content               `json:"content"`
}

type ConvertedFile struct {
	ConvertedDocument *ConvertedDocument `json:"convertedDocument"`
}

func (c *ConvertedFileMetadata) Equal(other *ConvertedFileMetadata) bool {
	if c.RawFilePath != other.RawFilePath {
		return false
	}
	if c.DocumentConverter != other.DocumentConverter {
		return false
	}
	if !cmp.Equal(c.DoclingConfig, other.DoclingConfig) {
		return false
	}

	return true
}
