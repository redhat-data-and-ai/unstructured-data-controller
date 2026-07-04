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

const (
	DefaultDocumentProcessorType = "docling"
	DefaultOCRPreset             = "auto"
	DefaultPDFBackend            = "docling_parse"
	DefaultPipeline              = "standard"
	DefaultTableMode             = "accurate"
	DefaultImageExportMode       = "embedded"
	DefaultImagesScale           = "2.0"

	DefaultChunkingStrategy   = ChunkingStrategyRecursiveCharacter
	DefaultChunkSize          = 1000
	DefaultChunkOverlap       = 200
	DefaultEmbeddingModelName = "nomic-ai/nomic-embed-text-v1.5"
)

var defaultFromFormats = []string{"docx", "pptx", "html", "image", "pdf", "asciidoc", "md", "csv", "xlsx"}

func boolPtr(b bool) *bool { return &b }
