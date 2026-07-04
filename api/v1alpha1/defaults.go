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

var DefaultFromFormats = []string{"docx", "pptx", "html", "image", "pdf", "asciidoc", "md", "csv", "xlsx"}

func boolPtr(b bool) *bool { return &b }

// SetDefaults fills in sane defaults for any unset fields.
// If no type is specified, docling is used.
func (c *DocumentProcessorConfig) SetDefaults() {
	if c.Type == "" {
		c.Type = DefaultDocumentProcessorType
	}
	c.DoclingConfig.SetDefaults()
}

// SetDefaults fills in sane defaults for any unset DoclingConfig fields.
func (c *DoclingConfig) SetDefaults() {
	if len(c.FromFormats) == 0 {
		c.FromFormats = DefaultFromFormats
	}
	if len(c.ToFormats) == 0 {
		c.ToFormats = []string{"md"}
	}
	if c.ImageExportMode == "" {
		c.ImageExportMode = DefaultImageExportMode
	}
	if c.OCRPreset == "" {
		c.OCRPreset = DefaultOCRPreset
	}
	if c.PDFBackend == "" {
		c.PDFBackend = DefaultPDFBackend
	}
	if c.Pipeline == "" {
		c.Pipeline = DefaultPipeline
	}
	if c.TableMode == "" {
		c.TableMode = DefaultTableMode
	}
	if c.TableCellMatching == nil {
		c.TableCellMatching = boolPtr(true)
	}
	if c.DoTableStructure == nil {
		c.DoTableStructure = boolPtr(true)
	}
	if c.IncludeImages == nil {
		c.IncludeImages = boolPtr(true)
	}
	if c.ImagesScale == "" {
		c.ImagesScale = DefaultImagesScale
	}
}

// SetDefaults fills in sane defaults for any unset fields.
// If no strategy is specified, recursiveCharacterTextSplitter is used.
// If chunkSize or chunkOverlap are zero for the active strategy, they default
// to 1000 and 200 respectively.
func (c *ChunksGeneratorConfig) SetDefaults() {
	if c.Strategy == "" {
		c.Strategy = DefaultChunkingStrategy
	}

	switch c.Strategy {
	case ChunkingStrategyRecursiveCharacter:
		if c.RecursiveCharacterSplitterConfig.ChunkSize == 0 {
			c.RecursiveCharacterSplitterConfig.ChunkSize = DefaultChunkSize
		}
		if c.RecursiveCharacterSplitterConfig.ChunkOverlap == 0 {
			c.RecursiveCharacterSplitterConfig.ChunkOverlap = DefaultChunkOverlap
		}
	case ChunkingStrategyMarkdown:
		if c.MarkdownSplitterConfig.ChunkSize == 0 {
			c.MarkdownSplitterConfig.ChunkSize = DefaultChunkSize
		}
		if c.MarkdownSplitterConfig.ChunkOverlap == 0 {
			c.MarkdownSplitterConfig.ChunkOverlap = DefaultChunkOverlap
		}
	case ChunkingStrategyToken:
		if c.TokenSplitterConfig.ChunkSize == 0 {
			c.TokenSplitterConfig.ChunkSize = DefaultChunkSize
		}
		if c.TokenSplitterConfig.ChunkOverlap == 0 {
			c.TokenSplitterConfig.ChunkOverlap = DefaultChunkOverlap
		}
	default:
	}
}

// SetDefaults fills in sane defaults for any unset fields.
// If no model name is specified, nomic-ai/nomic-embed-text-v1.5 is used.
func (c *VectorEmbeddingsGeneratorConfig) SetDefaults() {
	if c.ModelName == "" {
		c.ModelName = DefaultEmbeddingModelName
	}
}

// SetDefaults infers the source type from the populated sub-config when Type is
// not explicitly set.
func (c *SourceCrawlerConfig) SetDefaults() {
	if c.Type != "" {
		return
	}
	if c.GoogleDriveConfig != nil {
		c.Type = TypeGoogleDrive
		return
	}
	if c.S3Config.Bucket != "" {
		c.Type = TypeS3
	}
}
