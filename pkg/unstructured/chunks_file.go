package unstructured

import (
	"github.com/google/go-cmp/cmp"
)

type ChunkingTool string

type DocumentConverter string
type ChunkingStrategy string

const (
	LangchainChunkingTool              ChunkingTool      = "langchain"
	DoclingDocumentConverter           DocumentConverter = "docling"
	ChunkingStrategyRecursiveCharacter ChunkingStrategy  = "recursiveCharacterTextSplitter"
	ChunkingStrategyMarkdown           ChunkingStrategy  = "markdownTextSplitter"
	ChunkingStrategyToken              ChunkingStrategy  = "tokenTextSplitter"
)

type Chunks struct {
	Text []string `json:"text"`
}

type ConvertedFileMetadata struct {
	RawFilePath       string            `json:"rawFilePath"`
	DocumentConverter DocumentConverter `json:"documentConverter"`
	DoclingConfig     DoclingConfig     `json:"doclingConfig"`
}

type Content struct {
	Markdown string `json:"markdown"`
}

type ConvertedDocument struct {
	Metadata ConvertedFileMetadata `json:"metadata"`
	Content  Content               `json:"content"`
}

type ChunksGeneratorConfig struct {
	Strategy                         ChunkingStrategy                  `json:"strategy"`
	RecursiveCharacterSplitterConfig *RecursiveCharacterSplitterConfig `json:"recursiveCharacterSplitterConfig,omitempty"`
	MarkdownSplitterConfig           *MarkdownSplitterConfig           `json:"markdownSplitterConfig,omitempty"`
	TokenSplitterConfig              *TokenSplitterConfig              `json:"tokenSplitterConfig,omitempty"`
}

type RecursiveCharacterSplitterConfig struct {
	Separators    []string `json:"separators,omitempty"`
	ChunkSize     int      `json:"chunkSize,omitempty"`
	ChunkOverlap  int      `json:"chunkOverlap,omitempty"`
	KeepSeparator bool     `json:"keepSeparator,omitempty"`
}

type MarkdownSplitterConfig struct {
	ChunkSize        int  `json:"chunkSize,omitempty"`
	ChunkOverlap     int  `json:"chunkOverlap,omitempty"`
	CodeBlocks       bool `json:"codeBlocks,omitempty"`
	ReferenceLinks   bool `json:"referenceLinks,omitempty"`
	HeadingHierarchy bool `json:"headingHierarchy,omitempty"`
	JoinTableRows    bool `json:"joinTableRows,omitempty"`
}

type TokenSplitterConfig struct {
	ChunkSize         int      `json:"chunkSize,omitempty"`
	ChunkOverlap      int      `json:"chunkOverlap,omitempty"`
	ModelName         string   `json:"modelName,omitempty"`
	EncodingName      string   `json:"encodingName,omitempty"`
	AllowedSpecial    []string `json:"allowedSpecial,omitempty"`
	DisallowedSpecial []string `json:"disallowedSpecial,omitempty"`
}

type DoclingConfig struct {
	FromFormats     []string `json:"from_formats,omitempty"`
	ImageExportMode string   `json:"image_export_mode,omitempty"`
	DoOCR           bool     `json:"do_ocr,omitempty"`
	ForceOCR        bool     `json:"force_ocr,omitempty"`
	OCREngine       string   `json:"ocr_engine,omitempty"`
	OCRLang         []string `json:"ocr_lang,omitempty"`
	PDFBackend      string   `json:"pdf_backend,omitempty"`
	TableMode       string   `json:"table_mode,omitempty"`
}

type ChunksFileMetadata struct {
	ConvertedFileMetadata *ConvertedFileMetadata `json:"convertedFileMetadata"`
	ChunkingTool          *ChunkingTool          `json:"chunkingTool"`
	ChunksGeneratorConfig *ChunksGeneratorConfig `json:"chunksGeneratorConfig"`
}

type ChunksDocument struct {
	Metadata *ChunksFileMetadata `json:"metadata"`
	Chunks   *Chunks             `json:"chunks"`
}

type ChunksFile struct {
	ConvertedDocument *ConvertedDocument `json:"convertedDocument"`
	ChunksDocument    *ChunksDocument    `json:"chunksDocument"`
}

func (c *ChunksFileMetadata) Equal(other *ChunksFileMetadata) bool {
	if !cmp.Equal(c.ConvertedFileMetadata, other.ConvertedFileMetadata) {
		return false
	}
	if !cmp.Equal(c.ChunkingTool, other.ChunkingTool) {
		return false
	}
	if !cmp.Equal(c.ChunksGeneratorConfig, other.ChunksGeneratorConfig) {
		return false
	}
	return true
}
