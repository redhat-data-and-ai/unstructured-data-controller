package controller

import (
	"context"
	"testing"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/langchain"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

// recordingStructuredChunker stands in for a block-aware chunker so a test can
// check that the reconciler reaches for it when one is configured.
type recordingStructuredChunker struct {
	chunks    []unstructured.DocumentChunk
	callCount int
}

func (c *recordingStructuredChunker) ChunkDocument(
	_ context.Context, _ *unstructured.ConvertedDocument,
) ([]unstructured.DocumentChunk, error) {
	c.callCount++
	return c.chunks, nil
}

func TestTextChunkerFor(t *testing.T) {
	splitterClient := langchain.NewClient(langchain.ClientConfig{MaxConcurrentRequests: 1})

	tests := map[string]struct {
		strategy operatorv1alpha1.ChunkingStrategy
		want     unstructured.Chunker
		wantErr  bool
	}{
		"recursive character": {
			strategy: operatorv1alpha1.ChunkingStrategyRecursiveCharacter,
			want:     &unstructured.RecursiveCharacterSplitter{},
		},
		"markdown": {
			strategy: operatorv1alpha1.ChunkingStrategyMarkdown,
			want:     &unstructured.MarkdownSplitter{},
		},
		"token": {
			strategy: operatorv1alpha1.ChunkingStrategyToken,
			want:     &unstructured.TokenSplitter{},
		},
		"unknown strategy is an error": {
			strategy: operatorv1alpha1.ChunkingStrategy("no-such-strategy"),
			wantErr:  true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			config := &operatorv1alpha1.ChunksGeneratorConfig{Strategy: test.strategy}

			got, err := textChunkerFor(splitterClient, config)
			if test.wantErr {
				if err == nil {
					t.Fatalf("textChunkerFor(%q) returned no error, want one", test.strategy)
				}
				return
			}
			if err != nil {
				t.Fatalf("textChunkerFor(%q) returned %v, want no error", test.strategy, err)
			}
			if gotType, wantType := typeName(got), typeName(test.want); gotType != wantType {
				t.Errorf("textChunkerFor(%q) = %s, want %s", test.strategy, gotType, wantType)
			}
		})
	}
}

func typeName(chunker unstructured.Chunker) string {
	switch chunker.(type) {
	case *unstructured.RecursiveCharacterSplitter:
		return "RecursiveCharacterSplitter"
	case *unstructured.MarkdownSplitter:
		return "MarkdownSplitter"
	case *unstructured.TokenSplitter:
		return "TokenSplitter"
	default:
		return "unknown"
	}
}

func TestDocumentChunkerFallsBackToTheConfiguredTextSplitter(t *testing.T) {
	withLangchainClient(t)

	reconciler := &ChunksGeneratorReconciler{}
	config := &operatorv1alpha1.ChunksGeneratorConfig{
		Strategy: operatorv1alpha1.ChunkingStrategyRecursiveCharacter,
	}
	config.SetDefaults()

	chunker, err := reconciler.documentChunker(config)
	if err != nil {
		t.Fatalf("documentChunker() returned %v, want no error", err)
	}

	adapter, ok := chunker.(*unstructured.TextChunkerAdapter)
	if !ok {
		t.Fatalf("documentChunker() = %T, want *unstructured.TextChunkerAdapter", chunker)
	}
	if _, ok := adapter.Chunker.(*unstructured.RecursiveCharacterSplitter); !ok {
		t.Fatalf("adapter wraps %T, want *unstructured.RecursiveCharacterSplitter", adapter.Chunker)
	}

	// The markdown path still produces the same flat list of strings.
	doc := &unstructured.ConvertedDocument{
		Content: &unstructured.Content{Markdown: "some text to split"},
	}
	chunks, err := chunker.ChunkDocument(context.Background(), doc)
	if err != nil {
		t.Fatalf("ChunkDocument() returned %v, want no error", err)
	}
	texts := unstructured.ChunkTexts(chunks)
	if len(texts) != 1 || texts[0] != doc.Content.Markdown {
		t.Errorf("ChunkTexts() = %#v, want [%q]", texts, doc.Content.Markdown)
	}
}

func TestDocumentChunkerPrefersTheStructuredChunker(t *testing.T) {
	withLangchainClient(t)

	structured := &recordingStructuredChunker{
		chunks: []unstructured.DocumentChunk{
			{Text: "a chunk", BlockIDs: []string{"#/texts/7"}},
		},
	}
	reconciler := &ChunksGeneratorReconciler{structuredChunker: structured}

	// An unknown strategy would fail the text splitter path, which proves the
	// structured chunker is chosen before the strategy is even looked at.
	config := &operatorv1alpha1.ChunksGeneratorConfig{
		Strategy: operatorv1alpha1.ChunkingStrategy("no-such-strategy"),
	}

	chunker, err := reconciler.documentChunker(config)
	if err != nil {
		t.Fatalf("documentChunker() returned %v, want no error", err)
	}
	if chunker != unstructured.StructuredChunker(structured) {
		t.Fatalf("documentChunker() = %T, want the configured structured chunker", chunker)
	}

	chunks, err := chunker.ChunkDocument(context.Background(), &unstructured.ConvertedDocument{})
	if err != nil {
		t.Fatalf("ChunkDocument() returned %v, want no error", err)
	}
	if structured.callCount != 1 {
		t.Errorf("structured chunker called %d times, want 1", structured.callCount)
	}
	if len(chunks) != 1 || len(chunks[0].BlockIDs) != 1 {
		t.Fatalf("ChunkDocument() = %#v, want one chunk carrying one block id", chunks)
	}
}

// withLangchainClient sets the package level client the text splitters need
// and puts it back afterwards.
func withLangchainClient(t *testing.T) {
	t.Helper()

	previous := langchainClient
	langchainClient = langchain.NewClient(langchain.ClientConfig{MaxConcurrentRequests: 1})
	t.Cleanup(func() { langchainClient = previous })
}
