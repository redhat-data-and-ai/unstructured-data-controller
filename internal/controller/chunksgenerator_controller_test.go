package controller

import (
	"context"
	"reflect"
	"testing"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/langchain"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
	"github.com/tmc/langchaingo/textsplitter"
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

	// Every sub-config is filled with a distinct value, so a field read from
	// the wrong sub-config shows up as a mismatch rather than two zeroes
	// comparing equal.
	config := &operatorv1alpha1.ChunksGeneratorConfig{
		RecursiveCharacterSplitterConfig: operatorv1alpha1.RecursiveCharacterSplitterConfig{
			Separators:    []string{"\n\n", "\n"},
			ChunkSize:     111,
			ChunkOverlap:  11,
			KeepSeparator: true,
		},
		MarkdownSplitterConfig: operatorv1alpha1.MarkdownSplitterConfig{
			ChunkSize:        222,
			ChunkOverlap:     22,
			CodeBlocks:       true,
			ReferenceLinks:   true,
			HeadingHierarchy: true,
			JoinTableRows:    false,
		},
		TokenSplitterConfig: operatorv1alpha1.TokenSplitterConfig{
			ChunkSize:         333,
			ChunkOverlap:      33,
			ModelName:         "a-model",
			EncodingName:      "an-encoding",
			AllowedSpecial:    []string{"allowed"},
			DisallowedSpecial: []string{"disallowed"},
		},
	}

	tests := map[string]struct {
		strategy operatorv1alpha1.ChunkingStrategy
		want     unstructured.Chunker
		wantErr  bool
	}{
		"recursive character": {
			strategy: operatorv1alpha1.ChunkingStrategyRecursiveCharacter,
			want: &unstructured.RecursiveCharacterSplitter{
				LangchainClient: splitterClient,
				Config: &textsplitter.Options{
					Separators:    []string{"\n\n", "\n"},
					ChunkSize:     111,
					ChunkOverlap:  11,
					KeepSeparator: true,
				},
			},
		},
		"markdown": {
			strategy: operatorv1alpha1.ChunkingStrategyMarkdown,
			want: &unstructured.MarkdownSplitter{
				LangchainClient: splitterClient,
				Config: &textsplitter.Options{
					ChunkSize:            222,
					ChunkOverlap:         22,
					CodeBlocks:           true,
					ReferenceLinks:       true,
					KeepHeadingHierarchy: true,
					JoinTableRows:        false,
				},
			},
		},
		"token": {
			strategy: operatorv1alpha1.ChunkingStrategyToken,
			want: &unstructured.TokenSplitter{
				LangchainClient: splitterClient,
				Config: &textsplitter.Options{
					ChunkSize:         333,
					ChunkOverlap:      33,
					ModelName:         "a-model",
					EncodingName:      "an-encoding",
					AllowedSpecial:    []string{"allowed"},
					DisallowedSpecial: []string{"disallowed"},
				},
			},
		},
		"unknown strategy is an error": {
			strategy: operatorv1alpha1.ChunkingStrategy("no-such-strategy"),
			wantErr:  true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			strategyConfig := *config
			strategyConfig.Strategy = test.strategy

			got, err := textChunkerFor(splitterClient, &strategyConfig)
			if test.wantErr {
				if err == nil {
					t.Fatalf("textChunkerFor(%q) returned no error, want one", test.strategy)
				}
				return
			}
			if err != nil {
				t.Fatalf("textChunkerFor(%q) returned %v, want no error", test.strategy, err)
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("textChunkerFor(%q) = %T%+v, want %T%+v",
					test.strategy, got, splitterOptions(got), test.want, splitterOptions(test.want))
			}
		})
	}
}

// splitterOptions reads the Config field every splitter type has, so a failed
// comparison prints the options that differ instead of a pointer address.
func splitterOptions(chunker unstructured.Chunker) any {
	config := reflect.Indirect(reflect.ValueOf(chunker)).FieldByName("Config")
	if !config.IsValid() || config.IsNil() {
		return nil
	}
	return config.Elem().Interface()
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

	// The text splitter path still produces the same flat list of strings.
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

func TestDocumentChunkerRejectsAnUnknownStrategy(t *testing.T) {
	withLangchainClient(t)

	// With no structured chunker there is nothing to fall back to, so an
	// unrecognised strategy has to surface as an error rather than silently
	// picking a splitter.
	reconciler := &ChunksGeneratorReconciler{}
	config := &operatorv1alpha1.ChunksGeneratorConfig{
		Strategy: operatorv1alpha1.ChunkingStrategy("no-such-strategy"),
	}

	chunker, err := reconciler.documentChunker(config)
	if err == nil {
		t.Fatalf("documentChunker() = %#v, want an error", chunker)
	}
	if chunker != nil {
		t.Errorf("documentChunker() = %#v, want nil alongside the error", chunker)
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
