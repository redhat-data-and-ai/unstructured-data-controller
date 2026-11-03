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

package unstructured_test

import (
	"context"
	"errors"
	"testing"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

// stubChunker returns a fixed result, so a test can drive TextChunkerAdapter
// without a langchain client.
type stubChunker struct {
	texts []string
	err   error

	calledWith string
	callCount  int
}

func (s *stubChunker) Chunk(text string) ([]string, error) {
	s.calledWith = text
	s.callCount++
	return s.texts, s.err
}

func TestChunkTexts(t *testing.T) {
	t.Parallel()

	tests := map[string]struct {
		chunks []unstructured.DocumentChunk
		want   []string
	}{
		"nil stays nil so the artifact serialises the same way": {
			chunks: nil,
			want:   nil,
		},
		"empty stays empty": {
			chunks: []unstructured.DocumentChunk{},
			want:   []string{},
		},
		"text is taken in order and block ids are dropped": {
			chunks: []unstructured.DocumentChunk{
				{Text: "first", BlockIDs: []string{"#/texts/0"}},
				{Text: "second", BlockIDs: []string{"#/texts/1", "#/texts/2"}},
				{Text: "third"},
			},
			want: []string{"first", "second", "third"},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			got := unstructured.ChunkTexts(test.chunks)
			if (got == nil) != (test.want == nil) {
				t.Fatalf("ChunkTexts() = %#v, want %#v", got, test.want)
			}
			if len(got) != len(test.want) {
				t.Fatalf("ChunkTexts() = %#v, want %#v", got, test.want)
			}
			for i := range got {
				if got[i] != test.want[i] {
					t.Errorf("ChunkTexts()[%d] = %q, want %q", i, got[i], test.want[i])
				}
			}
		})
	}
}

func TestTextChunkerAdapterChunksTheMarkdown(t *testing.T) {
	t.Parallel()

	chunker := &stubChunker{texts: []string{"one", "two"}}
	adapter := &unstructured.TextChunkerAdapter{Chunker: chunker}
	doc := &unstructured.ConvertedDocument{
		Content: &unstructured.Content{Markdown: "# heading\n\nbody"},
	}

	chunks, err := adapter.ChunkDocument(context.Background(), doc)
	if err != nil {
		t.Fatalf("ChunkDocument() returned %v, want no error", err)
	}

	if chunker.calledWith != doc.Content.Markdown {
		t.Errorf("wrapped chunker got %q, want %q", chunker.calledWith, doc.Content.Markdown)
	}
	if chunker.callCount != 1 {
		t.Errorf("wrapped chunker called %d times, want 1", chunker.callCount)
	}

	want := []string{"one", "two"}
	if len(chunks) != len(want) {
		t.Fatalf("ChunkDocument() returned %d chunks, want %d", len(chunks), len(want))
	}
	for i, chunk := range chunks {
		if chunk.Text != want[i] {
			t.Errorf("chunk %d text = %q, want %q", i, chunk.Text, want[i])
		}
		// Markdown carries no block identifiers, so the passthrough must not
		// invent any.
		if len(chunk.BlockIDs) != 0 {
			t.Errorf("chunk %d has block ids %v, want none", i, chunk.BlockIDs)
		}
	}
}

func TestTextChunkerAdapterKeepsANilResultNil(t *testing.T) {
	t.Parallel()

	adapter := &unstructured.TextChunkerAdapter{Chunker: &stubChunker{texts: nil}}
	doc := &unstructured.ConvertedDocument{Content: &unstructured.Content{}}

	chunks, err := adapter.ChunkDocument(context.Background(), doc)
	if err != nil {
		t.Fatalf("ChunkDocument() returned %v, want no error", err)
	}
	if chunks != nil {
		t.Fatalf("ChunkDocument() = %#v, want nil", chunks)
	}
	if texts := unstructured.ChunkTexts(chunks); texts != nil {
		t.Fatalf("ChunkTexts() = %#v, want nil", texts)
	}
}

func TestTextChunkerAdapterPropagatesTheChunkerError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("splitter failed")
	adapter := &unstructured.TextChunkerAdapter{Chunker: &stubChunker{err: wantErr}}
	doc := &unstructured.ConvertedDocument{Content: &unstructured.Content{Markdown: "body"}}

	_, err := adapter.ChunkDocument(context.Background(), doc)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ChunkDocument() returned %v, want %v", err, wantErr)
	}
}

func TestTextChunkerAdapterRejectsAMissingChunker(t *testing.T) {
	t.Parallel()

	adapter := &unstructured.TextChunkerAdapter{}
	doc := &unstructured.ConvertedDocument{Content: &unstructured.Content{Markdown: "body"}}

	if _, err := adapter.ChunkDocument(context.Background(), doc); err == nil {
		t.Fatal("ChunkDocument() returned no error, want one")
	}
}

func TestTextChunkerAdapterRejectsADocumentWithNoContent(t *testing.T) {
	t.Parallel()

	tests := map[string]*unstructured.ConvertedDocument{
		"nil document": nil,
		"nil content":  {},
	}

	for name, doc := range tests {
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			chunker := &stubChunker{texts: []string{"unused"}}
			adapter := &unstructured.TextChunkerAdapter{Chunker: chunker}

			if _, err := adapter.ChunkDocument(context.Background(), doc); err == nil {
				t.Fatal("ChunkDocument() returned no error, want one")
			}
			if chunker.callCount != 0 {
				t.Errorf("wrapped chunker called %d times, want 0", chunker.callCount)
			}
		})
	}
}
