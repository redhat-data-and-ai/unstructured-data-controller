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
	"bytes"
	"cmp"
	"context"
	"errors"
	"fmt"
	"slices"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

// ErrNoStructuredDocument reports a converted file that holds only markdown.
//
// Expected for anything converted before structured output was asked for.
// Retrying cannot help while the converted file stays as it is, so the caller
// should treat it as a reason to stop rather than as a chunking failure.
var ErrNoStructuredDocument = errors.New("the converted file holds no structured document, only markdown")

// hasStructuredDocument reports whether a converted file carries a document
// worth sending or indexing.
//
// A length test alone is not enough, which is the same rule
// docling.DoclingResponseDocument.JSONDocument states: the field is absent
// when nothing asked for it and the JSON literal null when docling ran and
// produced nothing, and null is four bytes. Letting null through sends "null"
// to be chunked, and unmarshals clean into an empty index here, which would
// resolve every chunk to no regions at all and say nothing about it.
func hasStructuredDocument(document *ConvertedDocument) bool {
	if document == nil || document.Metadata == nil || document.Content == nil {
		return false
	}
	trimmed := bytes.TrimSpace(document.Content.DoclingDocument)
	return len(trimmed) > 0 && !bytes.Equal(trimmed, []byte("null"))
}

// DoclingChunker splits on the document's structure by handing the stored
// document back to the conversion service. It is the only chunker that can
// produce a citation, because it is the only one that knows which elements a
// chunk came from.
//
// Its work outlives the call, so it implements AsyncChunker too.
type DoclingChunker struct {
	Client  *docling.Client
	Options docling.ChunkingOptions
}

// Dropping Collect by accident would otherwise surface as a failed type
// assertion minutes later, on the collect path.
var (
	_ Chunker      = (*DoclingChunker)(nil)
	_ AsyncChunker = (*DoclingChunker)(nil)
)

// Chunk submits the stored document and returns the accepted task, not
// chunks: the work has only started.
//
// The stored document is sent rather than the original file. A second
// conversion produces a different document, and a chunk names its elements
// by address, so those addresses would then point at the wrong elements.
func (c *DoclingChunker) Chunk(ctx context.Context, document *ConvertedDocument) (ChunkResult, error) {
	if !hasStructuredDocument(document) {
		return ChunkResult{}, ErrNoStructuredDocument
	}

	response, err := c.Client.ChunkDoclingDocument(
		ctx, document.Metadata.RawFilePath, document.Content.DoclingDocument, c.Options)
	if err != nil {
		return ChunkResult{}, err
	}

	return ChunkResult{TaskID: response.TaskID}, nil
}

// Collect returns a started task's chunks, with their elements already
// resolved to rectangles.
//
// A running task comes back pending rather than as an error. One that ended
// badly wraps docling.ErrChunkTaskEnded, so the caller can tell it from a
// poll it simply could not read.
func (c *DoclingChunker) Collect(
	ctx context.Context, taskID string, document *ConvertedDocument,
) (ChunkResult, error) {
	logger := log.FromContext(ctx)

	status, response, err := c.Client.GetChunks(ctx, taskID)
	if err != nil && !errors.Is(err, docling.ErrNoChunksReturned) {
		return ChunkResult{}, err
	}

	// Still working. Reported as pending so the caller holds onto the job,
	// exactly as it does for the result of Chunk.
	if status == docling.TaskStatusPending || status == docling.TaskStatusStarted {
		return ChunkResult{TaskID: taskID}, nil
	}

	// A successful task with no chunks is deliberately not an error here. It
	// is both how the chunker reports being unable to load its tokenizer and
	// what a document with no content legitimately produces, and only the
	// document tells them apart, so the caller decides.
	chunks := []docling.Chunk{}
	if response != nil {
		chunks = response.Chunks
	}

	resolved, err := c.resolve(ctx, chunks, document)
	if err != nil {
		// The task has ended and given its slot back, so the caller must not
		// poll it again whatever went wrong after that.
		return ChunkResult{}, fmt.Errorf("%w: %w", docling.ErrChunkTaskEnded, err)
	}

	logger.Info("collected chunks from the conversion service", "taskID", taskID, "chunks", len(resolved))
	return ChunkResult{Chunks: resolved}, nil
}

// resolve turns the service's chunks into ours, which mostly means finding
// each chunk's rectangles in the document it was built from.
func (*DoclingChunker) resolve(
	ctx context.Context, chunks []docling.Chunk, document *ConvertedDocument,
) ([]Chunk, error) {
	logger := log.FromContext(ctx)

	if !hasStructuredDocument(document) {
		return nil, ErrNoStructuredDocument
	}

	// Built once per document. The chunks of one file all resolve against
	// the same document, and building this per chunk would reparse several
	// megabytes of JSON a hundred times over.
	index, err := docling.NewDocumentIndex(document.Content.DoclingDocument)
	if err != nil {
		return nil, err
	}

	// The service numbers its chunks and the stored artifact is read by
	// position, so the two have to agree. Sorting costs nothing and the
	// alternative is an artifact pairing one chunk's text with another
	// chunk's provenance, which nothing downstream can detect.
	ordered := slices.Clone(chunks)
	slices.SortFunc(ordered, func(a, b docling.Chunk) int {
		return cmp.Compare(a.ChunkIndex, b.ChunkIndex)
	})

	resolved := make([]Chunk, 0, len(ordered))
	unresolvedTotal := 0
	for _, chunk := range ordered {
		regions, unresolved := index.Regions(chunk.DocItems)
		unresolvedTotal += len(unresolved)

		first, last := pageRange(chunk.PageNumbers)
		resolved = append(resolved, Chunk{
			Text:      chunk.Text,
			RawText:   chunk.RawText,
			BlockIDs:  chunk.DocItems,
			Regions:   regions,
			PageFirst: first,
			PageLast:  last,
		})
	}

	// Logged rather than failed. It means the chunks and the document
	// disagree about what is in the document, which is worth seeing, but a
	// chunk with some of its rectangles still beats one with none.
	if unresolvedTotal > 0 {
		logger.Info("some chunk elements could not be placed on a page",
			"unresolved", unresolvedTotal, "chunks", len(resolved))
	}

	return resolved, nil
}

// pageRange returns the lowest and highest page a chunk touches, or zero for
// a format that has no pages.
func pageRange(pageNumbers []int) (first, last int) {
	if len(pageNumbers) == 0 {
		return 0, 0
	}
	return slices.Min(pageNumbers), slices.Max(pageNumbers)
}
