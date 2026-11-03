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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"slices"
	"strings"
)

// Block-based chunking: one semantic block becomes one chunk, carrying the
// page, class, section and bounding box that the flat markdown splitter throws
// away.
//
// This is a port of the reference implementation at
// nemotron_rag/chunking/from_page_json.py and Chunk.embed_text() in
// nemotron_rag/models.py. Behaviour is kept identical to that code, including
// the parts that look incidental: the section tracker updates before the skip
// check, blocks with an empty body are dropped, and content is preferred over
// text so tables chunk as HTML rather than LaTeX.

// chunkIDLength is the number of hex characters kept from the sha256 digest,
// matching _stable_id() in the reference implementation.
const chunkIDLength = 24

// embedTextBodyPrefixLength is how much of the body feeds the chunk ID hash.
const embedTextBodyPrefixLength = 80

// minChunkChars floors the per-chunk budget so an unusually long section
// header cannot shrink it to nothing.
const minChunkChars = 256

// titleClasses update the running section header. Matches the reference
// implementation's `if cls in {"Title", "Section-header"}`.
var titleClasses = []string{"Title", "Section-header"}

// skipClasses are dropped rather than chunked. Mirrors SKIP_CLASSES in
// nemotron_rag/models.py.
var skipClasses = []string{"Page-header", "Page-footer", "Picture"}

// BlockChunkOptions tunes block chunking.
type BlockChunkOptions struct {
	// DocID identifies the source document in the chunk ID hash. The pipeline
	// uses the file identifier, which plays the role of the reference
	// implementation's sha256-of-folder-path.
	DocID string
	// DocTitle appears in the embed text. The pipeline uses the file name.
	DocTitle string

	// MaxChunkChars splits any block whose body exceeds it, carrying the block
	// metadata onto every part. Zero disables splitting, which is the
	// reference implementation's behaviour.
	//
	// This exists because block chunking produces unbounded chunks where the
	// previous splitter capped them at 1000 characters: a full-page table can
	// be tens of kilobytes of HTML and would be silently truncated by the
	// embedding model's context window. Splitting is preferred over
	// truncation because truncation loses the tail of the table without any
	// signal.
	MaxChunkChars int
	// Splitter performs the split when MaxChunkChars is exceeded. Required
	// only when MaxChunkChars is non-zero.
	Splitter Chunker
}

// ChunksFromPages converts parsed pages into chunks, one per indexable block.
func ChunksFromPages(pages []Page, options BlockChunkOptions) ([]BlockChunk, error) {
	chunks := []BlockChunk{}

	for _, page := range pages {
		pageHeader, pageFooter := pageFurniture(page.Blocks)

		// The section header carries across blocks within a page, so a table
		// inherits the heading that introduced it.
		sectionHeader := ""

		for index, block := range page.Blocks {
			body := blockBody(block)

			// Updated before the skip check so a Title that is itself skipped
			// still contributes context to the blocks beneath it.
			if slices.Contains(titleClasses, block.Class) && body != "" {
				sectionHeader = body
			}

			if slices.Contains(skipClasses, block.Class) {
				continue
			}
			if body == "" {
				continue
			}

			base := BlockChunk{
				ChunkID:       chunkID(options.DocID, page.PageNumber, index, block.Class, body),
				Text:          body,
				BlockClass:    block.Class,
				PageNumber:    page.PageNumber,
				BlockIndex:    index,
				SectionHeader: sectionHeader,
				PageHeader:    pageHeader,
				PageFooter:    pageFooter,
				BBox:          block.BBox,
				CropURI:       block.CropURI,
			}

			// The embedding model sees embedText, not body, so the budget has
			// to be what is left after the metadata prefix. Sizing the split
			// against body alone lets every chunk exceed the cap by the
			// length of its own prefix.
			budget := options.MaxChunkChars
			if budget > 0 {
				budget -= len(embedTextPrefix(options.DocTitle, base))
				if budget < minChunkChars {
					budget = minChunkChars
				}
			}

			parts, err := splitBody(body, budget, options.Splitter)
			if err != nil {
				return nil, fmt.Errorf("splitting block %d on page %d: %w", index, page.PageNumber, err)
			}

			for partIndex, part := range parts {
				chunk := base
				chunk.Text = part
				if len(parts) > 1 {
					// Only a split block carries a part index, and each part
					// needs its own ID or they collide in the chunk table.
					chunk.PartIndex = partIndex
					chunk.ChunkID = chunkID(options.DocID, page.PageNumber,
						index, block.Class, fmt.Sprintf("%d|%s", partIndex, part))
				}
				chunk.EmbedText = embedText(options.DocTitle, chunk)
				chunks = append(chunks, chunk)
			}
		}
	}

	return chunks, nil
}

// blockBody picks the text to index, preferring the postprocessed content so
// tables are chunked as HTML rather than raw LaTeX. Mirrors _clean_body().
func blockBody(block Block) string {
	if strings.TrimSpace(block.Content) != "" {
		return strings.TrimSpace(block.Content)
	}
	return strings.TrimSpace(block.Text)
}

// pageFurniture returns the first non-empty running header and footer on a
// page. These are not chunked themselves but are recorded on every chunk from
// the page, as the reference implementation does.
func pageFurniture(blocks []Block) (header, footer string) {
	for _, block := range blocks {
		body := blockBody(block)
		if body == "" {
			continue
		}
		if block.Class == "Page-header" && header == "" {
			header = body
		}
		if block.Class == "Page-footer" && footer == "" {
			footer = body
		}
	}
	return header, footer
}

// chunkID is a stable identifier for a block. Matches _stable_id(): sha256
// over pipe-joined parts, truncated to 24 hex characters.
func chunkID(docID string, pageNumber, blockIndex int, blockClass, body string) string {
	prefix := body
	if len(prefix) > embedTextBodyPrefixLength {
		prefix = prefix[:embedTextBodyPrefixLength]
	}
	parts := []string{
		docID,
		// Stands in for the reference implementation's page file stem, which
		// has no equivalent in this pipeline.
		fmt.Sprintf("page_%03d", pageNumber),
		fmt.Sprintf("%d", blockIndex),
		blockClass,
		prefix,
	}
	digest := sha256.Sum256([]byte(strings.Join(parts, "|")))
	return hex.EncodeToString(digest[:])[:chunkIDLength]
}

// embedTextPrefix is the metadata header prepended to a chunk's body, with
// the trailing newline that separates it from the body.
func embedTextPrefix(docTitle string, chunk BlockChunk) string {
	parts := []string{
		fmt.Sprintf("document: %s", docTitle),
		fmt.Sprintf("page: %d", chunk.PageNumber),
		fmt.Sprintf("class: %s", chunk.BlockClass),
	}
	if chunk.SectionHeader != "" {
		parts = append(parts, fmt.Sprintf("section: %s", chunk.SectionHeader))
	}
	return strings.Join(parts, "\n") + "\n"
}

// embedText builds the metadata-prefixed text handed to the embedding model.
// Matches Chunk.embed_text(): newline-joined, "class" not "type", and the
// section line omitted entirely when there is no section.
func embedText(docTitle string, chunk BlockChunk) string {
	return embedTextPrefix(docTitle, chunk) + chunk.Text
}

// splitBody returns the body unchanged unless it exceeds the budget.
func splitBody(body string, budget int, splitter Chunker) ([]string, error) {
	if budget <= 0 || len(body) <= budget {
		return []string{body}, nil
	}
	if splitter == nil {
		return nil, fmt.Errorf("a chunk exceeds the %d character budget but no splitter was configured", budget)
	}

	parts, err := splitter.Chunk(body)
	if err != nil {
		return nil, err
	}
	if len(parts) == 0 {
		return []string{body}, nil
	}
	return parts, nil
}

// ChunkTexts returns the plain text of each chunk, for populating the
// pre-existing chunks.text[] field so v1 consumers keep working.
func ChunkTexts(chunks []BlockChunk) []string {
	texts := make([]string, len(chunks))
	for i, chunk := range chunks {
		texts[i] = chunk.Text
	}
	return texts
}
