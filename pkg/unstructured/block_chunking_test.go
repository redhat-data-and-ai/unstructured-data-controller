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
	"errors"
	"strings"
	"testing"
)

func block(index int, class, text, content string) Block {
	return Block{Index: index, Class: class, Text: text, Content: content}
}

func defaultOptions() BlockChunkOptions {
	return BlockChunkOptions{DocID: "doc-1", DocTitle: "financial-report.pdf"}
}

func TestChunksFromPages_OneChunkPerIndexableBlock(t *testing.T) {
	pages := []Page{{
		PageNumber: 1,
		Blocks: []Block{
			block(0, "Title", "Quarterly Report", "Quarterly Report"),
			block(1, "Text", "Revenue grew.", "Revenue grew."),
			block(2, "Table", `\begin{tabular}`, "<table></table>"),
		},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 3 {
		t.Fatalf("expected 3 chunks, got %d", len(chunks))
	}
	for i, want := range []string{"Title", "Text", "Table"} {
		if chunks[i].BlockClass != want {
			t.Errorf("chunk %d: expected class %q, got %q", i, want, chunks[i].BlockClass)
		}
		if chunks[i].PageNumber != 1 {
			t.Errorf("chunk %d: expected page 1, got %d", i, chunks[i].PageNumber)
		}
		if chunks[i].BlockIndex != i {
			t.Errorf("chunk %d: expected block index %d, got %d", i, i, chunks[i].BlockIndex)
		}
	}
}

func TestChunksFromPages_SkipsPageFurnitureAndPictures(t *testing.T) {
	// Mirrors SKIP_CLASSES in the reference implementation.
	pages := []Page{{
		PageNumber: 1,
		Blocks: []Block{
			block(0, "Page-header", "Confidential", "Confidential"),
			block(1, "Text", "Body text.", "Body text."),
			block(2, "Picture", "a chart", "a chart"),
			block(3, "Page-footer", "Page 1 of 9", "Page 1 of 9"),
		},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected only the Text block to chunk, got %d chunks", len(chunks))
	}
	if chunks[0].BlockClass != "Text" {
		t.Errorf("expected Text, got %q", chunks[0].BlockClass)
	}
}

func TestChunksFromPages_RecordsPageHeaderAndFooterOnEveryChunk(t *testing.T) {
	pages := []Page{{
		PageNumber: 2,
		Blocks: []Block{
			block(0, "Page-header", "Confidential", "Confidential"),
			block(1, "Text", "First.", "First."),
			block(2, "Text", "Second.", "Second."),
			block(3, "Page-footer", "Page 2", "Page 2"),
		},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, chunk := range chunks {
		if chunk.PageHeader != "Confidential" {
			t.Errorf("chunk %d: expected page header carried over, got %q", i, chunk.PageHeader)
		}
		if chunk.PageFooter != "Page 2" {
			t.Errorf("chunk %d: expected page footer carried over, got %q", i, chunk.PageFooter)
		}
	}
}

func TestChunksFromPages_SectionHeaderCarriesForward(t *testing.T) {
	pages := []Page{{
		PageNumber: 1,
		Blocks: []Block{
			block(0, "Section-header", "Income Statement", "Income Statement"),
			block(1, "Table", "", "<table></table>"),
			block(2, "Section-header", "Balance Sheet", "Balance Sheet"),
			block(3, "Table", "", "<table>2</table>"),
		},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chunks[1].SectionHeader != "Income Statement" {
		t.Errorf("expected first table under Income Statement, got %q", chunks[1].SectionHeader)
	}
	if chunks[3].SectionHeader != "Balance Sheet" {
		t.Errorf("expected second table under Balance Sheet, got %q", chunks[3].SectionHeader)
	}
}

func TestChunksFromPages_SectionTrackerUpdatesBeforeSkip(t *testing.T) {
	// A Title that is itself skipped must still set the section for the blocks
	// beneath it. This is why the reference implementation updates the tracker
	// before the skip check rather than after.
	pages := []Page{{
		PageNumber: 1,
		Blocks: []Block{
			block(0, "Title", "Annual Report", "Annual Report"),
			block(1, "Picture", "logo", "logo"),
			block(2, "Text", "Body.", "Body."),
		},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body := chunks[len(chunks)-1]
	if body.SectionHeader != "Annual Report" {
		t.Errorf("expected section to survive the skipped block, got %q", body.SectionHeader)
	}
}

func TestChunksFromPages_SectionResetsPerPage(t *testing.T) {
	pages := []Page{
		{PageNumber: 1, Blocks: []Block{
			block(0, "Section-header", "Page One Section", "Page One Section"),
		}},
		{PageNumber: 2, Blocks: []Block{
			block(0, "Text", "Body on page two.", "Body on page two."),
		}},
	}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	page2 := chunks[len(chunks)-1]
	if page2.SectionHeader != "" {
		t.Errorf("section must not leak across pages, got %q", page2.SectionHeader)
	}
}

func TestChunksFromPages_DropsEmptyBlocks(t *testing.T) {
	pages := []Page{{
		PageNumber: 1,
		Blocks: []Block{
			block(0, "Text", "", ""),
			block(1, "Text", "   \n  ", "   \n  "),
			block(2, "Text", "Real content.", "Real content."),
		},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("expected empty blocks to be dropped, got %d chunks", len(chunks))
	}
}

func TestChunksFromPages_PrefersContentOverText(t *testing.T) {
	// Tables must index as HTML, not raw LaTeX.
	pages := []Page{{
		PageNumber: 1,
		Blocks:     []Block{block(0, "Table", `\begin{tabular}{cc}A & B\end{tabular}`, "<table><tr><td>A</td></tr></table>")},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !strings.HasPrefix(chunks[0].Text, "<table>") {
		t.Errorf("expected HTML content, got %q", chunks[0].Text)
	}
}

func TestChunksFromPages_FallsBackToTextWhenContentEmpty(t *testing.T) {
	pages := []Page{{
		PageNumber: 1,
		Blocks:     []Block{block(0, "Text", "only raw text", "")},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chunks[0].Text != "only raw text" {
		t.Errorf("expected the raw text, got %q", chunks[0].Text)
	}
}

func TestEmbedText_MatchesReferenceFormat(t *testing.T) {
	// Newline-joined, "class" not "type". The original design doc described a
	// pipe-separated prefix; the reference implementation does not do that.
	pages := []Page{{
		PageNumber: 3,
		Blocks: []Block{
			block(0, "Section-header", "Income Statement", "Income Statement"),
			block(1, "Table", "", "<table></table>"),
		},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := "document: financial-report.pdf\npage: 3\nclass: Table\nsection: Income Statement\n<table></table>"
	if chunks[1].EmbedText != want {
		t.Errorf("embed text mismatch:\n got: %q\nwant: %q", chunks[1].EmbedText, want)
	}
}

func TestEmbedText_OmitsSectionLineWhenAbsent(t *testing.T) {
	pages := []Page{{
		PageNumber: 1,
		Blocks:     []Block{block(0, "Text", "Body.", "Body.")},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if strings.Contains(chunks[0].EmbedText, "section:") {
		t.Errorf("expected no section line, got %q", chunks[0].EmbedText)
	}
	want := "document: financial-report.pdf\npage: 1\nclass: Text\nBody."
	if chunks[0].EmbedText != want {
		t.Errorf("embed text mismatch:\n got: %q\nwant: %q", chunks[0].EmbedText, want)
	}
}

func TestChunkID_IsStableAndDistinct(t *testing.T) {
	pages := []Page{{
		PageNumber: 1,
		Blocks: []Block{
			block(0, "Text", "First.", "First."),
			block(1, "Text", "Second.", "Second."),
		},
	}}

	first, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	second, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if first[0].ChunkID != second[0].ChunkID {
		t.Error("chunk id is not stable across runs")
	}
	if first[0].ChunkID == first[1].ChunkID {
		t.Error("distinct blocks produced the same chunk id")
	}
	if len(first[0].ChunkID) != chunkIDLength {
		t.Errorf("expected a %d-character id, got %d", chunkIDLength, len(first[0].ChunkID))
	}
}

func TestChunkID_ChangesWithDocument(t *testing.T) {
	pages := []Page{{PageNumber: 1, Blocks: []Block{block(0, "Text", "Same.", "Same.")}}}

	a, _ := ChunksFromPages(pages, BlockChunkOptions{DocID: "doc-a", DocTitle: "a.pdf"})
	b, _ := ChunksFromPages(pages, BlockChunkOptions{DocID: "doc-b", DocTitle: "b.pdf"})

	if a[0].ChunkID == b[0].ChunkID {
		t.Error("identical blocks in different documents must not share a chunk id")
	}
}

func TestChunksFromPages_CarriesCropURIAndBBox(t *testing.T) {
	pages := []Page{{
		PageNumber: 1,
		Blocks: []Block{{
			Index: 0, Class: "Table", Content: "<table></table>",
			BBox:    []float64{0.1, 0.2, 0.9, 0.4},
			CropURI: "pipelines/p/stages/convert/assets/f1/p001_b000.png",
		}},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if chunks[0].CropURI != "pipelines/p/stages/convert/assets/f1/p001_b000.png" {
		t.Errorf("crop uri not carried, got %q", chunks[0].CropURI)
	}
	if len(chunks[0].BBox) != 4 {
		t.Errorf("expected a 4-element bbox, got %v", chunks[0].BBox)
	}
}

// -- splitting -------------------------------------------------------------

type fixedSplitter struct {
	parts []string
	err   error
}

func (f *fixedSplitter) Chunk(string) ([]string, error) { return f.parts, f.err }

func TestSplitting_DisabledByDefaultMatchesReference(t *testing.T) {
	long := strings.Repeat("x", 50_000)
	pages := []Page{{PageNumber: 1, Blocks: []Block{block(0, "Table", "", long)}}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 1 {
		t.Fatalf("splitting must be off by default, got %d chunks", len(chunks))
	}
	if len(chunks[0].Text) != 50_000 {
		t.Error("body was modified while splitting was disabled")
	}
}

func TestSplitting_OversizedBlockBecomesMultipleParts(t *testing.T) {
	long := strings.Repeat("x", 5_000)
	pages := []Page{{PageNumber: 1, Blocks: []Block{block(0, "Table", "", long)}}}

	options := defaultOptions()
	options.MaxChunkChars = 1_000
	options.Splitter = &fixedSplitter{parts: []string{"part one", "part two", "part three"}}

	chunks, err := ChunksFromPages(pages, options)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 3 {
		t.Fatalf("expected 3 parts, got %d", len(chunks))
	}
	for i, chunk := range chunks {
		if chunk.PartIndex != i {
			t.Errorf("part %d: expected part index %d, got %d", i, i, chunk.PartIndex)
		}
		// Block metadata is copied onto every part.
		if chunk.BlockIndex != 0 || chunk.BlockClass != "Table" || chunk.PageNumber != 1 {
			t.Errorf("part %d lost its block metadata: %+v", i, chunk)
		}
	}
	if chunks[0].ChunkID == chunks[1].ChunkID {
		t.Error("parts of one block must not share a chunk id")
	}
}

func TestSplitting_BudgetAccountsForTheMetadataPrefix(t *testing.T) {
	// The embedding model sees embedText, not the bare body. Sizing the
	// split against the body alone lets every chunk exceed the cap by the
	// length of its own prefix, which is what sent a 3943-character chunk to
	// a model with a 2048-token window.
	const cap = 1000
	longSection := strings.Repeat("S", 300)

	options := defaultOptions()
	options.MaxChunkChars = cap
	options.Splitter = &fixedSplitter{parts: []string{"split happened"}}

	pages := []Page{{
		PageNumber: 1,
		Blocks: []Block{
			block(0, "Section-header", "", longSection),
			// Under the raw cap, but over it once the 300-character section
			// header and the rest of the prefix are prepended.
			block(1, "Table", "", strings.Repeat("x", 900)),
		},
	}}

	chunks, err := ChunksFromPages(pages, options)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	table := chunks[len(chunks)-1]
	if table.Text != "split happened" {
		t.Errorf("expected the oversized chunk to be split once the prefix was counted, got %d chars",
			len(table.Text))
	}
}

func TestEmbedText_StaysWithinTheConfiguredCap(t *testing.T) {
	const cap = 1200
	options := defaultOptions()
	options.MaxChunkChars = cap
	// Parts sized so the prefix cannot push them back over the cap.
	options.Splitter = &fixedSplitter{parts: []string{
		strings.Repeat("a", 500), strings.Repeat("b", 500),
	}}

	pages := []Page{{
		PageNumber: 1,
		Blocks: []Block{
			block(0, "Section-header", "", "Consolidated Statements of Operations"),
			block(1, "Table", "", strings.Repeat("x", 5000)),
		},
	}}

	chunks, err := ChunksFromPages(pages, options)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	for i, chunk := range chunks {
		if len(chunk.EmbedText) > cap {
			t.Errorf("chunk %d: embedText is %d chars, over the %d cap",
				i, len(chunk.EmbedText), cap)
		}
	}
}

func TestSplitting_BudgetIsFlooredForALongSectionHeader(t *testing.T) {
	// A pathological section header must not drive the budget to zero or
	// negative, which would split every block into noise.
	options := defaultOptions()
	options.MaxChunkChars = 100
	options.Splitter = &fixedSplitter{parts: []string{"part"}}

	pages := []Page{{
		PageNumber: 1,
		Blocks: []Block{
			block(0, "Section-header", "", strings.Repeat("H", 500)),
			block(1, "Text", "", "short body"),
		},
	}}

	chunks, err := ChunksFromPages(pages, options)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	body := chunks[len(chunks)-1]
	if body.Text != "short body" {
		t.Errorf("a body under the floor should not be split, got %q", body.Text)
	}
}

func TestSplitting_SmallBlockIsLeftAlone(t *testing.T) {
	options := defaultOptions()
	options.MaxChunkChars = 1_000
	options.Splitter = &fixedSplitter{parts: []string{"should", "not", "be", "used"}}

	pages := []Page{{PageNumber: 1, Blocks: []Block{block(0, "Text", "", "short body")}}}

	chunks, err := ChunksFromPages(pages, options)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 1 || chunks[0].Text != "short body" {
		t.Errorf("a block under the limit must not be split, got %d chunks", len(chunks))
	}
	if chunks[0].PartIndex != 0 {
		t.Errorf("an unsplit block should have part index 0, got %d", chunks[0].PartIndex)
	}
}

func TestSplitting_MissingSplitterIsAnError(t *testing.T) {
	options := defaultOptions()
	options.MaxChunkChars = 1000

	pages := []Page{{PageNumber: 1, Blocks: []Block{block(0, "Text", "", strings.Repeat("y", 3000))}}}

	_, err := ChunksFromPages(pages, options)
	if err == nil || !strings.Contains(err.Error(), "no splitter") {
		t.Errorf("expected a missing-splitter error, got %v", err)
	}
}

func TestSplitting_SplitterErrorIsWrapped(t *testing.T) {
	options := defaultOptions()
	options.MaxChunkChars = 1000
	options.Splitter = &fixedSplitter{err: errors.New("splitter exploded")}

	pages := []Page{{PageNumber: 1, Blocks: []Block{block(0, "Text", "", strings.Repeat("y", 3000))}}}

	_, err := ChunksFromPages(pages, options)
	if err == nil {
		t.Fatal("expected an error")
	}
	if !strings.Contains(err.Error(), "splitter exploded") {
		t.Errorf("expected the splitter error to be wrapped, got %v", err)
	}
	if !strings.Contains(err.Error(), "page 1") {
		t.Errorf("expected the error to name the page, got %v", err)
	}
}

func TestChunkTexts_MirrorsChunkBodies(t *testing.T) {
	pages := []Page{{
		PageNumber: 1,
		Blocks: []Block{
			block(0, "Text", "", "one"),
			block(1, "Text", "", "two"),
		},
	}}

	chunks, err := ChunksFromPages(pages, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	texts := ChunkTexts(chunks)
	if len(texts) != 2 || texts[0] != "one" || texts[1] != "two" {
		t.Errorf("unexpected texts: %v", texts)
	}
}

func TestChunksFromPages_NoPagesYieldsNoChunks(t *testing.T) {
	chunks, err := ChunksFromPages(nil, defaultOptions())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(chunks) != 0 {
		t.Errorf("expected no chunks, got %d", len(chunks))
	}
}
