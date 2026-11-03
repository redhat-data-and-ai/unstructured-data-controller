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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// inlineSplitRendition is the shape a parser produces for a paragraph with
// inline markup. The whole block's rectangle lands on an element whose text
// field is empty, the runs holding the text are left with no placement, and
// a third element has neither and is the only one that should be dropped.
const inlineSplitRendition = `{
  "version": "1.7.0",
  "texts": [
    {
      "self_ref": "#/texts/0",
      "label": "text",
      "text": "",
      "orig": "",
      "prov": [{"page_no": 1, "bbox": {"l": 100, "t": 140, "r": 500, "b": 100,
                                       "coord_origin": "BOTTOMLEFT"}}]
    },
    {"self_ref": "#/texts/1", "label": "text", "text": "A", "prov": []},
    {"self_ref": "#/texts/2", "label": "text", "text": "bold", "prov": []},
    {"self_ref": "#/texts/3", "label": "text", "text": "", "prov": []}
  ]
}`

// TestExtractKeepsPlacedBlocksWithoutText is the behaviour the citation chain
// rests on. Docling attaches a block's rectangle to an empty-string wrapper
// element and clears the placement on the runs that carry the text, and the
// chunker then refers to the wrapper. Dropping elements that have no text of
// their own therefore removes exactly the ones that can be pointed at.
func TestExtractKeepsPlacedBlocksWithoutText(t *testing.T) {
	var content Content
	require.NoError(t, content.ExtractDoclingDocument(json.RawMessage(inlineSplitRendition)))

	ids := make([]string, 0, len(content.Blocks))
	for _, block := range content.Blocks {
		ids = append(ids, block.ID)
	}

	assert.Equal(t, []string{"#/texts/0", "#/texts/1", "#/texts/2"}, ids,
		"the placed but empty wrapper must be kept and only the element with neither text nor a placement dropped")

	require.NotEmpty(t, content.Blocks)
	placed := content.Blocks[0]
	assert.Empty(t, placed.Text)
	assert.Equal(t, 1, placed.PageNo)
	assert.Equal(t, [4]float64{100, 140, 500, 100}, placed.BBox)

	// Elements docling could not place are kept for their text, and say so by
	// reporting no page.
	assert.Equal(t, 0, content.Blocks[1].PageNo)
	assert.Equal(t, [4]float64{}, content.Blocks[1].BBox)
}

// TestExtractBlockIDIsTheParserSelfReference guards the link between a block
// and the stored document. The block ID is the only thing that resolves back
// to a rectangle, so an ID we generated ourselves would break the chain
// without breaking anything that looks at the artifact.
func TestExtractBlockIDIsTheParserSelfReference(t *testing.T) {
	const rendition = `{
      "texts": [{"self_ref": "#/texts/59", "label": "section_header", "text": "Revenue"}],
      "tables": [{"self_ref": "#/tables/3", "label": "table"}],
      "pictures": [{"self_ref": "#/pictures/1", "label": "picture"}]
    }`

	var content Content
	require.NoError(t, content.ExtractDoclingDocument(json.RawMessage(rendition)))

	require.Len(t, content.Blocks, 3)

	assert.Equal(t, "#/texts/59", content.Blocks[0].ID)
	assert.Equal(t, BlockClass("section_header"), content.Blocks[0].Class)
	assert.Equal(t, "Revenue", content.Blocks[0].Text)

	// Tables and pictures hold no text, so the text-or-placement test would
	// drop every one of them on a format with no pages.
	assert.Equal(t, "#/tables/3", content.Blocks[1].ID)
	assert.Equal(t, BlockClass("table"), content.Blocks[1].Class)
	assert.Empty(t, content.Blocks[1].Text)

	assert.Equal(t, "#/pictures/1", content.Blocks[2].ID)
	assert.Equal(t, BlockClass("picture"), content.Blocks[2].Class)
}

// TestExtractPagesAreOrdered checks the page list. Docling sends pages as an
// object keyed by page number, so ranging over them is in random order and
// two conversions of one document would otherwise disagree byte for byte.
func TestExtractPagesAreOrdered(t *testing.T) {
	const rendition = `{
      "pages": {
        "3": {"page_no": 3, "size": {"width": 612, "height": 792}},
        "1": {"page_no": 1, "size": {"width": 612, "height": 792}},
        "2": {"page_no": 2, "size": {"width": 792, "height": 612}}
      }
    }`

	var content Content
	require.NoError(t, content.ExtractDoclingDocument(json.RawMessage(rendition)))

	assert.Equal(t, []Page{
		{PageNo: 1, Width: 612, Height: 792},
		{PageNo: 2, Width: 792, Height: 612},
		{PageNo: 3, Width: 612, Height: 792},
	}, content.Pages)
}

// TestExtractUnpaginatedDocument covers markdown, HTML, AsciiDoc and CSV,
// which have no pages for a rectangle to sit on. The text still has to come
// through; only the geometry is missing.
func TestExtractUnpaginatedDocument(t *testing.T) {
	const rendition = `{
      "version": "1.7.0",
      "texts": [{"self_ref": "#/texts/0", "label": "title", "text": "Readme"}],
      "pages": {}
    }`

	var content Content
	require.NoError(t, content.ExtractDoclingDocument(json.RawMessage(rendition)))

	require.Len(t, content.Blocks, 1)
	assert.Equal(t, "Readme", content.Blocks[0].Text)
	assert.Equal(t, 0, content.Blocks[0].PageNo)
	assert.Nil(t, content.Pages)
}

// TestExtractRecordsTheParser checks that a stored document says what made
// it. Without that, a change of parser leaves every stored artifact looking
// valid with no way to tell which produced which.
func TestExtractRecordsTheParser(t *testing.T) {
	var content Content
	require.NoError(t, content.ExtractDoclingDocument(json.RawMessage(inlineSplitRendition)))

	require.NotNil(t, content.Parser)
	assert.Equal(t, "docling", content.Parser.Name)
	assert.Equal(t, "1.7.0", content.Parser.Version)
}

// TestExtractKeepsTheRenditionVerbatim matters because a block ID resolves by
// indexing into the rendition's arrays. Decoding into types of ours and
// building the document again would drop every field those types do not name,
// and nothing guarantees the addresses survive it.
func TestExtractKeepsTheRenditionVerbatim(t *testing.T) {
	rendition := json.RawMessage(inlineSplitRendition)

	var content Content
	require.NoError(t, content.ExtractDoclingDocument(rendition))

	assert.Equal(t, inlineSplitRendition, string(content.DoclingDocument))
}

// TestExtractRejectsARenditionItCannotUse covers the failure paths. The null
// case is the interesting one: unmarshalling the JSON literal null into a
// struct succeeds and leaves every field zero, so without an explicit check
// the artifact would record an empty document as though docling had sent one,
// and store the four bytes "null" that omitempty cannot drop.
func TestExtractRejectsARenditionItCannotUse(t *testing.T) {
	tests := []struct {
		name      string
		rendition string
		wantErr   error
	}{
		{name: "the literal null", rendition: `null`, wantErr: ErrNoDoclingDocument},
		{name: "null with whitespace round it", rendition: "  null\n", wantErr: ErrNoDoclingDocument},
		{name: "nothing at all", rendition: ``, wantErr: ErrNoDoclingDocument},
		{name: "truncated", rendition: `{"texts": `},
		{name: "not an object", rendition: `["#/texts/0"]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			content := Content{Markdown: "# heading"}

			err := content.ExtractDoclingDocument(json.RawMessage(tt.rendition))
			require.Error(t, err)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)
			} else {
				assert.Contains(t, err.Error(), "reading the structured document")
			}

			// Markdown survives and nothing else is written, so the artifact
			// is exactly what it would have been without a rendition.
			assert.Equal(t, "# heading", content.Markdown)
			assert.Nil(t, content.Blocks)
			assert.Nil(t, content.Pages)
			assert.Nil(t, content.Parser)
			assert.Nil(t, content.DoclingDocument)
		})
	}
}

// TestExtractReadsTextFromEitherField is the other half of keeping placed
// elements whose text field is empty. Several parser paths put the real
// content in the field beside it: docling does that for formulas, and the
// VLM parsers do it for a heading or list item whose content has inline
// markup. Reading only the first field stores a correct rectangle drawn
// around words that were thrown away.
func TestExtractReadsTextFromEitherField(t *testing.T) {
	const rendition = `{
      "texts": [
        {"self_ref": "#/texts/0", "label": "formula", "text": "", "orig": "E = mc^2",
         "prov": [{"page_no": 1, "bbox": {"l": 10, "t": 20, "r": 30, "b": 40}}]},
        {"self_ref": "#/texts/1", "label": "section_header", "text": "", "orig": "Revenue"},
        {"self_ref": "#/texts/2", "label": "text", "text": "cleaned", "orig": "raw"}
      ]
    }`

	var content Content
	require.NoError(t, content.ExtractDoclingDocument(json.RawMessage(rendition)))

	require.Len(t, content.Blocks, 3)

	// Placed, empty first field, content in the second.
	assert.Equal(t, "E = mc^2", content.Blocks[0].Text)
	assert.Equal(t, 1, content.Blocks[0].PageNo)

	// Not placed, so it only survives at all because the second field has
	// something in it.
	assert.Equal(t, "Revenue", content.Blocks[1].Text)

	// When both are filled the first wins, because that is the one the
	// parser cleaned up.
	assert.Equal(t, "cleaned", content.Blocks[2].Text)
}

// TestExtractPageNumberFallsBackToTheKey covers a rendition that leaves
// page_no out of the page object. The number is also the key the page is
// filed under, so reading that keeps the pages numbered and ordered instead
// of making every one of them page 0 in whatever order the map ranged.
func TestExtractPageNumberFallsBackToTheKey(t *testing.T) {
	const rendition = `{
      "pages": {
        "2": {"size": {"width": 595, "height": 842}},
        "1": {"size": {"width": 612, "height": 792}}
      }
    }`

	var content Content
	require.NoError(t, content.ExtractDoclingDocument(json.RawMessage(rendition)))

	assert.Equal(t, []Page{
		{PageNo: 1, Width: 612, Height: 792},
		{PageNo: 2, Width: 595, Height: 842},
	}, content.Pages)
}

// TestExtractReadsTheFormCollections checks the collections a form or an
// invoice produces. They carry a placement and no text, so leaving them
// unread would drop a placed region the stated rule says is kept.
func TestExtractReadsTheFormCollections(t *testing.T) {
	const rendition = `{
      "key_value_items": [
        {"self_ref": "#/key_value_items/0", "label": "key_value_region",
         "prov": [{"page_no": 2, "bbox": {"l": 10, "t": 60, "r": 90, "b": 20}}]}
      ],
      "form_items": [{"self_ref": "#/form_items/0", "label": "form"}]
    }`

	var content Content
	require.NoError(t, content.ExtractDoclingDocument(json.RawMessage(rendition)))

	require.Len(t, content.Blocks, 2)
	assert.Equal(t, "#/key_value_items/0", content.Blocks[0].ID)
	assert.Equal(t, 2, content.Blocks[0].PageNo)
	assert.Equal(t, "#/form_items/0", content.Blocks[1].ID)
}

// TestContentMarkdownIsUnchanged pins the artifact shape that already exists.
// Consumers outside this repository read content.markdown by literal path, so
// the new fields have to be optional siblings that are absent unless a
// structured document was stored.
func TestContentMarkdownIsUnchanged(t *testing.T) {
	encoded, err := json.Marshal(Content{Markdown: "# heading"})
	require.NoError(t, err)

	assert.JSONEq(t, `{"markdown":"# heading"}`, string(encoded))
}

// TestContentNewFieldsAreSiblingsOfMarkdown checks the same path survives once
// a structured document is present, and that the new fields sit beside it
// rather than moving it.
func TestContentNewFieldsAreSiblingsOfMarkdown(t *testing.T) {
	content := Content{Markdown: "# heading"}
	require.NoError(t, content.ExtractDoclingDocument(json.RawMessage(inlineSplitRendition)))

	encoded, err := json.Marshal(content)
	require.NoError(t, err)

	var decoded struct {
		Markdown        string          `json:"markdown"`
		Blocks          []Block         `json:"blocks"`
		Pages           []Page          `json:"pages"`
		Parser          *ParserInfo     `json:"parser"`
		DoclingDocument json.RawMessage `json:"doclingDocument"`
	}
	require.NoError(t, json.Unmarshal(encoded, &decoded))

	assert.Equal(t, "# heading", decoded.Markdown)
	assert.Len(t, decoded.Blocks, 3)
	assert.NotNil(t, decoded.Parser)
	assert.JSONEq(t, inlineSplitRendition, string(decoded.DoclingDocument))
}
