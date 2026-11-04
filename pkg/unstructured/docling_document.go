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
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

// This file reads docling's DoclingDocument rendition. Below is a real one,
// abridged, so the shape is visible without opening a conversion response.
//
// It is the output of converting a one page document reading "Quarterly
// Widget Report", a "Summary" heading and two lines of regional sales, with
// to_formats ["json"]. Everything shown is verbatim, including the OCR of the
// page dimensions. Only repetition is cut, marked with an ellipsis.
//
//	{
//	  "schema_name": "DoclingDocument",
//	  "version": "1.10.0",
//	  "name": "report",
//	  "origin": { ... },
//	  "furniture": { ... },          // running headers and footers
//	  "body": {
//	    "self_ref": "#/body",
//	    "parent": null,
//	    "children": [ {"$ref": "#/texts/0"}, {"$ref": "#/texts/1"}, ... ]
//	  },
//	  "groups": [],                  // branch nodes, no text and no prov
//	  "texts": [
//	    {
//	      "self_ref": "#/texts/0",
//	      "parent": {"$ref": "#/body"},
//	      "children": [],
//	      "content_layer": "body",
//	      "label": "text",
//	      "prov": [
//	        {
//	          "page_no": 1,
//	          "bbox": {
//	            "l": 65.0, "t": 644.6666666666666,
//	            "r": 187.0, "b": 623.0,
//	            "coord_origin": "BOTTOMLEFT"
//	          },
//	          "charspan": [0, 23]
//	        }
//	      ],
//	      "orig": "Quarterly Widget Report",
//	      "text": "Quarterly Widget Report",
//	      "formatting": null,
//	      "hyperlink": null
//	    },
//	    ...                          // 5 more
//	  ],
//	  "pictures": [],
//	  "tables": [],
//	  "key_value_items": [],
//	  "form_items": [],
//	  "pages": {
//	    "1": {
//	      "size": {"width": 1240.0, "height": 700.0},
//	      "image": null,
//	      "page_no": 1
//	    }
//	  }
//	}
//
// Four things in there drive the code below.
//
// An element is addressed by "self_ref" and never by position, so the order
// of the collections carries no meaning.
//
// "pages" is an object keyed by the page number as a string, not an array,
// because JSON object keys are always strings.
//
// "prov" is a list, and empty on an element docling could not place. Every
// element in this sample has one because the input was an image, so all six
// were laid out. On a richer document roughly half of "texts" arrives with
// none, and takes its placement from an ancestor instead.
//
// "orig" and "text" agree here and do not always. Several paths place an
// element, leave "text" empty, and put the content in "orig".
//
// "coord_origin" says which corner the rectangle is measured from. It is
// BOTTOMLEFT on this path and TOPLEFT on the VLM one, so the values are not
// comparable between documents until something converts them.

// ErrNoDoclingDocument means the conversion carried no structured document,
// so there was nothing to read. It is not a parse failure.
var ErrNoDoclingDocument = errors.New("the response carried no structured document")

// ExtractDoclingDocument fills in the structured half of Content from
// docling's DoclingDocument rendition, and keeps the rendition itself.
//
// Markdown is left alone. Everything this sets is additive, so a document
// converted before this existed, or one whose rendition could not be read,
// still has exactly the content it had before.
//
// # Which elements become blocks
//
// A text element is kept when it has text or a placement on a page, and
// dropped only when it has neither. Keeping the ones with a placement but no
// text of their own matters more than it looks, because more than one parser
// path produces exactly that. Such an element is placed, carries the whole
// block's rectangle, and is also the element docling's own chunker refers to,
// so dropping it throws away precisely what is needed to show a reader where
// a passage came from. On a 143 page test document it was the difference
// between 2,255 and 2,540 placed elements.
//
// Tables, pictures and the form-like collections are kept whatever happens.
// None of them holds text, so the test above would drop every one of them on
// an input format that has no pages.
//
// # Where the text of an element comes from
//
// From whichever of docling's two text fields is filled, which is what
// doclingText does. Several paths write a placed element whose text field is
// empty and whose content sits in the field beside it: docling does it for
// formulas, and the VLM parsers do it for titles, headings and list items
// whose content has inline markup, building the runs underneath and leaving
// the placed element with nothing. Reading only the first field would give
// those blocks an accurate rectangle drawn around content never stored.
//
// Two things a reader should not assume. Blocks cover more of the document
// than the markdown does, because docling classes running headers and footers
// as furniture and leaves them out of its markdown export while still listing
// them as elements. And a block that a parent element was split away from is
// not linked to it here: reconnecting a placed wrapper with the runs holding
// its text means walking the tree in the stored rendition.
//
// An error means there was no document to read, or it could not be read as
// one. The caller should carry on with markdown: nothing reads the structured
// fields yet, and failing a conversion that produced perfectly good markdown
// helps nobody.
func (c *Content) ExtractDoclingDocument(rawDoclingDocument json.RawMessage) error {
	// Unmarshalling the JSON literal null into a struct succeeds and leaves
	// every field zero, so without this the artifact would record a document
	// that merely happened to be empty, and store the four bytes "null" as
	// the rendition. omitempty does not drop those, because there are four
	// of them.
	if trimmed := bytes.TrimSpace(rawDoclingDocument); len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return ErrNoDoclingDocument
	}

	var doc docling.Document
	if err := json.Unmarshal(rawDoclingDocument, &doc); err != nil {
		return fmt.Errorf("reading the structured document: %w", err)
	}

	c.Blocks = doclingBlocks(&doc)
	c.Pages = doclingPages(&doc)
	c.Parser = &ParserInfo{
		Name:    string(DocumentConverterDocling),
		Version: doc.Version,
	}
	// Held rather than copied. A rendition runs to megabytes and the one
	// caller hands over a slice it has finished with, so a defensive copy
	// would double the peak memory per document in flight to guard against
	// a caller that does not exist.
	c.DoclingDocument = rawDoclingDocument

	return nil
}

// doclingBlocks flattens the document's elements into blocks. They come out
// grouped by kind, in the order docling listed them. Blocks are addressed by
// ID and never by position, so the order carries no meaning.
func doclingBlocks(doc *docling.Document) []Block {
	textless := [][]docling.DocItem{doc.Tables, doc.Pictures, doc.KeyValueItems, doc.FormItems}

	size := len(doc.Texts)
	for _, items := range textless {
		size += len(items)
	}
	blocks := make([]Block, 0, size)

	for i := range doc.Texts {
		item := &doc.Texts[i]
		text := doclingText(item)
		if text == "" && len(item.Prov) == 0 {
			continue
		}
		blocks = append(blocks, doclingBlock(&item.DocItem, text))
	}

	for _, items := range textless {
		for i := range items {
			blocks = append(blocks, doclingBlock(&items[i], ""))
		}
	}

	return blocks
}

// doclingText reads an element's text out of whichever of docling's two text
// fields holds it.
//
// They normally agree. The one that matters is when they do not: a parser can
// place an element, leave its text field empty and put the real content in
// the field beside it, which is how docling emits a formula and how the VLM
// parsers emit a title, heading or list item whose content has inline markup.
// Those elements have a correct rectangle, so reading only the first field
// would store a box around words that were never kept.
func doclingText(item *docling.TextItem) string {
	if item.Text != "" {
		return item.Text
	}

	return item.Orig
}

// doclingBlock copies one element across. The geometry is carried over as the
// parser reported it, in its own units and coordinate origin.
func doclingBlock(item *docling.DocItem, text string) Block {
	block := Block{
		ID:    item.SelfRef,
		Class: BlockClass(item.Label),
		Text:  text,
	}

	// An element can be placed more than once, which is how docling reports
	// something that straddles a page break. The first placement is taken and
	// the rest are left in the stored document, since one rectangle is all a
	// block has room for.
	if len(item.Prov) > 0 {
		prov := item.Prov[0]
		block.PageNo = prov.PageNo
		block.BBox = [4]float64{prov.BBox.Left, prov.BBox.Top, prov.BBox.Right, prov.BBox.Bottom}
	}

	return block
}

// doclingPages lists the pages and their dimensions. Input formats with no
// pages, such as markdown or CSV, produce none.
func doclingPages(doc *docling.Document) []Page {
	if len(doc.Pages) == 0 {
		return nil
	}

	pages := make([]Page, 0, len(doc.Pages))
	for key, page := range doc.Pages {
		pageNo := page.PageNo
		// The page number is written twice, inside the object and as the key
		// it is filed under. Reading the key when the inner field is missing
		// is what stops a rendition that leaves it out from numbering every
		// page 0, which would also make the sort below meaningless.
		if pageNo == 0 {
			if fromKey, err := strconv.Atoi(key); err == nil {
				pageNo = fromKey
			}
		}

		pages = append(pages, Page{
			PageNo: pageNo,
			Width:  page.Size.Width,
			Height: page.Size.Height,
		})
	}

	// Docling sends the pages as an object, so ranging over them is in random
	// order. Sorting keeps two conversions of the same document from
	// producing artifacts that differ only by shuffling. Pages that cannot be
	// told apart by number fall through to their dimensions, and two that
	// match on all three are interchangeable anyway.
	sort.Slice(pages, func(i, j int) bool {
		switch {
		case pages[i].PageNo != pages[j].PageNo:
			return pages[i].PageNo < pages[j].PageNo
		case pages[i].Width != pages[j].Width:
			return pages[i].Width < pages[j].Width
		default:
			return pages[i].Height < pages[j].Height
		}
	})

	return pages
}
