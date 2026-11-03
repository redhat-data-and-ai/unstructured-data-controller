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
	"fmt"
	"sort"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

// ExtractDoclingDocument fills in the structured half of Content from
// docling's DoclingDocument rendition, and keeps the rendition itself.
//
// Markdown is left alone. Everything this sets is additive, so a document
// converted before this existed, or one whose rendition could not be read,
// still has exactly the content it had before.
//
// # Which elements become blocks
//
// An element is kept when it has text or a placement on a page, and dropped
// only when it has neither. Keeping the ones with a placement but no text
// matters more than it looks: when a text block contains inline formatting,
// docling splits it into runs, puts the whole block's rectangle on an
// empty-string wrapper element, and leaves the runs that hold the actual text
// with no placement at all. So the element carrying the rectangle has no text,
// and on top of that it is the element docling's own chunker refers to.
// Keeping only elements that have text therefore throws away precisely the
// ones needed to show a reader where a passage came from. On a 143 page test
// document it was the difference between 2,255 and 2,540 placed elements.
//
// Tables and pictures are always kept. Neither holds text, so the test above
// would drop every one of them on an input format that has no pages.
//
// An error means the rendition could not be read as a document. The caller
// should carry on with markdown: nothing reads the structured fields yet, and
// failing a conversion that produced perfectly good markdown helps nobody.
func (c *Content) ExtractDoclingDocument(rendition json.RawMessage) error {
	var doc docling.Document
	if err := json.Unmarshal(rendition, &doc); err != nil {
		return fmt.Errorf("reading the structured document: %w", err)
	}

	c.Blocks = doclingBlocks(&doc)
	c.Pages = doclingPages(&doc)
	c.Parser = &ParserInfo{
		Name:    string(DocumentConverterDocling),
		Version: doc.Version,
	}
	c.DoclingDocument = rendition

	return nil
}

// doclingBlocks flattens the document's elements into blocks. They come out
// grouped by kind, in the order docling listed them. Blocks are addressed by
// ID and never by position, so the order carries no meaning.
func doclingBlocks(doc *docling.Document) []Block {
	blocks := make([]Block, 0, len(doc.Texts)+len(doc.Tables)+len(doc.Pictures))

	for i := range doc.Texts {
		text := &doc.Texts[i]
		if text.Text == "" && len(text.Prov) == 0 {
			continue
		}
		blocks = append(blocks, doclingBlock(&text.DocItem, text.Text))
	}

	for i := range doc.Tables {
		blocks = append(blocks, doclingBlock(&doc.Tables[i], ""))
	}

	for i := range doc.Pictures {
		blocks = append(blocks, doclingBlock(&doc.Pictures[i], ""))
	}

	return blocks
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
	for _, page := range doc.Pages {
		pages = append(pages, Page{
			PageNo: page.PageNo,
			Width:  page.Size.Width,
			Height: page.Size.Height,
		})
	}

	// Docling sends the pages as an object keyed by page number, so ranging
	// over them is in random order. Sorting keeps two conversions of the same
	// document from producing artifacts that differ only by shuffling.
	sort.Slice(pages, func(i, j int) bool { return pages[i].PageNo < pages[j].PageNo })

	return pages
}
