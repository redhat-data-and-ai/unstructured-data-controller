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

package docling

// Document is the subset of the DoclingDocument rendition this controller
// reads. Docling returns a considerably larger object than this; the whole of
// it is kept verbatim alongside, so only the fields needed to say what an
// element is and where it sits are modelled here.
//
// encoding/json ignores fields that are not listed, so docling adding to its
// schema does not break this type.
type Document struct {
	// Version is the DoclingDocument schema version, for example "1.7.0".
	Version string `json:"version"`

	// Texts holds paragraphs, headings, captions, list items, footnotes and
	// anything else whose content is a run of text.
	Texts []TextItem `json:"texts"`

	// Tables, Pictures, KeyValueItems and FormItems are separate collections
	// in the same document. None of them carries text of its own: a table's
	// words live in its cells, a picture's in its annotations, and the last
	// two hold the regions a form or an invoice produces. All four can carry
	// a placement.
	Tables        []DocItem `json:"tables"`
	Pictures      []DocItem `json:"pictures"`
	KeyValueItems []DocItem `json:"key_value_items"`
	FormItems     []DocItem `json:"form_items"`

	// Groups are deliberately absent. They are the tree's branch nodes and
	// carry neither text nor a placement, so reading them would add an entry
	// for every one and place none of them.

	// Pages is keyed by the page number written as a string, because the wire
	// format serialises the integer keys of a map as JSON object keys.
	Pages map[string]Page `json:"pages"`
}

// DocItem is what every element in the document has in common: an address, a
// label naming what it is, and zero or more placements on a page.
type DocItem struct {
	// SelfRef is the element's address inside this document, for example
	// "#/texts/59". It is an address rather than an index and only resolves
	// against the document it came from.
	SelfRef string `json:"self_ref"`

	// Label is docling's own name for the element: "text", "section_header",
	// "list_item", "table" and so on.
	Label string `json:"label"`

	// Prov is empty for elements docling could not place, and for input
	// formats that have no pages to place them on.
	Prov []Provenance `json:"prov"`
}

// TextItem is a DocItem that also carries text.
//
// There are two fields for it and they are usually the same. Several parser
// paths fill only Orig, so both have to be read. See doclingText in the
// unstructured package for which of them wins.
type TextItem struct {
	DocItem

	Text string `json:"text"`
	Orig string `json:"orig"`
}

// Provenance is one placement of an element: which page, and where on it.
type Provenance struct {
	PageNo int  `json:"page_no"`
	BBox   BBox `json:"bbox"`
}

// BBox is a rectangle in the parser's own coordinate space, in PDF points.
type BBox struct {
	Left   float64 `json:"l"`
	Top    float64 `json:"t"`
	Right  float64 `json:"r"`
	Bottom float64 `json:"b"`

	// CoordOrigin is "BOTTOMLEFT" or "TOPLEFT" and has to be read per box
	// rather than assumed: docling's PDF backends construct bottom-left boxes
	// even though the type's own default is top-left.
	CoordOrigin string `json:"coord_origin"`
}

// Page is one page of the source document.
type Page struct {
	PageNo int      `json:"page_no"`
	Size   PageSize `json:"size"`
}

// PageSize is a page's extent in PDF points, which is what makes a bounding
// box on that page meaningful.
type PageSize struct {
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}
