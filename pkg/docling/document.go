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

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// A chunk says which elements it was built from and nothing about where they
// are. This file turns those addresses into rectangles on a page, by looking
// them up in the document the chunk came from.
//
// Below is a real DoclingDocument, abridged, so the shape these types
// describe is visible without fetching a conversion response.
//
//	{
//	  "schema_name": "DoclingDocument",
//	  "version": "1.10.0",
//	  "body":   { "self_ref": "#/body", "children": [ {"$ref": "#/texts/0"} ] },
//	  "groups": [ {"self_ref": "#/groups/0", "parent": {"$ref": "#/body"},
//	               "label": "list"} ],
//	  "texts": [
//	    {
//	      "self_ref": "#/texts/0",
//	      "parent": {"$ref": "#/body"},
//	      "label": "text",
//	      "prov": [
//	        {
//	          "page_no": 1,
//	          "bbox": {"l": 65.0, "t": 644.7, "r": 187.0, "b": 623.0,
//	                   "coord_origin": "BOTTOMLEFT"},
//	          "charspan": [0, 23]
//	        }
//	      ],
//	      "orig": "Quarterly Widget Report",
//	      "text": "Quarterly Widget Report"
//	    }
//	  ],
//	  "tables": [], "pictures": [], "key_value_items": [], "form_items": [],
//	  "pages": { "1": {"size": {"width": 612.0, "height": 792.0}, "page_no": 1} }
//	}
//
// Four things in there drive the code below.
//
// An element is addressed by "self_ref" and never by position, so the order
// of the collections carries no meaning.
//
// "pages" is an object keyed by the page number written as a string, not an
// array, because JSON object keys are always strings.
//
// "prov" is a list and is empty on an element docling did not place, which is
// most of them. Measured on a 143 page document: 1,137 of 2,454 text elements
// carry a placement of their own and the other 1,317 take one from an
// ancestor.
//
// "coord_origin" says which corner the rectangle is measured from, and has to
// be read per box rather than assumed. It is BOTTOMLEFT on the standard path
// and TOPLEFT on the vision-language one.

const (
	// CoordOriginBottomLeft measures a box from the bottom of the page
	// upwards, which is the PDF convention and what docling's own backends
	// produce.
	CoordOriginBottomLeft = "BOTTOMLEFT"

	// CoordOriginTopLeft measures it from the top downwards, which is what
	// the vision-language pipeline produces and what a renderer expects.
	CoordOriginTopLeft = "TOPLEFT"
)

// maxAncestorLevels bounds the climb from an element to a placed ancestor.
//
// Four because three is the deepest observed and the parent chain is data
// from outside this process. A document whose parents form a cycle would
// otherwise loop forever inside a reconcile.
const maxAncestorLevels = 4

// regionPrecision is how many decimal places a normalised coordinate keeps.
//
// Five is a six hundredth of a point on a letter page, far below anything a
// reader could see, and it stops a float division writing seventeen digits
// per number into an artifact that holds thousands of them. It also makes two
// boxes that are the same box compare equal, which is what deduplication
// relies on.
const regionPrecision = 5

// Region is where a chunk's content sits: one page, one rectangle.
//
// The rectangle is normalised to 0 to 1 and measured from the top left,
// whatever convention it arrived in. Normalised coordinates survive any
// rendering resolution, which a pixel box does not.
type Region struct {
	PageNo int `json:"pageNo"`

	// BBox is left, top, right, bottom, in that order.
	BBox [4]float64 `json:"bbox"`
}

// DocItem is what every element of a DoclingDocument has in common: an
// address, what it is, where it sits, and what contains it.
//
// Text elements are read through this type too. Their content is not needed
// here, because a chunk already carries its own text; only the geometry is
// missing.
type DocItem struct {
	// SelfRef is the element's address inside this document, for example
	// "#/texts/59". It is an address and not an index, and resolves only
	// against the document it came from.
	SelfRef string `json:"self_ref"`

	// Label is docling's own name for the element: "text", "section_header",
	// "list_item", "table" and so on.
	Label string `json:"label"`

	// Parent is nil on the root. Everything else names its container, which
	// is how an element with no placement of its own finds one.
	Parent *Reference `json:"parent"`

	// Prov is empty for elements docling did not place, and for input formats
	// that have no pages to place anything on.
	Prov []Provenance `json:"prov"`
}

// Reference is a pointer to another element.
//
// Two spellings exist. Current docling writes "$ref" and older documents
// carry "cref", and a stored artifact can have been written by either, so
// both are read.
type Reference struct {
	Ref  string `json:"$ref"`
	CRef string `json:"cref"`
}

// Address returns whichever spelling this reference used.
func (r *Reference) Address() string {
	if r == nil {
		return ""
	}
	if r.Ref != "" {
		return r.Ref
	}
	return r.CRef
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

	// CoordOrigin is BOTTOMLEFT or TOPLEFT. Read it rather than assume it:
	// docling's PDF backends construct bottom-left boxes even though the
	// type's own default is top-left.
	CoordOrigin string `json:"coord_origin"`
}

// Page is one page of the source document.
type Page struct {
	PageNo int      `json:"page_no"`
	Size   PageSize `json:"size"`
}

// PageSize is the page's extent in PDF points, which is what makes a bounding
// box on that page mean anything.
type PageSize struct {
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

// document is the subset of a DoclingDocument this package reads. Docling
// returns a considerably larger object; encoding/json ignores what is not
// listed, so docling adding to its schema does not break this.
type document struct {
	Texts         []DocItem `json:"texts"`
	Tables        []DocItem `json:"tables"`
	Pictures      []DocItem `json:"pictures"`
	KeyValueItems []DocItem `json:"key_value_items"`
	FormItems     []DocItem `json:"form_items"`

	// Groups are the tree's branch nodes and carry no placement of their
	// own, so nothing is ever found on one. They are still read, because the
	// climb from a text element to a placed ancestor passes through them:
	// measured on a 143 page document, 1,146 of 2,454 text elements take
	// their box from exactly two levels up, with a group in between.
	Groups []DocItem `json:"groups"`

	// Pages is keyed by the page number written as a string, because JSON
	// object keys are always strings.
	Pages map[string]Page `json:"pages"`
}

// DocumentIndex answers "where is this element" for one document.
//
// It is built once per file and queried once per chunk. Building it per chunk
// would reparse several megabytes of JSON for every one of a document's
// chunks.
type DocumentIndex struct {
	items map[string]*DocItem
	pages map[string]Page
}

// NewDocumentIndex reads a stored DoclingDocument so the elements a chunk
// names can be looked up in it.
func NewDocumentIndex(raw json.RawMessage) (*DocumentIndex, error) {
	var parsed document
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, fmt.Errorf("reading the stored document: %w", err)
	}

	index := &DocumentIndex{
		items: map[string]*DocItem{},
		pages: parsed.Pages,
	}
	for _, collection := range [][]DocItem{
		parsed.Texts, parsed.Tables, parsed.Pictures,
		parsed.KeyValueItems, parsed.FormItems, parsed.Groups,
	} {
		for i := range collection {
			item := &collection[i]
			if item.SelfRef == "" {
				continue
			}
			index.items[item.SelfRef] = item
		}
	}

	return index, nil
}

// Regions returns where the named elements sit, and the addresses it could
// not place.
//
// The two are returned separately rather than one failing the other. An
// address that does not resolve is worth seeing, because it means the chunk
// and the document disagree about what is in the document, but a chunk with
// some of its rectangles is still better than a chunk with none.
//
// Duplicates are dropped. A run of inline text resolves through its parent,
// so several addresses in one chunk commonly name the same box, and a reader
// would see the same rectangle drawn repeatedly.
func (index *DocumentIndex) Regions(addresses []string) (regions []Region, unresolved []string) {
	regions = []Region{}
	seen := map[Region]bool{}

	for _, address := range addresses {
		item, ok := index.items[address]
		if !ok {
			unresolved = append(unresolved, address)
			continue
		}

		placement := index.placement(item)
		if placement == nil {
			unresolved = append(unresolved, address)
			continue
		}

		region, ok := index.normalise(*placement)
		if !ok {
			unresolved = append(unresolved, address)
			continue
		}
		if seen[region] {
			continue
		}

		seen[region] = true
		regions = append(regions, region)
	}

	return regions, unresolved
}

// placement finds the element's own box, or the nearest one above it.
//
// Climbing is not a fallback for a rare case: more than half of a real
// document's text elements carry no placement, because the parser puts the
// box on the block and the text on the inline runs inside it. Reading only an
// element's own prov loses them.
//
// It climbs rather than reading the parent directly. An element that has its
// own box must keep it, or a sentence inside a paragraph would be drawn
// around the whole paragraph, and groups may gain placements of their own in
// a later docling release without this needing to change.
func (index *DocumentIndex) placement(item *DocItem) *Provenance {
	for range maxAncestorLevels + 1 {
		if own := item.ownPlacement(); own != nil {
			return own
		}

		parent, ok := index.items[item.Parent.Address()]
		if !ok {
			return nil
		}
		item = parent
	}

	return nil
}

// ownPlacement returns the element's first real box, if it has one.
//
// An entry with an empty rectangle is skipped rather than returned. bbox is a
// value and not a pointer, so an absent one decodes to four zeroes, and
// taking that as a placement would draw a zero-sized box in the page corner
// and stop the climb that would have found the real one.
func (item *DocItem) ownPlacement() *Provenance {
	for i := range item.Prov {
		box := item.Prov[i].BBox
		if box.Left == 0 && box.Top == 0 && box.Right == 0 && box.Bottom == 0 {
			continue
		}
		return &item.Prov[i]
	}
	return nil
}

// normalise converts one placement into a 0 to 1 top-left rectangle.
//
// It reports false for a page it cannot measure. Without the page's extent
// there is nothing to divide by, and emitting the raw points would put a box
// hundreds of times the size of the page into an artifact that says it is
// normalised.
func (index *DocumentIndex) normalise(placement Provenance) (Region, bool) {
	page, ok := index.pages[strconv.Itoa(placement.PageNo)]
	if !ok || page.Size.Width <= 0 || page.Size.Height <= 0 {
		return Region{}, false
	}

	box := placement.BBox
	top, bottom := box.Top, box.Bottom

	// Flip a bottom-left box onto a top-left page. Both edges are measured
	// from the opposite side, so both are subtracted from the height.
	if strings.EqualFold(box.CoordOrigin, CoordOriginBottomLeft) {
		top, bottom = page.Size.Height-box.Top, page.Size.Height-box.Bottom
	}

	// The flip puts the larger number first, and a top-left box is not
	// guaranteed to arrive the right way round either.
	if top > bottom {
		top, bottom = bottom, top
	}

	return Region{
		PageNo: placement.PageNo,
		BBox: [4]float64{
			round(box.Left / page.Size.Width),
			round(top / page.Size.Height),
			round(box.Right / page.Size.Width),
			round(bottom / page.Size.Height),
		},
	}, true
}

func round(value float64) float64 {
	scale := math.Pow(10, regionPrecision)
	return math.Round(value*scale) / scale
}
