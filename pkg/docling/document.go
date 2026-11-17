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

// A chunk names the elements it was built from, not where they are. This file
// turns those addresses into rectangles on a page.

const (
	// CoordOriginBottomLeft measures a box from the bottom of the page
	// upwards, which is the PDF convention and what docling's own backends
	// produce.
	CoordOriginBottomLeft = "BOTTOMLEFT"

	// CoordOriginTopLeft measures it from the top downwards, which is what
	// the vision-language pipeline produces and what a renderer expects.
	CoordOriginTopLeft = "TOPLEFT"
)

// maxAncestorLevels bounds the climb to a placed ancestor. The parent chain is
// untrusted data, so a cycle in it would otherwise spin inside a reconcile.
const maxAncestorLevels = 4

// regionPrecision keeps five decimal places, which stops a float division
// writing seventeen digits per number and makes two identical boxes compare
// equal, which deduplication relies on.
const regionPrecision = 5

// Region is where a chunk's content sits: one page, one rectangle,
// normalised to 0 to 1 from the top left whatever convention it arrived in.
// Normalised coordinates survive any rendering resolution.
type Region struct {
	PageNo int `json:"pageNo"`

	// BBox is left, top, right, bottom, in that order.
	BBox [4]float64 `json:"bbox"`

	// Image locates the rendered page in the destination, relative to its
	// prefix, and is empty for inputs that render no pages. Carried rather
	// than derived so the reader never reconstructs the writer's naming.
	Image string `json:"image,omitempty"`
}

// DocItem is what every element of a DoclingDocument has in common: an
// address, what it is, where it sits, and what contains it. Text elements
// are read through it too, since only their geometry is wanted here.
type DocItem struct {
	// SelfRef is the element's address, for example "#/texts/59". It
	// resolves only against the document it came from.
	SelfRef string `json:"self_ref"`

	// Label is docling's own name for the element: "text", "section_header",
	// "list_item", "table" and so on.
	Label string `json:"label"`

	// Parent is nil on the root, and is how an unplaced element finds a
	// placement.
	Parent *Reference `json:"parent"`

	// Prov is empty for elements docling did not place, and for input formats
	// that have no pages to place anything on.
	Prov []Provenance `json:"prov"`
}

// Reference points at another element. Current docling writes "$ref" and
// older documents carry "cref", and a stored artifact can be either.
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

	// Groups are branch nodes and carry no placement of their own. They are
	// still read because the climb to a placed ancestor passes through them.
	Groups []DocItem `json:"groups"`

	// Pages is keyed by the page number written as a string, because JSON
	// object keys are always strings.
	Pages map[string]Page `json:"pages"`
}

// DocumentIndex answers "where is this element" for one document. Build it
// once per file: per chunk would reparse the whole JSON each time.
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

// Regions returns where the named elements sit, and separately the
// addresses it could not place: one bad address should not cost a chunk the
// rectangles it did resolve.
//
// Duplicates are dropped, because inline runs resolve through the same
// parent and a reader would see one rectangle drawn repeatedly.
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
// Climbing is the common case, not a fallback: the parser puts the box on the
// block and the text on the inline runs inside it.
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

// ownPlacement returns the element's first real box. An absent bbox decodes
// to four zeroes, because it is a value and not a pointer, and taking that
// as a placement would draw an empty box and stop the climb.
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

// normalise converts one placement into a 0 to 1 top-left rectangle, and
// reports false for a page whose extent is missing or zero, since there is
// then nothing to divide by.
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
