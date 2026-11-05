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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// A page 1000 points square, so a normalised coordinate is the raw value with
// the decimal point moved and the arithmetic can be checked by eye.
const squarePage = `"pages":{"1":{"page_no":1,"size":{"width":1000.0,"height":1000.0}}}`

func topLeftDocument() json.RawMessage {
	return json.RawMessage(`{
	  "texts":[
	    {"self_ref":"#/texts/0","label":"text","parent":{"$ref":"#/body"},
	     "prov":[{"page_no":1,"bbox":{"l":100,"t":200,"r":500,"b":300,
	              "coord_origin":"TOPLEFT"}}]}
	  ],
	  "groups":[],` + squarePage + `}`)
}

func TestDocumentIndexRegions_UsesAnElementsOwnBox(t *testing.T) {
	index, err := NewDocumentIndex(topLeftDocument())
	require.NoError(t, err)

	regions, unresolved := index.Regions([]string{"#/texts/0"})

	assert.Empty(t, unresolved)
	assert.Equal(t, []Region{{PageNo: 1, BBox: [4]float64{0.1, 0.2, 0.5, 0.3}}}, regions)
}

// The bottom-left convention is what docling's own PDF backends produce, so
// this is the common case rather than the exotic one. Both edges are measured
// from the opposite side of the page, and a box read without the flip appears
// upside down near the wrong end of the page.
func TestDocumentIndexRegions_FlipsABottomLeftBox(t *testing.T) {
	raw := json.RawMessage(`{
	  "texts":[
	    {"self_ref":"#/texts/0","label":"text",
	     "prov":[{"page_no":1,"bbox":{"l":100,"t":800,"r":500,"b":700,
	              "coord_origin":"BOTTOMLEFT"}}]}
	  ],` + squarePage + `}`)

	index, err := NewDocumentIndex(raw)
	require.NoError(t, err)

	regions, unresolved := index.Regions([]string{"#/texts/0"})

	assert.Empty(t, unresolved)
	// 800 points up from the bottom is 200 down from the top, and 700 up is
	// 300 down, so the box keeps its height and moves to the top of the page.
	assert.Equal(t, []Region{{PageNo: 1, BBox: [4]float64{0.1, 0.2, 0.5, 0.3}}}, regions)
}

// The two conventions have to produce the same rectangle for the same place
// on the page, or a corpus converted by both parsers cites inconsistently.
func TestDocumentIndexRegions_BothOriginsAgreeOnTheSamePlace(t *testing.T) {
	bottomLeft, err := NewDocumentIndex(json.RawMessage(`{
	  "texts":[{"self_ref":"#/texts/0","prov":[{"page_no":1,
	    "bbox":{"l":100,"t":800,"r":500,"b":700,"coord_origin":"BOTTOMLEFT"}}]}],` +
		squarePage + `}`))
	require.NoError(t, err)

	topLeft, err := NewDocumentIndex(topLeftDocument())
	require.NoError(t, err)

	fromBottom, _ := bottomLeft.Regions([]string{"#/texts/0"})
	fromTop, _ := topLeft.Regions([]string{"#/texts/0"})

	assert.Equal(t, fromTop, fromBottom)
}

// A box arriving with its edges the other way round still has to come out as
// a rectangle, because top is subtracted from the page height and ends up
// larger than bottom whenever the source measured upwards.
func TestDocumentIndexRegions_OrdersTheVerticalEdges(t *testing.T) {
	raw := json.RawMessage(`{
	  "texts":[
	    {"self_ref":"#/texts/0",
	     "prov":[{"page_no":1,"bbox":{"l":100,"t":300,"r":500,"b":200,
	              "coord_origin":"TOPLEFT"}}]}
	  ],` + squarePage + `}`)

	index, err := NewDocumentIndex(raw)
	require.NoError(t, err)

	regions, _ := index.Regions([]string{"#/texts/0"})

	require.Len(t, regions, 1)
	assert.Less(t, regions[0].BBox[1], regions[0].BBox[3], "top should come before bottom")
	assert.Equal(t, [4]float64{0.1, 0.2, 0.5, 0.3}, regions[0].BBox)
}

// Most of a real document's text elements have no placement of their own: the
// parser puts the box on the block and the text on the inline runs inside it.
// Reading only an element's own prov places almost nothing.
func TestDocumentIndexRegions_ClimbsToAPlacedAncestor(t *testing.T) {
	raw := json.RawMessage(`{
	  "texts":[
	    {"self_ref":"#/texts/0","label":"text","parent":{"$ref":"#/body"},
	     "prov":[{"page_no":1,"bbox":{"l":100,"t":200,"r":500,"b":300,
	              "coord_origin":"TOPLEFT"}}]},
	    {"self_ref":"#/texts/1","label":"text","parent":{"$ref":"#/groups/0"},
	     "prov":[]}
	  ],
	  "groups":[
	    {"self_ref":"#/groups/0","label":"inline","parent":{"$ref":"#/texts/0"}}
	  ],` + squarePage + `}`)

	index, err := NewDocumentIndex(raw)
	require.NoError(t, err)

	regions, unresolved := index.Regions([]string{"#/texts/1"})

	// Two levels up, through a group that carries no placement of its own,
	// which is where the majority of a real document's boxes are found.
	assert.Empty(t, unresolved)
	assert.Equal(t, []Region{{PageNo: 1, BBox: [4]float64{0.1, 0.2, 0.5, 0.3}}}, regions)
}

// An element with its own box keeps it. Climbing unconditionally would draw
// every sentence of a paragraph around the whole paragraph.
func TestDocumentIndexRegions_PrefersAnElementsOwnBoxOverItsParents(t *testing.T) {
	raw := json.RawMessage(`{
	  "texts":[
	    {"self_ref":"#/texts/0","parent":{"$ref":"#/body"},
	     "prov":[{"page_no":1,"bbox":{"l":0,"t":0,"r":1000,"b":1000,
	              "coord_origin":"TOPLEFT"}}]},
	    {"self_ref":"#/texts/1","parent":{"$ref":"#/texts/0"},
	     "prov":[{"page_no":1,"bbox":{"l":100,"t":200,"r":500,"b":300,
	              "coord_origin":"TOPLEFT"}}]}
	  ],` + squarePage + `}`)

	index, err := NewDocumentIndex(raw)
	require.NoError(t, err)

	regions, _ := index.Regions([]string{"#/texts/1"})

	assert.Equal(t, []Region{{PageNo: 1, BBox: [4]float64{0.1, 0.2, 0.5, 0.3}}}, regions)
}

// Inline runs in one chunk resolve through the same parent, so without this a
// reader sees the same rectangle drawn once per run.
func TestDocumentIndexRegions_DropsDuplicates(t *testing.T) {
	raw := json.RawMessage(`{
	  "texts":[
	    {"self_ref":"#/texts/0",
	     "prov":[{"page_no":1,"bbox":{"l":100,"t":200,"r":500,"b":300,
	              "coord_origin":"TOPLEFT"}}]},
	    {"self_ref":"#/texts/1","parent":{"$ref":"#/texts/0"},"prov":[]},
	    {"self_ref":"#/texts/2","parent":{"$ref":"#/texts/0"},"prov":[]}
	  ],` + squarePage + `}`)

	index, err := NewDocumentIndex(raw)
	require.NoError(t, err)

	regions, unresolved := index.Regions([]string{"#/texts/0", "#/texts/1", "#/texts/2"})

	assert.Empty(t, unresolved)
	assert.Len(t, regions, 1, "three addresses, one place on the page")
}

func TestDocumentIndexRegions_KeepsDistinctBoxesInOrder(t *testing.T) {
	raw := json.RawMessage(`{
	  "texts":[
	    {"self_ref":"#/texts/0",
	     "prov":[{"page_no":1,"bbox":{"l":100,"t":200,"r":500,"b":300,
	              "coord_origin":"TOPLEFT"}}]},
	    {"self_ref":"#/texts/1",
	     "prov":[{"page_no":2,"bbox":{"l":100,"t":400,"r":500,"b":500,
	              "coord_origin":"TOPLEFT"}}]}
	  ],
	  "pages":{
	    "1":{"page_no":1,"size":{"width":1000.0,"height":1000.0}},
	    "2":{"page_no":2,"size":{"width":1000.0,"height":1000.0}}
	  }}`)

	index, err := NewDocumentIndex(raw)
	require.NoError(t, err)

	regions, _ := index.Regions([]string{"#/texts/1", "#/texts/0"})

	assert.Equal(t, []Region{
		{PageNo: 2, BBox: [4]float64{0.1, 0.4, 0.5, 0.5}},
		{PageNo: 1, BBox: [4]float64{0.1, 0.2, 0.5, 0.3}},
	}, regions, "regions follow the order the chunk named its elements in")
}

func TestDocumentIndexRegions_ReportsWhatItCannotPlace(t *testing.T) {
	tests := []struct {
		name     string
		document string
		address  string
	}{
		{
			name:     "the address is not in the document",
			document: `{"texts":[],` + squarePage + `}`,
			address:  "#/texts/9",
		},
		{
			name: "nothing above the element carries a box",
			document: `{"texts":[
			  {"self_ref":"#/texts/0","parent":{"$ref":"#/body"},"prov":[]}],` +
				squarePage + `}`,
			address: "#/texts/0",
		},
		{
			// Without the page's extent there is nothing to divide by, and
			// emitting raw points would store a box hundreds of times the
			// size of the page in a field that says it is normalised.
			name: "the page it names has no size",
			document: `{"texts":[
			  {"self_ref":"#/texts/0","prov":[{"page_no":1,
			    "bbox":{"l":100,"t":200,"r":500,"b":300,"coord_origin":"TOPLEFT"}}]}],
			  "pages":{"1":{"page_no":1,"size":{"width":0,"height":0}}}}`,
			address: "#/texts/0",
		},
		{
			name: "the box is on a page the document does not list",
			document: `{"texts":[
			  {"self_ref":"#/texts/0","prov":[{"page_no":7,
			    "bbox":{"l":100,"t":200,"r":500,"b":300,"coord_origin":"TOPLEFT"}}]}],` +
				squarePage + `}`,
			address: "#/texts/0",
		},
		{
			// An absent bbox decodes to four zeroes, because it is a value
			// and not a pointer. Taking that as a placement draws a
			// zero-sized box in the page corner.
			name: "the placement carries no rectangle",
			document: `{"texts":[
			  {"self_ref":"#/texts/0","prov":[{"page_no":1}]}],` + squarePage + `}`,
			address: "#/texts/0",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			index, err := NewDocumentIndex(json.RawMessage(test.document))
			require.NoError(t, err)

			regions, unresolved := index.Regions([]string{test.address})

			assert.Empty(t, regions)
			assert.Equal(t, []string{test.address}, unresolved,
				"an address that cannot be placed is reported, not silently dropped")
		})
	}
}

// One bad address must not cost the others. A chunk with some of its
// rectangles is more useful than a chunk with none.
func TestDocumentIndexRegions_AnUnresolvableAddressDoesNotLoseTheRest(t *testing.T) {
	index, err := NewDocumentIndex(topLeftDocument())
	require.NoError(t, err)

	regions, unresolved := index.Regions([]string{"#/texts/9", "#/texts/0"})

	assert.Len(t, regions, 1)
	assert.Equal(t, []string{"#/texts/9"}, unresolved)
}

// Older documents spell a reference "cref". A stored artifact can have been
// written by either version, and reading only one spelling silently loses
// every box that needed the climb.
func TestDocumentIndexRegions_ReadsTheOlderReferenceSpelling(t *testing.T) {
	raw := json.RawMessage(`{
	  "texts":[
	    {"self_ref":"#/texts/0",
	     "prov":[{"page_no":1,"bbox":{"l":100,"t":200,"r":500,"b":300,
	              "coord_origin":"TOPLEFT"}}]},
	    {"self_ref":"#/texts/1","parent":{"cref":"#/texts/0"},"prov":[]}
	  ],` + squarePage + `}`)

	index, err := NewDocumentIndex(raw)
	require.NoError(t, err)

	regions, unresolved := index.Regions([]string{"#/texts/1"})

	assert.Empty(t, unresolved)
	assert.Len(t, regions, 1)
}

// The parent chain is data from outside this process, so a document whose
// parents point at each other would otherwise loop forever inside a
// reconcile.
func TestDocumentIndexRegions_StopsOnACycleInTheParentChain(t *testing.T) {
	raw := json.RawMessage(`{
	  "texts":[
	    {"self_ref":"#/texts/0","parent":{"$ref":"#/texts/1"},"prov":[]},
	    {"self_ref":"#/texts/1","parent":{"$ref":"#/texts/0"},"prov":[]}
	  ],` + squarePage + `}`)

	index, err := NewDocumentIndex(raw)
	require.NoError(t, err)

	regions, unresolved := index.Regions([]string{"#/texts/0"})

	assert.Empty(t, regions)
	assert.Equal(t, []string{"#/texts/0"}, unresolved)
}

// Tables and pictures are separate collections and a chunk cites them by the
// same kind of address, so missing one leaves every table chunk with no box.
func TestDocumentIndexRegions_IndexesEveryCollection(t *testing.T) {
	raw := json.RawMessage(`{
	  "tables":[{"self_ref":"#/tables/0","label":"table",
	    "prov":[{"page_no":1,"bbox":{"l":100,"t":200,"r":500,"b":300,
	             "coord_origin":"TOPLEFT"}}]}],
	  "pictures":[{"self_ref":"#/pictures/0","label":"picture",
	    "prov":[{"page_no":1,"bbox":{"l":100,"t":400,"r":500,"b":500,
	             "coord_origin":"TOPLEFT"}}]}],
	  "key_value_items":[{"self_ref":"#/key_value_items/0",
	    "prov":[{"page_no":1,"bbox":{"l":100,"t":600,"r":500,"b":700,
	             "coord_origin":"TOPLEFT"}}]}],
	  "form_items":[{"self_ref":"#/form_items/0",
	    "prov":[{"page_no":1,"bbox":{"l":100,"t":800,"r":500,"b":900,
	             "coord_origin":"TOPLEFT"}}]}],` + squarePage + `}`)

	index, err := NewDocumentIndex(raw)
	require.NoError(t, err)

	regions, unresolved := index.Regions([]string{
		"#/tables/0", "#/pictures/0", "#/key_value_items/0", "#/form_items/0"})

	assert.Empty(t, unresolved)
	assert.Len(t, regions, 4)
}

func TestNewDocumentIndex_RejectsSomethingThatIsNotADocument(t *testing.T) {
	_, err := NewDocumentIndex(json.RawMessage(`not json`))
	require.Error(t, err)
	assert.Contains(t, err.Error(), "reading the stored document")
}

// An empty list of addresses is normal: a chunk can be built entirely from
// elements docling did not place.
func TestDocumentIndexRegions_NoAddressesIsNotAnError(t *testing.T) {
	index, err := NewDocumentIndex(topLeftDocument())
	require.NoError(t, err)

	regions, unresolved := index.Regions(nil)

	assert.Empty(t, regions)
	assert.Empty(t, unresolved)
	assert.NotNil(t, regions, "an empty artifact field should serialise as [] and not null")
}
