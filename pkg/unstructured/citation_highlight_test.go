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
	"context"
	"image"
	"image/color"
	"image/png"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

// textPagePNG returns a white page carrying one black block, standing in for
// a run of text at a known position.
func textPagePNG(t *testing.T, width, height int, glyph image.Rectangle) []byte {
	t.Helper()
	page := image.NewRGBA(image.Rect(0, 0, width, height))
	for x := range width {
		for y := range height {
			page.Set(x, y, color.White)
		}
	}
	for x := glyph.Min.X; x < glyph.Max.X; x++ {
		for y := glyph.Min.Y; y < glyph.Max.Y; y++ {
			page.Set(x, y, color.Black)
		}
	}
	var out bytes.Buffer
	require.NoError(t, png.Encode(&out, page))
	return out.Bytes()
}

func rgbAt(t *testing.T, raw []byte, x, y int) [3]uint8 {
	t.Helper()
	decoded, err := png.Decode(bytes.NewReader(raw))
	require.NoError(t, err)
	r, g, b, _ := decoded.At(x, y).RGBA()
	return [3]uint8{uint8(r >> 8), uint8(g >> 8), uint8(b >> 8)}
}

func TestHighlightCitation_WashesTheRegionAndSparesTheText(t *testing.T) {
	page := textPagePNG(t, 1000, 1000, image.Rect(480, 480, 520, 520))
	regions := []docling.Region{{PageNo: 1, BBox: [4]float64{0.3, 0.3, 0.7, 0.7}}}

	highlighted, err := HighlightCitation(t.Context(), page, regions, 1)

	require.NoError(t, err)
	// The wash darkens the glyph but must leave it far darker than the paper
	// around it, or the citation is unreadable.
	glyph := rgbAt(t, highlighted, 500, 500)
	paper := rgbAt(t, highlighted, 400, 400)
	assert.Less(t, int(glyph[0]), 128, "text under the wash must stay dark")
	assert.Greater(t, int(paper[0]), 200, "paper under the wash must stay light")
	assert.Equal(t, [3]uint8{255, 255, 255}, rgbAt(t, highlighted, 100, 100),
		"page outside the region must be untouched")
}

func TestHighlightCitation_CoversEveryRegionOnThePageAndIgnoresOthers(t *testing.T) {
	page := textPagePNG(t, 1000, 1000, image.Rect(0, 0, 0, 0))
	regions := []docling.Region{
		{PageNo: 1, BBox: [4]float64{0.1, 0.1, 0.2, 0.2}},
		{PageNo: 1, BBox: [4]float64{0.7, 0.7, 0.8, 0.8}},
		{PageNo: 2, BBox: [4]float64{0.4, 0.4, 0.5, 0.5}},
	}

	highlighted, err := HighlightCitation(t.Context(), page, regions, 1)

	require.NoError(t, err)
	assert.NotEqual(t, [3]uint8{255, 255, 255}, rgbAt(t, highlighted, 150, 150),
		"first region on the page must be washed")
	assert.NotEqual(t, [3]uint8{255, 255, 255}, rgbAt(t, highlighted, 750, 750),
		"second region on the page must be washed")
	assert.Equal(t, [3]uint8{255, 255, 255}, rgbAt(t, highlighted, 450, 450),
		"a region cited on another page must be left alone")
}

// A region that covers no whole pixel paints nothing. Returning the page
// unmarked would present a clean scan as a successful citation.
func TestHighlightCitation_RefusesRegionsThatPaintNothing(t *testing.T) {
	page := textPagePNG(t, 1000, 1000, image.Rect(0, 0, 0, 0))

	tests := []struct {
		name string
		bbox [4]float64
	}{
		{name: "wholly past the right edge", bbox: [4]float64{1.1, 1.1, 1.3, 1.3}},
		{name: "wholly before the left edge", bbox: [4]float64{-0.5, -0.5, -0.1, -0.1}},
		{name: "zero extent", bbox: [4]float64{0.5, 0.5, 0.5, 0.5}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			highlighted, err := HighlightCitation(t.Context(), page,
				[]docling.Region{{PageNo: 1, BBox: test.bbox}}, 1)

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrNoRegionsOnPage)
			assert.Nil(t, highlighted)
		})
	}
}

// Compositing is unbounded work on data read back out of the warehouse, so a
// page is only ever asked to draw maxRegionsPerPage of them.
func TestHighlightCitation_CapsRegionsPerPage(t *testing.T) {
	page := textPagePNG(t, 1000, 1000, image.Rect(0, 0, 0, 0))

	regions := make([]docling.Region, 0, maxRegionsPerPage+1)
	for range maxRegionsPerPage {
		regions = append(regions, docling.Region{PageNo: 1, BBox: [4]float64{0.1, 0.1, 0.2, 0.2}})
	}
	// One past the cap, somewhere nothing else touches.
	regions = append(regions, docling.Region{PageNo: 1, BBox: [4]float64{0.8, 0.8, 0.9, 0.9}})

	highlighted, err := HighlightCitation(t.Context(), page, regions, 1)

	require.NoError(t, err)
	assert.NotEqual(t, [3]uint8{255, 255, 255}, rgbAt(t, highlighted, 150, 150),
		"regions within the cap must be drawn")
	assert.Equal(t, [3]uint8{255, 255, 255}, rgbAt(t, highlighted, 850, 850),
		"the region past the cap must not be drawn")
}

func TestHighlightCitation_StopsWhenTheCallerGivesUp(t *testing.T) {
	page := textPagePNG(t, 1000, 1000, image.Rect(0, 0, 0, 0))
	regions := []docling.Region{{PageNo: 1, BBox: [4]float64{0.3, 0.3, 0.7, 0.7}}}

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	highlighted, err := HighlightCitation(ctx, page, regions, 1)

	require.Error(t, err)
	assert.ErrorIs(t, err, context.Canceled)
	assert.Nil(t, highlighted)
}

func TestHighlightCitation_Errors(t *testing.T) {
	tests := []struct {
		name    string
		page    []byte
		regions []docling.Region
		pageNo  int
		wantErr string
	}{
		{
			name:    "page image is not decodable",
			page:    []byte("this is not an image"),
			regions: []docling.Region{{PageNo: 1, BBox: [4]float64{0.1, 0.1, 0.2, 0.2}}},
			pageNo:  1,
			wantErr: "decoding page 1 image",
		},
		{
			name:    "no region cites the requested page",
			page:    textPagePNG(t, 100, 100, image.Rect(0, 0, 0, 0)),
			regions: []docling.Region{{PageNo: 2, BBox: [4]float64{0.1, 0.1, 0.2, 0.2}}},
			pageNo:  1,
			wantErr: "no regions on that page: page 1",
		},
		{
			name:    "no regions at all",
			page:    textPagePNG(t, 100, 100, image.Rect(0, 0, 0, 0)),
			regions: nil,
			pageNo:  1,
			wantErr: "no regions on that page: page 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			highlighted, err := HighlightCitation(t.Context(), test.page, test.regions, test.pageNo)

			require.Error(t, err)
			assert.Nil(t, highlighted)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}
}

// The highlight must cover the same pixels the crop would cut, or the two
// modes disagree about where a citation is.
func TestHighlightCitation_AgreesWithTheCropOnGeometry(t *testing.T) {
	const size = 1000
	bbox := [4]float64{0.1234, 0.2345, 0.5678, 0.6789}

	cropped, err := CropRegion(quadrantPNG(t, size, size), bbox)
	require.NoError(t, err)
	cropWidth, cropHeight := decodeSize(t, cropped)

	rect := pixelRect(image.Rect(0, 0, size, size), bbox)

	assert.Equal(t, cropWidth, rect.Dx(), "crop and highlight must agree on width")
	assert.Equal(t, cropHeight, rect.Dy(), "crop and highlight must agree on height")
}
