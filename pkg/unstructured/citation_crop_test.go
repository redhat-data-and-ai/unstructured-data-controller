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
	"image"
	"image/color"
	"image/png"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// The four quadrant colours. A uniform page would let a crop taken at the
// wrong offset pass every size assertion, so each quarter is distinct and
// crops are checked by what they contain.
var (
	topLeft     = color.RGBA{R: 255, A: 255}
	topRight    = color.RGBA{G: 255, A: 255}
	bottomLeft  = color.RGBA{B: 255, A: 255}
	bottomRight = color.RGBA{R: 255, G: 255, A: 255}
)

// quadrantPNG returns a page split into four solid colours.
func quadrantPNG(t *testing.T, width, height int) []byte {
	t.Helper()
	page := image.NewRGBA(image.Rect(0, 0, width, height))
	for x := range width {
		for y := range height {
			switch {
			case x < width/2 && y < height/2:
				page.Set(x, y, topLeft)
			case y < height/2:
				page.Set(x, y, topRight)
			case x < width/2:
				page.Set(x, y, bottomLeft)
			default:
				page.Set(x, y, bottomRight)
			}
		}
	}
	var out bytes.Buffer
	require.NoError(t, png.Encode(&out, page))
	return out.Bytes()
}

func decodeSize(t *testing.T, raw []byte) (int, int) {
	t.Helper()
	decoded, err := png.Decode(bytes.NewReader(raw))
	require.NoError(t, err)
	return decoded.Bounds().Dx(), decoded.Bounds().Dy()
}

// soleColor asserts the crop is one flat colour and returns it, which is what
// proves the crop came from the quadrant it was asked for.
func soleColor(t *testing.T, raw []byte) color.RGBA {
	t.Helper()
	decoded, err := png.Decode(bytes.NewReader(raw))
	require.NoError(t, err)

	at := func(x, y int) color.RGBA {
		r, g, b, a := decoded.At(x, y).RGBA()
		return color.RGBA{R: uint8(r >> 8), G: uint8(g >> 8), B: uint8(b >> 8), A: uint8(a >> 8)}
	}
	bounds := decoded.Bounds()
	first := at(bounds.Min.X, bounds.Min.Y)
	for y := bounds.Min.Y; y < bounds.Max.Y; y++ {
		for x := bounds.Min.X; x < bounds.Max.X; x++ {
			require.Equal(t, first, at(x, y), "crop is not one flat colour, at (%d,%d)", x, y)
		}
	}
	return first
}

func TestCropRegion_CutsTheRegionAsked(t *testing.T) {
	page := quadrantPNG(t, 1000, 1000)

	tests := []struct {
		name string
		bbox [4]float64
		want color.RGBA
	}{
		{name: "top left quadrant", bbox: [4]float64{0, 0, 0.5, 0.5}, want: topLeft},
		{name: "top right quadrant", bbox: [4]float64{0.5, 0, 1, 0.5}, want: topRight},
		{name: "bottom left quadrant", bbox: [4]float64{0, 0.5, 0.5, 1}, want: bottomLeft},
		{name: "bottom right quadrant", bbox: [4]float64{0.5, 0.5, 1, 1}, want: bottomRight},
		// Edges the wrong way round must still name the same quadrant, not
		// its complement.
		{name: "inverted top right", bbox: [4]float64{1, 0.5, 0.5, 0}, want: topRight},
		{name: "inverted bottom left", bbox: [4]float64{0.5, 1, 0, 0.5}, want: bottomLeft},
		// Clamped back onto the page, so still wholly inside one quadrant.
		{name: "overhanging bottom right", bbox: [4]float64{0.6, 0.6, 1.8, 1.8}, want: bottomRight},
		{name: "underhanging top left", bbox: [4]float64{-0.8, -0.8, 0.4, 0.4}, want: topLeft},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cropped, err := CropRegion(page, test.bbox)

			require.NoError(t, err)
			assert.Equal(t, test.want, soleColor(t, cropped))
		})
	}
}

func TestCropRegion_Size(t *testing.T) {
	cropped, err := CropRegion(quadrantPNG(t, 1000, 1000), [4]float64{0.2, 0.3, 0.6, 0.4})

	require.NoError(t, err)
	width, height := decodeSize(t, cropped)
	// Flush to the box: 0.4 of the width and 0.1 of the height, no padding.
	assert.InDelta(t, 400, width, 2)
	assert.InDelta(t, 100, height, 2)
}

// A box covering the whole page must crop to the whole page rather than
// overrun it, which would either panic or silently return a smaller image.
func TestCropRegion_ClampsAtTheEdges(t *testing.T) {
	cropped, err := CropRegion(quadrantPNG(t, 1000, 1000), [4]float64{0, 0, 1, 1})

	require.NoError(t, err)
	width, height := decodeSize(t, cropped)
	assert.Equal(t, 1000, width)
	assert.Equal(t, 1000, height)
}

// Rounding outward keeps a sliver of a region rather than losing it, which is
// what lets the crop and the highlight agree on sub-pixel boxes.
func TestCropRegion_KeepsASubPixelRegion(t *testing.T) {
	cropped, err := CropRegion(quadrantPNG(t, 1000, 1000), [4]float64{0.5001, 0.2, 0.5004, 0.3})

	require.NoError(t, err)
	width, _ := decodeSize(t, cropped)
	assert.Equal(t, 1, width)
}

func TestCropRegion_Errors(t *testing.T) {
	tests := []struct {
		name    string
		page    []byte
		bbox    [4]float64
		wantErr string
	}{
		{
			name:    "page image is not decodable",
			page:    []byte("not a png"),
			bbox:    [4]float64{0.2, 0.3, 0.6, 0.4},
			wantErr: "decoding page image",
		},
		{
			// Nothing pads this out, so the box covers no pixels and is
			// rejected rather than encoded as a zero-sized PNG.
			name:    "region rounds to no pixels",
			page:    quadrantPNG(t, 4, 4),
			bbox:    [4]float64{0.5, 0.5, 0.5, 0.5},
			wantErr: "is empty against a 4x4 page",
		},
		{
			name:    "region lies entirely off the page",
			page:    quadrantPNG(t, 1000, 1000),
			bbox:    [4]float64{1.5, 1.5, 2, 2},
			wantErr: "is empty against a 1000x1000 page",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cropped, err := CropRegion(test.page, test.bbox)

			require.Error(t, err)
			assert.Nil(t, cropped)
			assert.Contains(t, err.Error(), test.wantErr)
		})
	}
}

// pixelRect is the one place the crop and the highlight agree on which pixels
// a region covers, so its rounding and clamping are pinned here.
func TestPixelRect(t *testing.T) {
	page := image.Rect(0, 0, 1000, 1000)

	tests := []struct {
		name string
		box  [4]float64
		want image.Rectangle
	}{
		{name: "whole page", box: [4]float64{0, 0, 1, 1}, want: image.Rect(0, 0, 1000, 1000)},
		{name: "upright box", box: [4]float64{0.2, 0.3, 0.6, 0.4}, want: image.Rect(200, 300, 600, 400)},
		{name: "edges the wrong way round", box: [4]float64{0.6, 0.4, 0.2, 0.3}, want: image.Rect(200, 300, 600, 400)},
		{name: "clamped to the page", box: [4]float64{-0.5, -0.5, 0.4, 0.4}, want: image.Rect(0, 0, 400, 400)},
		{
			name: "rounds outward, never clipping",
			box:  [4]float64{0.2001, 0.3001, 0.5999, 0.4999},
			want: image.Rect(200, 300, 600, 500),
		},
		{
			name: "sub-pixel box keeps one pixel",
			box:  [4]float64{0.5001, 0.2, 0.5004, 0.3},
			want: image.Rect(500, 200, 501, 300),
		},
		// Intersect normalises anything empty to the zero rectangle, so these
		// two are reported as empty rather than as a degenerate box in place.
		{name: "zero extent is empty", box: [4]float64{0.5, 0.5, 0.5, 0.5}, want: image.Rectangle{}},
		{name: "wholly off the page is empty", box: [4]float64{1.5, 1.5, 2, 2}, want: image.Rectangle{}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := pixelRect(page, test.box)

			assert.Equal(t, test.want, got)
			assert.Equal(t, test.want.Empty(), got.Empty())
		})
	}
}
