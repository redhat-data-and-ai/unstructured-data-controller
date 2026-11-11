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

// Tests use a 1000x1000 page, so a fraction reads as a pixel count.
func pagePNG(t *testing.T, width, height int) []byte {
	t.Helper()
	page := image.NewRGBA(image.Rect(0, 0, width, height))
	for x := range width {
		for y := range height {
			page.Set(x, y, color.White)
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

func TestCropRegion(t *testing.T) {
	cropped, err := CropRegion(pagePNG(t, 1000, 1000), [4]float64{0.2, 0.3, 0.6, 0.4})

	require.NoError(t, err)
	width, height := decodeSize(t, cropped)
	// Flush to the box: 0.4 of the width and 0.1 of the height, no padding.
	assert.InDelta(t, 400, width, 2)
	assert.InDelta(t, 100, height, 2)
}

// A box covering the whole page must crop to the whole page rather than
// overrun it, which would either panic or silently return a smaller image.
func TestCropRegion_ClampsAtTheEdges(t *testing.T) {
	cropped, err := CropRegion(pagePNG(t, 1000, 1000), [4]float64{0, 0, 1, 1})

	require.NoError(t, err)
	width, height := decodeSize(t, cropped)
	assert.Equal(t, 1000, width)
	assert.Equal(t, 1000, height)
}

// A box running off the page keeps the part that is on it.
func TestCropRegion_ClampsAnOverhangingBox(t *testing.T) {
	cropped, err := CropRegion(pagePNG(t, 1000, 1000), [4]float64{0.8, 0.8, 1.4, 1.4})

	require.NoError(t, err)
	width, height := decodeSize(t, cropped)
	assert.InDelta(t, 200, width, 2)
	assert.InDelta(t, 200, height, 2)
}

// image.Rect swaps an inverted rectangle rather than refusing it, so a box
// that arrives the wrong way round would crop the complement of the citation
// and look entirely plausible doing it.
func TestCropRegion_BoxTheWrongWayRound(t *testing.T) {
	page := pagePNG(t, 1000, 1000)

	fromUpright, err := CropRegion(page, [4]float64{0.2, 0.3, 0.6, 0.4})
	require.NoError(t, err)
	fromInverted, err := CropRegion(page, [4]float64{0.6, 0.4, 0.2, 0.3})
	require.NoError(t, err)

	assert.Equal(t, fromUpright, fromInverted, "the same region, however its edges arrive")
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
			// Nothing pads this out, so the box covers no pixels and is rejected
			// rather than encoded as a zero-sized PNG that renders as nothing.
			name:    "region rounds to no pixels",
			page:    pagePNG(t, 4, 4),
			bbox:    [4]float64{0.5, 0.5, 0.5, 0.5},
			wantErr: "is empty against a 4x4 page",
		},
		{
			name:    "region lies entirely off the page",
			page:    pagePNG(t, 1000, 1000),
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
