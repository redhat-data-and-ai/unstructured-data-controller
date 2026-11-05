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

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

// A 1000x1000 page, so a fraction is the pixel count with the decimal point
// moved and the arithmetic can be read at a glance.
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

func TestCropCitation(t *testing.T) {
	page := pagePNG(t, 1000, 1000)
	regions := []docling.Region{{PageNo: 1, BBox: [4]float64{0.2, 0.3, 0.6, 0.4}}}

	cropped, err := CropCitation(page, regions, 1)

	require.NoError(t, err)
	width, height := decodeSize(t, cropped)
	// 0.4 wide and 0.1 tall, plus the 0.02 margin on each side. Rounded
	// outward on both edges so nothing is clipped, which costs a pixel or
	// two to floating point.
	assert.InDelta(t, 440, width, 2)
	assert.InDelta(t, 140, height, 2)
}

// One chunk gives one image. Its blocks are cropped together, not one per
// block, because the chunk is the thing being cited.
func TestCropCitation_CoversEveryRegionOnThePage(t *testing.T) {
	page := pagePNG(t, 1000, 1000)
	regions := []docling.Region{
		{PageNo: 1, BBox: [4]float64{0.2, 0.3, 0.5, 0.35}},
		{PageNo: 1, BBox: [4]float64{0.3, 0.5, 0.7, 0.55}},
	}

	cropped, err := CropCitation(page, regions, 1)

	require.NoError(t, err)
	width, height := decodeSize(t, cropped)
	// Extent is 0.2 to 0.7 across and 0.3 to 0.55 down, plus margins.
	assert.InDelta(t, 540, width, 2)
	assert.InDelta(t, 290, height, 2)
}

// A chunk straddling a page break has regions on both, and each page is
// cropped from its own image.
func TestCropCitation_IgnoresOtherPages(t *testing.T) {
	page := pagePNG(t, 1000, 1000)
	// Deliberately different sizes, so each crop can only come from its own
	// page's region and not from the other.
	regions := []docling.Region{
		{PageNo: 1, BBox: [4]float64{0.2, 0.8, 0.6, 0.9}},
		{PageNo: 2, BBox: [4]float64{0.2, 0.1, 0.9, 0.5}},
	}

	first, err := CropCitation(page, regions, 1)
	require.NoError(t, err)
	second, err := CropCitation(page, regions, 2)
	require.NoError(t, err)

	firstWidth, firstHeight := decodeSize(t, first)
	secondWidth, secondHeight := decodeSize(t, second)
	assert.InDelta(t, 440, firstWidth, 2)
	assert.InDelta(t, 140, firstHeight, 2)
	assert.InDelta(t, 740, secondWidth, 2)
	assert.InDelta(t, 440, secondHeight, 2)
}

// The margin must not push the crop off the page, which would either panic
// or silently return a smaller image than asked for.
func TestCropCitation_ClampsAtTheEdges(t *testing.T) {
	page := pagePNG(t, 1000, 1000)
	regions := []docling.Region{{PageNo: 1, BBox: [4]float64{0, 0, 1, 1}}}

	cropped, err := CropCitation(page, regions, 1)

	require.NoError(t, err)
	width, height := decodeSize(t, cropped)
	assert.Equal(t, 1000, width)
	assert.Equal(t, 1000, height)
}

func TestCropCitation_NothingCitedOnThatPage(t *testing.T) {
	page := pagePNG(t, 1000, 1000)
	regions := []docling.Region{{PageNo: 2, BBox: [4]float64{0.2, 0.3, 0.6, 0.4}}}

	_, err := CropCitation(page, regions, 1)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoRegionsOnPage)
}

func TestCropCitation_NoRegionsAtAll(t *testing.T) {
	_, err := CropCitation(pagePNG(t, 100, 100), nil, 1)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNoRegionsOnPage)
}

func TestCropCitation_NotAnImage(t *testing.T) {
	regions := []docling.Region{{PageNo: 1, BBox: [4]float64{0.2, 0.3, 0.6, 0.4}}}

	_, err := CropCitation([]byte("not a png"), regions, 1)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "decoding page 1 image")
}

// A region can round to nothing on a small page. An empty crop encodes to a
// PNG that renders as nothing, which looks like a broken citation rather
// than a failed one.
func TestCropCitation_RejectsAnEmptyCrop(t *testing.T) {
	page := pagePNG(t, 4, 4)
	regions := []docling.Region{{PageNo: 1, BBox: [4]float64{0.5, 0.5, 0.5, 0.5}}}

	_, err := CropCitation(page, regions, 1)

	// The margin saves this one, so assert it produced something real
	// rather than that it failed.
	require.NoError(t, err)
	width, height := decodeSize(t, page)
	assert.Positive(t, width)
	assert.Positive(t, height)
}
