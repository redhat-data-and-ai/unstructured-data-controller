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

package tools

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

func TestCitedPages(t *testing.T) {
	pages, dropped := citedPages([]docling.Region{
		{PageNo: 3, BBox: [4]float64{0, 0, 1, 1}, Image: "stages/convert/f.pdf.page-3.png"},
		{PageNo: 1, BBox: [4]float64{0, 0, 1, 1}, Image: "stages/convert/f.pdf.page-1.png"},
		// Same page again, which is the common case: a chunk is several
		// blocks and most of them sit on one page.
		{PageNo: 1, BBox: [4]float64{0, 0, 1, 1}, Image: "stages/convert/f.pdf.page-1.png"},
	})

	require.Len(t, pages, 2)
	assert.Zero(t, dropped)
	assert.Equal(t, 1, pages[0].pageNo, "ascending, so the caller sees the first page first")
	assert.Equal(t, 3, pages[1].pageNo)
	assert.Equal(t, "stages/convert/f.pdf.page-1.png", pages[0].image)
}

// Most input formats render no pages, and the writer leaves Image empty for
// those. Returning a page for one would send the reader after a 404.
func TestCitedPages_SkipsRegionsWithNoImage(t *testing.T) {
	pages, _ := citedPages([]docling.Region{
		{PageNo: 1, Image: ""},
		{PageNo: 2, Image: "stages/convert/f.pdf.page-2.png"},
	})

	require.Len(t, pages, 1)
	assert.Equal(t, 2, pages[0].pageNo)
}

func TestCitedPages_NoRegions(t *testing.T) {
	pages, dropped := citedPages(nil)
	assert.Empty(t, pages)
	assert.Zero(t, dropped)

	pages, _ = citedPages([]docling.Region{{PageNo: 1, Image: ""}})
	assert.Empty(t, pages)
}

// A chunk covers a handful of pages. A larger number means the regions are
// wrong, and the cost of being wrong is one S3 read and one crop per page.
// The count comes back so the caller can say so rather than present a
// truncated citation as a whole one.
func TestCitedPages_BoundsTheNumberOfImages(t *testing.T) {
	regions := make([]docling.Region, 0, maxCitationPages*2)
	for pageNo := 1; pageNo <= maxCitationPages*2; pageNo++ {
		regions = append(regions, docling.Region{PageNo: pageNo, Image: "stages/convert/f.pdf.png"})
	}

	pages, dropped := citedPages(regions)

	assert.Len(t, pages, maxCitationPages)
	assert.Equal(t, maxCitationPages, dropped)
	assert.Equal(t, 1, pages[0].pageNo, "kept from the start, so the bound drops the tail")
}

func TestDecodeRegions(t *testing.T) {
	raw := json.RawMessage(`[{"pageNo":2,"bbox":[0.1,0.2,0.3,0.4],"image":"stages/convert/f.pdf.page-2.png"}]`)

	regions, err := decodeRegions(raw)

	require.NoError(t, err)
	require.Len(t, regions, 1)
	assert.Equal(t, 2, regions[0].PageNo)
	assert.Equal(t, "stages/convert/f.pdf.page-2.png", regions[0].Image)
	assert.InDelta(t, 0.1, regions[0].BBox[0], 1e-9)
}

// A chunk embedded before citations existed has no regions column at all.
func TestDecodeRegions_Absent(t *testing.T) {
	regions, err := decodeRegions(nil)

	require.NoError(t, err)
	assert.Empty(t, regions)
}

func TestDecodeRegions_Malformed(t *testing.T) {
	_, err := decodeRegions(json.RawMessage(`{"not":"an array"}`))

	require.Error(t, err)
	assert.Contains(t, err.Error(), "decoding regions")
}

func TestPageList(t *testing.T) {
	assert.Equal(t, "1, 4", pageList([]citedPage{{pageNo: 1}, {pageNo: 4}}))
}

// The key comes out of a warehouse column and is joined onto the pipeline's
// own prefix, so one that walks upwards reads another product's data out of
// the same bucket. Refused before any request is made, so a nil s3Client is
// enough to prove it never got that far.
func TestFetchRefusesAKeyThatIsNotAPlainRelativePath(t *testing.T) {
	destination := &destinationClient{bucket: "shared", prefix: "my-product/destination"}

	for _, key := range []string{
		"",
		"../other-product/destination/stages/convert/secret.pdf.page-1.png",
		"stages/convert/../../../other-product/secret.pdf.page-1.png",
		"/etc/passwd",
		"./stages/convert/f.pdf.page-1.png",
	} {
		_, err := destination.fetch(t.Context(), key)

		require.Error(t, err, "key %q", key)
		assert.Contains(t, err.Error(), "not a plain relative path")
	}
}
