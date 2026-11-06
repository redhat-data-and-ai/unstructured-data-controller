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
	"errors"
	"fmt"
	"image"
	"image/png"
	"math"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

// cropMargin pads the crop on every side, as a fraction of the page.
//
// Without it the cited text sits flush against the edge and a reader cannot
// tell whether a sentence continues.
const cropMargin = 0.02

// ErrNoRegionsOnPage reports a request to crop a page nothing was cited on.
var ErrNoRegionsOnPage = errors.New("no regions on that page")

// CropCitation cuts the part of a page image that a chunk was built from.
//
// The crop is the extent of every region on that page plus a margin, so one
// chunk gives one image rather than one per block. Regions on other pages
// are ignored; the caller crops those from their own page images.
func CropCitation(pageImage []byte, regions []docling.Region, pageNo int) ([]byte, error) {
	bounds, ok := regionExtent(regions, pageNo)
	if !ok {
		return nil, fmt.Errorf("%w: page %d", ErrNoRegionsOnPage, pageNo)
	}

	decoded, err := png.Decode(bytes.NewReader(pageImage))
	if err != nil {
		return nil, fmt.Errorf("decoding page %d image: %w", pageNo, err)
	}

	cropped, err := cropFraction(decoded, bounds)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	if err := png.Encode(&out, cropped); err != nil {
		return nil, fmt.Errorf("encoding crop of page %d: %w", pageNo, err)
	}
	return out.Bytes(), nil
}

// regionExtent returns the box covering every region on one page, padded,
// and clamped back inside the page.
func regionExtent(regions []docling.Region, pageNo int) ([4]float64, bool) {
	extent := [4]float64{math.Inf(1), math.Inf(1), math.Inf(-1), math.Inf(-1)}
	found := false

	for _, region := range regions {
		if region.PageNo != pageNo {
			continue
		}
		found = true
		extent[0] = math.Min(extent[0], region.BBox[0])
		extent[1] = math.Min(extent[1], region.BBox[1])
		extent[2] = math.Max(extent[2], region.BBox[2])
		extent[3] = math.Max(extent[3], region.BBox[3])
	}
	if !found {
		return extent, false
	}

	return [4]float64{
		math.Max(0, extent[0]-cropMargin),
		math.Max(0, extent[1]-cropMargin),
		math.Min(1, extent[2]+cropMargin),
		math.Min(1, extent[3]+cropMargin),
	}, true
}

// cropFraction turns a 0 to 1 box into pixels and cuts it out.
func cropFraction(source image.Image, box [4]float64) (image.Image, error) {
	// SubImage is on the concrete types rather than on image.Image, and it
	// shares the backing pixels instead of copying them.
	sub, ok := source.(interface {
		SubImage(image.Rectangle) image.Image
	})
	if !ok {
		return nil, fmt.Errorf("page image of type %T cannot be cropped", source)
	}

	size := source.Bounds()
	rect := image.Rect(
		size.Min.X+int(box[0]*float64(size.Dx())),
		size.Min.Y+int(box[1]*float64(size.Dy())),
		size.Min.X+int(math.Ceil(box[2]*float64(size.Dx()))),
		size.Min.Y+int(math.Ceil(box[3]*float64(size.Dy()))),
	).Intersect(size)

	// A region can round to nothing on a small page, and an empty crop
	// encodes to a zero-sized PNG that renders as nothing at all.
	if rect.Dx() < 1 || rect.Dy() < 1 {
		return nil, fmt.Errorf("crop of %v is empty against a %dx%d page",
			box, size.Dx(), size.Dy())
	}

	return sub.SubImage(rect), nil
}
