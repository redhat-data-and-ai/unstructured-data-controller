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
	"fmt"
	"image"

	// Registered so a page image is decoded by what it contains rather than
	// by what this assumed. The service renders png today and the stored
	// format is a cost decision that may change.
	_ "image/jpeg"
	"image/png"
	"math"
)

// CropRegion cuts a single bounding-box region out of a page image, flush to
// the box. Use this when one crop per region is needed rather than one merged
// crop per page.
func CropRegion(pageImage []byte, bbox [4]float64) ([]byte, error) {
	decoded, _, err := image.Decode(bytes.NewReader(pageImage))
	if err != nil {
		return nil, fmt.Errorf("decoding page image: %w", err)
	}

	bounds := [4]float64{
		math.Max(0, math.Min(bbox[0], bbox[2])),
		math.Max(0, math.Min(bbox[1], bbox[3])),
		math.Min(1, math.Max(bbox[0], bbox[2])),
		math.Min(1, math.Max(bbox[1], bbox[3])),
	}

	cropped, err := cropFraction(decoded, bounds)
	if err != nil {
		return nil, err
	}

	var out bytes.Buffer
	if err := png.Encode(&out, cropped); err != nil {
		return nil, fmt.Errorf("encoding region crop: %w", err)
	}
	return out.Bytes(), nil
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
