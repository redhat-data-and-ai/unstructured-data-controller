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
	"errors"
	"fmt"
	"image"
	"image/color"
	"image/draw"
	"image/png"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

// ErrNoRegionsOnPage reports a request to highlight a page nothing was cited on.
var ErrNoRegionsOnPage = errors.New("no regions on that page")

const (
	// highlightThickness is the border width in pixels.
	highlightThickness = 1

	// highlightHaloThickness is the width of the white rule drawn just outside
	// the border, which keeps the box visible where the page background is
	// itself close to the border colour.
	highlightHaloThickness = 1

	// highlightDash is the length in pixels of each dash, and of the gap after
	// it.
	highlightDash = 3

	// maxRegionsPerPage bounds the compositing one page can be asked to do.
	// The regions are caller data read back out of the warehouse, and a chunk
	// cites a handful of blocks, so a larger number means they are wrong.
	maxRegionsPerPage = 50
)

// A highlighter-yellow wash inside a dotted black outline. Both stay
// translucent so the text underneath survives: the fill sits at 25%, which
// reads unmistakably as yellow while leaving black text at 10.7:1 contrast
// against it, comfortably past the 7:1 bar. Yellow needs more alpha than a
// cooler colour to register, and costs more contrast per unit, because its
// red and green channels are at full strength and so lift dark pixels fast.
//
// These must be color.NRGBA, not color.RGBA. color.RGBA is alpha-premultiplied
// and image/draw does not clamp, so a literal whose R, G or B exceeds its A
// wraps the channel: as color.RGBA a translucent fill painted the paper
// magenta and turned the text under it solid blue.
var (
	highlightBorder = color.NRGBA{R: 0, G: 0, B: 0, A: 180}
	highlightFill   = color.NRGBA{R: 255, G: 242, B: 0, A: 64}
	highlightHalo   = color.NRGBA{R: 255, G: 255, B: 255, A: 200}
)

// HighlightCitation tints every cited region on the given page, outlines it,
// and returns the highlighted full-page PNG. The image is not cropped: the
// caller receives the entire page so the region is visible in context.
//
// Returns ErrNoRegionsOnPage if nothing was painted, so a page whose regions
// all collapse to no pixels is refused rather than returned unmarked. At most
// maxRegionsPerPage regions are drawn, and ctx is honoured between them.
func HighlightCitation(ctx context.Context, pageImage []byte,
	regions []docling.Region, pageNo int) ([]byte, error) {
	decoded, _, err := image.Decode(bytes.NewReader(pageImage))
	if err != nil {
		return nil, fmt.Errorf("decoding page %d image: %w", pageNo, err)
	}

	// Copy into a mutable RGBA canvas — the decoded type may be read-only.
	dst := image.NewRGBA(decoded.Bounds())
	draw.Draw(dst, decoded.Bounds(), decoded, image.Point{}, draw.Src)

	drew, drawn := false, 0
	for _, r := range regions {
		if r.PageNo != pageNo || drawn == maxRegionsPerPage {
			continue
		}
		// Compositing a region is unbounded work on caller data, so a
		// cancelled request stops here rather than running to completion.
		if ctxErr := ctx.Err(); ctxErr != nil {
			return nil, fmt.Errorf("annotating page %d: %w", pageNo, ctxErr)
		}
		if drawHighlight(dst, r.BBox) {
			drew = true
		}
		drawn++
	}
	if !drew {
		return nil, fmt.Errorf("%w: page %d", ErrNoRegionsOnPage, pageNo)
	}

	var out bytes.Buffer
	if err := png.Encode(&out, dst); err != nil {
		return nil, fmt.Errorf("encoding highlighted page %d: %w", pageNo, err)
	}
	return out.Bytes(), nil
}

// drawHighlight washes the region in yellow and outlines it, flush to the
// cited box, reporting whether it painted anything. The wash is composited
// with draw.Over at a low alpha, so the document text underneath shifts but
// stays legible: black lands on rgb(64,60,0) rather than being replaced.
func drawHighlight(dst *image.RGBA, box [4]float64) bool {
	r := pixelRect(dst.Bounds(), box)
	if r.Empty() {
		return false
	}

	draw.Draw(dst, r, image.NewUniform(highlightFill), image.Point{}, draw.Over)

	// Inset by a negative grows the rectangle, so this frame is the ring just
	// outside the border rather than overlapping it.
	drawFrame(dst, r.Inset(-highlightHaloThickness), highlightHaloThickness, highlightHalo)
	drawFrame(dst, r, highlightThickness, highlightBorder)
	return true
}

// drawFrame outlines r with a dotted border of the given thickness, drawn
// inside r and clipped to dst.
func drawFrame(dst *image.RGBA, r image.Rectangle, thickness int, c color.Color) {
	src := image.NewUniform(c)
	bounds := dst.Bounds()
	paint := func(dash image.Rectangle) {
		draw.Draw(dst, dash.Intersect(bounds), src, image.Point{}, draw.Over)
	}

	// Each run starts on a multiple of the period rather than on r, so the
	// halo and the border drawn inside it keep their dashes in step.
	const period = 2 * highlightDash
	for x := r.Min.X - r.Min.X%period; x < r.Max.X; x += period {
		from, to := max(x, r.Min.X), min(x+highlightDash, r.Max.X)
		if from >= to {
			continue
		}
		paint(image.Rect(from, r.Min.Y, to, r.Min.Y+thickness)) // top
		paint(image.Rect(from, r.Max.Y-thickness, to, r.Max.Y)) // bottom
	}
	for y := r.Min.Y - r.Min.Y%period; y < r.Max.Y; y += period {
		from, to := max(y, r.Min.Y), min(y+highlightDash, r.Max.Y)
		if from >= to {
			continue
		}
		paint(image.Rect(r.Min.X, from, r.Min.X+thickness, to)) // left
		paint(image.Rect(r.Max.X-thickness, from, r.Max.X, to)) // right
	}
}
