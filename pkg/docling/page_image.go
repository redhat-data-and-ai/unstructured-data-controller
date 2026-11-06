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
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// ErrNotAnImageDataURI reports a page image that is a reference rather than
// the image itself.
var ErrNotAnImageDataURI = errors.New("page image is not an inline data URI")

// PageImage is one rendered page lifted out of a document.
type PageImage struct {
	// PageNo is the page it renders, counted from one.
	PageNo int
	// MIMEType is what the service said it encoded, for example "image/png".
	MIMEType string
	// Data is the decoded image.
	Data []byte
}

// TakePageImages removes every page image from a document and returns them.
// Removal is wanted: the chunking stage posts this document back, so rasters
// left inside it are uploaded again with every chunk request.
//
// The returned document is equivalent JSON with each pages[].image set to
// null. A document with no page images returns no images and the input
// unchanged.
func TakePageImages(document json.RawMessage) ([]PageImage, json.RawMessage, error) {
	if len(document) == 0 {
		return nil, document, nil
	}

	var root map[string]json.RawMessage
	if err := json.Unmarshal(document, &root); err != nil {
		return nil, nil, fmt.Errorf("reading document: %w", err)
	}

	rawPages, ok := root["pages"]
	if !ok {
		return nil, document, nil
	}

	// Keyed by page number as a string, because JSON object keys always are.
	var pages map[string]map[string]json.RawMessage
	if err := json.Unmarshal(rawPages, &pages); err != nil {
		return nil, nil, fmt.Errorf("reading document pages: %w", err)
	}
	// Dropped before the images are decoded: it is the same rasters a second
	// time, and a long document is hundreds of megabytes of them. Every
	// return below either rewrites this key or discards root.
	root["pages"] = nil

	var images []PageImage
	for key, page := range pages {
		rawImage, ok := page["image"]
		if !ok || string(rawImage) == "null" {
			continue
		}

		var image struct {
			MIMEType string `json:"mimetype"`
			URI      string `json:"uri"`
		}
		if err := json.Unmarshal(rawImage, &image); err != nil {
			return nil, nil, fmt.Errorf("reading image on page %s: %w", key, err)
		}

		data, err := decodeDataURI(image.URI)
		if err != nil {
			return nil, nil, fmt.Errorf("page %s: %w", key, err)
		}

		pageNo, err := strconv.Atoi(key)
		if err != nil {
			return nil, nil, fmt.Errorf("page key %q is not a number: %w", key, err)
		}

		images = append(images, PageImage{PageNo: pageNo, MIMEType: image.MIMEType, Data: data})
		page["image"] = json.RawMessage("null")
	}

	if len(images) == 0 {
		return nil, document, nil
	}

	// Ascending, because a map gives no order and the caller writes files.
	sort.Slice(images, func(i, j int) bool { return images[i].PageNo < images[j].PageNo })

	strippedPages, err := json.Marshal(pages)
	if err != nil {
		return nil, nil, fmt.Errorf("rewriting document pages: %w", err)
	}
	root["pages"] = strippedPages

	stripped, err := json.Marshal(root)
	if err != nil {
		return nil, nil, fmt.Errorf("rewriting document: %w", err)
	}

	return images, stripped, nil
}

// artifactImageLink matches the markdown image link docling writes for each
// picture, whose target is one of its own generated artifact names. The
// optional "<stem>_artifacts/" directory is matched because docling's
// referenced mode writes the name either way, and a miss here is silent.
var artifactImageLink = regexp.MustCompile(
	`!\[[^\]]*\]\((?:data:image/[^)]*|(?:[^)\s/]*_artifacts/)?(?:image|page)_[0-9a-fA-F_]+\.[a-zA-Z]+)\)`)

// StripImageLinks replaces docling's generated image links with the comment
// its "placeholder" export mode writes.
//
// The markdown is chunked and embedded, so without this, turning page images
// on changes every chunk's text.
func StripImageLinks(markdown string) string {
	return artifactImageLink.ReplaceAllString(markdown, "<!-- image -->")
}

// decodeDataURI reads the bytes out of a "data:image/png;base64,..." URI.
func decodeDataURI(uri string) ([]byte, error) {
	const marker = ";base64,"

	if !strings.HasPrefix(uri, "data:") {
		return nil, fmt.Errorf("%w: %.40q", ErrNotAnImageDataURI, uri)
	}
	start := strings.Index(uri, marker)
	if start < 0 {
		return nil, fmt.Errorf("%w: not base64 encoded", ErrNotAnImageDataURI)
	}

	data, err := base64.StdEncoding.DecodeString(uri[start+len(marker):])
	if err != nil {
		return nil, fmt.Errorf("decoding page image: %w", err)
	}
	return data, nil
}
