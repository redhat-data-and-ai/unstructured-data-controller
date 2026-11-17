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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func dataURI(payload string) string {
	return "data:image/png;base64," + base64.StdEncoding.EncodeToString([]byte(payload))
}

func documentWithPages(pages string) json.RawMessage {
	return json.RawMessage(`{"schema_name":"DoclingDocument","texts":[{"self_ref":"#/texts/0"}],` +
		`"pages":` + pages + `}`)
}

func TestTakePageImages(t *testing.T) {
	document := documentWithPages(`{
	  "1": {"page_no":1,"size":{"width":612,"height":792},
	        "image":{"mimetype":"image/png","dpi":144,"uri":"` + dataURI("page one") + `"}},
	  "2": {"page_no":2,"size":{"width":612,"height":792},
	        "image":{"mimetype":"image/png","dpi":144,"uri":"` + dataURI("page two") + `"}}
	}`)

	images, stripped, err := TakePageImages(document)

	require.NoError(t, err)
	require.Len(t, images, 2)
	assert.Equal(t, 1, images[0].PageNo, "ascending, because a map has no order")
	assert.Equal(t, "page one", string(images[0].Data))
	assert.Equal(t, "image/png", images[0].MIMEType)
	assert.Equal(t, 2, images[1].PageNo)
	assert.Equal(t, "page two", string(images[1].Data))

	var remaining struct {
		Pages map[string]struct {
			Image any `json:"image"`
		} `json:"pages"`
	}
	require.NoError(t, json.Unmarshal(stripped, &remaining))
	assert.Nil(t, remaining.Pages["1"].Image)
	assert.Nil(t, remaining.Pages["2"].Image)
	assert.NotContains(t, string(stripped), "base64")
}

// Everything else about the document has to survive, because a chunk names
// its elements by their position in these arrays.
func TestTakePageImages_KeepsTheRestOfTheDocument(t *testing.T) {
	document := documentWithPages(`{"1":{"page_no":1,"size":{"width":612,"height":792},
	  "image":{"mimetype":"image/png","uri":"` + dataURI("x") + `"}}}`)

	_, stripped, err := TakePageImages(document)
	require.NoError(t, err)

	var remaining struct {
		SchemaName string `json:"schema_name"`
		Texts      []any  `json:"texts"`
		Pages      map[string]struct {
			PageNo int `json:"page_no"`
			Size   any `json:"size"`
		} `json:"pages"`
	}
	require.NoError(t, json.Unmarshal(stripped, &remaining))
	assert.Equal(t, "DoclingDocument", remaining.SchemaName)
	assert.Len(t, remaining.Texts, 1)
	assert.Equal(t, 1, remaining.Pages["1"].PageNo)
	assert.NotNil(t, remaining.Pages["1"].Size, "page size is what normalises a region")
}

func TestTakePageImages_NothingToTake(t *testing.T) {
	tests := []struct {
		name     string
		document json.RawMessage
	}{
		{name: "no document at all", document: nil},
		{name: "no pages key", document: json.RawMessage(`{"texts":[]}`)},
		{name: "no pages", document: documentWithPages(`{}`)},
		{name: "a page with a null image", document: documentWithPages(`{"1":{"page_no":1,"image":null}}`)},
		{name: "a page with no image key", document: documentWithPages(`{"1":{"page_no":1}}`)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			images, stripped, err := TakePageImages(test.document)

			require.NoError(t, err)
			assert.Empty(t, images)
			assert.Equal(t, string(test.document), string(stripped),
				"with nothing to take the document is returned untouched")
		})
	}
}

// A path instead of inline bytes would silently become a dangling reference,
// so it has to fail loudly.
func TestTakePageImages_RejectsAReferenceInsteadOfAnImage(t *testing.T) {
	document := documentWithPages(`{"1":{"page_no":1,
	  "image":{"mimetype":"image/png","uri":"artifacts/page_000001_abc.png"}}}`)

	_, _, err := TakePageImages(document)

	require.Error(t, err)
	assert.ErrorIs(t, err, ErrNotAnImageDataURI)
}

// Every export mode but "placeholder" writes an image link into the markdown.
func TestStripImageLinks(t *testing.T) {
	tests := []struct {
		name     string
		markdown string
		want     string
	}{
		{
			name:     "a referenced artifact",
			markdown: "before\n\n![Image](image_000000_9980e8ff.png)\n\nafter",
			want:     "before\n\n<!-- image -->\n\nafter",
		},
		{
			name:     "a page artifact",
			markdown: "![Image](page_000001_abc123.png)",
			want:     "<!-- image -->",
		},
		{
			name:     "an embedded data uri",
			markdown: "text ![Image](data:image/png;base64,iVBORw0KGgo=) more",
			want:     "text <!-- image --> more",
		},
		{
			// Referenced mode writes the artifact under a per-document
			// directory, and a miss here is silent: the name reaches the
			// embedding model instead.
			name:     "an artifact under its document's directory",
			markdown: "![Image](report_artifacts/image_000000_9980e8ff.png)",
			want:     "<!-- image -->",
		},
		{
			name:     "uppercase hex and extension",
			markdown: "![Image](image_000000_9980E8FF.PNG)",
			want:     "<!-- image -->",
		},
		{
			name:     "several in one document",
			markdown: "![Image](image_000000_a.png) and ![Image](image_000001_b.png)",
			want:     "<!-- image --> and <!-- image -->",
		},
		{
			name:     "already the placeholder form",
			markdown: "before\n\n<!-- image -->\n\nafter",
			want:     "before\n\n<!-- image -->\n\nafter",
		},
		{
			name:     "no images at all",
			markdown: "# Heading\n\nsome text",
			want:     "# Heading\n\nsome text",
		},
		{
			// Only docling's own generated names match, so a link the
			// author wrote survives.
			name:     "a link the document itself contains",
			markdown: "see ![Diagram](https://example.invalid/diagram.png)",
			want:     "see ![Diagram](https://example.invalid/diagram.png)",
		},
		{
			name:     "an ordinary markdown link",
			markdown: "[the policy](https://example.invalid/policy)",
			want:     "[the policy](https://example.invalid/policy)",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, StripImageLinks(test.markdown))
		})
	}
}

func TestTakePageImages_RejectsMalformedInput(t *testing.T) {
	tests := []struct {
		name     string
		document json.RawMessage
	}{
		{name: "not a document", document: json.RawMessage(`not json`)},
		{name: "pages is not an object", document: json.RawMessage(`{"pages":[]}`)},
		{
			name: "a page key that is not a number",
			document: documentWithPages(`{"cover":{"page_no":1,
			  "image":{"mimetype":"image/png","uri":"` + dataURI("x") + `"}}}`),
		},
		{
			name: "base64 that does not decode",
			document: documentWithPages(`{"1":{"page_no":1,
			  "image":{"mimetype":"image/png","uri":"data:image/png;base64,!!!not base64!!!"}}}`),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := TakePageImages(test.document)
			require.Error(t, err)
		})
	}
}
