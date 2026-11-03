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
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/go-cmp/cmp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

// ErrNotAConvertedFile reports JSON that unmarshalled without error but is not
// a converted file. Every field of ConvertedFile is a pointer, so unrelated
// JSON decodes into a value whose pointers are all nil rather than failing,
// and dereferencing it panics.
//
// Callers should skip such a file rather than treat it as a failure: it will
// never become chunkable, so retrying cannot help.
var ErrNotAConvertedFile = errors.New("not a converted file")

type DocumentConverter string

const (
	DocumentConverterDocling DocumentConverter = "docling"
)

// BlockClass names what kind of element a Block holds.
//
// The values are currently the parser's own labels, so for docling they are
// "text", "section_header", "table" and the rest of its vocabulary. Different
// parsers name the same things differently, so these will later be mapped onto
// one vocabulary of our own; the named type is here so that mapping is a change
// to the values and not to the shape of the artifact.
type BlockClass string

// Block is one semantic element of a document: a paragraph, a heading, a
// table, a caption. It is what a citation points at.
type Block struct {
	// ID is the element's address inside the document stored in
	// Content.DoclingDocument, for example "#/texts/59". It is the parser's
	// own self-reference copied verbatim, because nothing can resolve a
	// reference we invented ourselves. It is an address and not an index, and
	// it is only valid against that exact document: the same file converted
	// twice does not produce the same addresses.
	ID string `json:"id"`

	Class BlockClass `json:"class"`

	// Text is the element as the parser read it.
	//
	// It is empty for a table or a picture, whose words are not on the
	// element itself: a table's live in its cells and a picture's in its
	// annotations, both inside Content.DoclingDocument. It is also empty for
	// the wrapper elements described on ExtractDoclingDocument.
	Text string `json:"text"`

	// PageNo is 1-based, and 0 when the parser gave this element no placement:
	// either it could not place it, or the input format has no pages at all.
	PageNo int `json:"pageNo"`

	// BBox is the element's rectangle as [left, top, right, bottom], in the
	// units and the coordinate origin the parser used. Docling reports PDF
	// points, and its PDF backends measure from the bottom left while other
	// paths measure from the top left. Which one applies is recorded per
	// rectangle inside Content.DoclingDocument and not here, so these values
	// are not yet drawable and not yet comparable between documents.
	// Converting them to one convention is a separate piece of work.
	//
	// It is the zero rectangle when the parser reported no placement. It can
	// also be zero alongside a real PageNo, when the parser named a page but
	// gave no rectangle on it, so it is not a reliable test for "this block
	// has geometry".
	BBox [4]float64 `json:"bbox"`
}

// Page records a page's extent so a bounding box on that page can be turned
// into a position on a rendering of it at any resolution.
type Page struct {
	PageNo int `json:"pageNo"`

	// Width and Height are in PDF points, the same units as Block.BBox.
	Width  float64 `json:"width"`
	Height float64 `json:"height"`
}

// ParserInfo records what produced the blocks. Without it a silent change of
// parser on the conversion service alters output while every stored document
// still looks valid, and there is no way afterwards to tell which half of a
// corpus came from which.
type ParserInfo struct {
	Name string `json:"name"`

	// Version says what shape the output is in. For docling it is the
	// DoclingDocument schema version, which is the only version string the
	// convert response carries. It is not the docling release that ran.
	Version string `json:"version"`
}

type Content struct {
	// Markdown is what every current consumer reads. Everything below it is
	// additive and optional.
	Markdown string `json:"markdown"`

	Blocks []Block     `json:"blocks,omitempty"`
	Pages  []Page      `json:"pages,omitempty"`
	Parser *ParserInfo `json:"parser,omitempty"`

	// DoclingDocument is the parser's own output, carried through rather than
	// decoded into types of ours and built again, for two reasons. Block.ID
	// only resolves against this document, so dropping a field or reordering
	// an array would break the blocks above it. And it lets a later stage work
	// from the parsed document instead of converting the file a second time,
	// which is both slower and not guaranteed to give the same answer.
	//
	// Writing it out still runs it through the JSON encoder, which strips
	// insignificant whitespace and escapes <, > and & inside strings. That
	// leaves an equivalent document rather than the identical bytes.
	DoclingDocument json.RawMessage `json:"doclingDocument,omitempty"`
}

type ConvertedFileMetadata struct {
	RawFilePath       string                 `json:"rawFilePath"`
	FileIdentifier    string                 `json:"fileIdentifier"`
	DocumentConverter DocumentConverter      `json:"documentConverter"`
	DoclingConfig     v1alpha1.DoclingConfig `json:"doclingConfig"`
}

type ConvertedDocument struct {
	Metadata *ConvertedFileMetadata `json:"metadata"`
	Content  *Content               `json:"content"`
}

type ConvertedFile struct {
	ConvertedDocument *ConvertedDocument `json:"convertedDocument"`
}

// Validate reports whether the file carries the document, metadata and
// content that consumers dereference. It returns ErrNotAConvertedFile when any
// of them is missing.
func (c *ConvertedFile) Validate() error {
	switch {
	case c == nil, c.ConvertedDocument == nil:
		return fmt.Errorf("%w: no convertedDocument", ErrNotAConvertedFile)
	case c.ConvertedDocument.Metadata == nil:
		return fmt.Errorf("%w: no convertedDocument.metadata", ErrNotAConvertedFile)
	case c.ConvertedDocument.Content == nil:
		return fmt.Errorf("%w: no convertedDocument.content", ErrNotAConvertedFile)
	}

	return nil
}

func (c *ConvertedFileMetadata) Equal(other *ConvertedFileMetadata) bool {
	if c.RawFilePath != other.RawFilePath {
		return false
	}
	if c.FileIdentifier != other.FileIdentifier {
		return false
	}
	if c.DocumentConverter != other.DocumentConverter {
		return false
	}
	if !cmp.Equal(c.DoclingConfig, other.DoclingConfig) {
		return false
	}

	return true
}
