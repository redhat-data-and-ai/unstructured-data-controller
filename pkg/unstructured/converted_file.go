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

type Content struct {
	// Markdown is what every current consumer reads. DoclingDocument is
	// additive and optional.
	Markdown string `json:"markdown"`

	// DoclingDocument is the conversion service's own output, kept whole.
	//
	// It is carried through rather than decoded into types of ours and built
	// again. A round trip through our own types would silently drop every
	// field they do not name, and an element's address inside the document,
	// for example "#/texts/59", indexes into its arrays, so losing or
	// reordering anything moves them.
	//
	// Nothing is derived from it at this stage, deliberately. The one reader
	// is the chunking stage, which posts it straight back to the conversion
	// service to chunk without parsing the file a second time, and so holds
	// the whole tree already. A flattened copy here would be duplicated text
	// in an artifact that is itself copied into the chunk and embedding
	// files.
	//
	// Writing it out runs it through the JSON encoder, which strips
	// insignificant whitespace and escapes <, > and & inside strings. That
	// leaves an equivalent document rather than the identical bytes, which
	// matters to nobody: it is read back as JSON.
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
