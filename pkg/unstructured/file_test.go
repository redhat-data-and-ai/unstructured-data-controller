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
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPageImagePath(t *testing.T) {
	assert.Equal(t, "docs/report.pdf.page-1.png", PageImagePath("docs/report.pdf", 1))
	assert.Equal(t, "docs/report.pdf.page-143.png", PageImagePath("docs/report.pdf", 143))
}

// The whole point of the whitelist: whatever else the convert stage leaves
// in that directory, only its documents come back.
func TestConvertedFilePaths(t *testing.T) {
	converted := ConvertedFilePaths([]string{
		"docs/report.pdf.json",
		"docs/report.pdf.page-1.png",
		"docs/report.pdf.page-2.png",
		"docs/diagram.png",
		"docs/notes.txt",
	})

	assert.Equal(t, []string{"docs/report.pdf.json"}, converted)
}

func TestConvertedFilePaths_NothingToKeep(t *testing.T) {
	assert.Empty(t, ConvertedFilePaths(nil))
	assert.Empty(t, ConvertedFilePaths([]string{"a.pdf.page-1.png"}))
}

// Page images are the one thing known to sit beside the documents today, so
// what the writer names must not be what the reader picks up.
func TestConvertedFilePathsExcludesWhatPageImagePathWrites(t *testing.T) {
	for _, rawFilePath := range []string{
		"docs/report.pdf",
		"a.pdf",
		"docs/quarterly review 2026.pdf",
		"docs/odd.name.with.dots.pdf",
	} {
		pageImage := PageImagePath(rawFilePath, 7)
		assert.Empty(t, ConvertedFilePaths([]string{pageImage}), "%q should be ignored", pageImage)
		assert.Equal(t, []string{ConvertedFilePath(rawFilePath)},
			ConvertedFilePaths([]string{ConvertedFilePath(rawFilePath), pageImage}))
	}
}
