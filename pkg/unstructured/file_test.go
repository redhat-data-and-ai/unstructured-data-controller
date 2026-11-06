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

// IsPageImagePath has to recognise everything PageImagePath writes.
func TestPageImagePathRoundTrips(t *testing.T) {
	for _, rawFilePath := range []string{
		"docs/report.pdf",
		"a.pdf",
		"docs/quarterly review 2026.pdf",
		"docs/odd.name.with.dots.pdf",
	} {
		path := PageImagePath(rawFilePath, 7)
		assert.True(t, IsPageImagePath(path), "%q should be recognised", path)
	}
}

// Both the chunking stage and the syncer list the convert directory, and
// both have to leave these behind.
func TestWithoutPageImages(t *testing.T) {
	kept := WithoutPageImages([]string{
		"docs/report.pdf",
		"docs/report.pdf.json",
		"docs/report.pdf.page-1.png",
		"docs/report.pdf.page-2.png",
		"docs/diagram.png",
	})

	assert.Equal(t, []string{
		"docs/report.pdf",
		"docs/report.pdf.json",
		"docs/diagram.png",
	}, kept)
}

func TestWithoutPageImages_NothingToDrop(t *testing.T) {
	assert.Empty(t, WithoutPageImages(nil))
	assert.Equal(t, []string{"a.json"}, WithoutPageImages([]string{"a.json"}))
	assert.Empty(t, WithoutPageImages([]string{"a.pdf.page-1.png"}))
}

func TestIsPageImagePath(t *testing.T) {
	tests := []struct {
		name string
		path string
		want bool
	}{
		{name: "a page image", path: "docs/report.pdf.page-1.png", want: true},
		{name: "a later page", path: "docs/report.pdf.page-143.png", want: true},
		{name: "a converted file", path: "docs/report.pdf.json", want: false},
		{name: "the raw file", path: "docs/report.pdf", want: false},
		{name: "a permissions file", path: "docs/report.pdf.permissions.json", want: false},
		{name: "some other png", path: "docs/diagram.png", want: false},
		{name: "the suffix without a number", path: "docs/report.pdf.page-.png", want: false},
		{name: "a non-numeric page", path: "docs/report.pdf.page-one.png", want: false},
		{name: "page zero", path: "docs/report.pdf.page-0.png", want: false},
		{name: "a negative page", path: "docs/report.pdf.page--1.png", want: false},
		{name: "the suffix with no extension", path: "docs/report.pdf.page-1", want: false},
		{name: "empty", path: "", want: false},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			assert.Equal(t, test.want, IsPageImagePath(test.path))
		})
	}
}
