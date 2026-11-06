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

// DestinationKey reproduces where the syncer puts a file, so that a citation
// can name a page image it never uploaded itself. Two implementations of one
// convention, and if they drift every citation image is a 404 that no other
// test would catch.
func TestDestinationKeyMatchesTheSyncer(t *testing.T) {
	destination := &S3Destination{Prefix: "my-product/destination", StageName: "convert"}

	for _, filePath := range []string{
		"pipelines/p/stages/convert/docs/report.pdf.json",
		"pipelines/p/stages/convert/docs/report.pdf.page-3.png",
		"pipelines/p/stages/convert/report.pdf.page-1.png",
		// A document whose own path says "stages/", which must not be
		// mistaken for the stage directory.
		"pipelines/p/stages/convert/docs/stages/report.pdf.page-2.png",
		"pipelines/p/stages/convert/docs/odd name 2026.pdf.page-12.png",
	} {
		assert.Equal(t, destination.s3Key(filePath), destination.Prefix+"/"+DestinationKey(filePath),
			"key for %q", filePath)
	}
}

// The page image path is built from the converted file the chunk came from,
// so the whole round trip has to land where the syncer put the image.
func TestPageImageDestinationKeyMatchesTheSyncer(t *testing.T) {
	destination := &S3Destination{Prefix: "my-product/destination", StageName: "convert"}
	convertedFilePath := "pipelines/p/stages/convert/docs/report.pdf.json"

	key := PageImageDestinationKey(convertedFilePath, 3)

	assert.Equal(t, "stages/convert/docs/report.pdf.page-3.png", key)
	assert.Equal(t,
		destination.s3Key("pipelines/p/stages/convert/docs/report.pdf.page-3.png"),
		destination.Prefix+"/"+key)
}
