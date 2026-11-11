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

package tools

import "testing"

const testPrefix = "my-product/destination"

// A stored path that escapes the stage must be refused before any URL is
// signed. path.Join resolves "..", so without this check a crafted path would
// sign a link to another product's data in the same bucket.
func TestStoredObjectKeyRefusesAnEscapingPath(t *testing.T) {
	tests := []struct {
		name       string
		storedPath string
	}{
		{
			name:       "climbs out of the prefix",
			storedPath: "stages/crawl/../../../other-product/secret.pdf",
		},
		{"absolute path", "/etc/passwd"},
		{"not under the named stage", "pipelines/p/stages/convert/file.pdf"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key, err := storedObjectKey(testPrefix, tt.storedPath, "crawl")

			if err == nil {
				t.Fatalf("accepted %q, producing key %q", tt.storedPath, key)
			}
			if key != "" {
				t.Errorf("returned key %q alongside an error", key)
			}
		})
	}
}

func TestStoredObjectKeyBuildsAPrefixedKey(t *testing.T) {
	key, err := storedObjectKey(testPrefix, "pipelines/p/stages/crawl/file.pdf", "crawl")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	want := "my-product/destination/stages/crawl/file.pdf"
	if key != want {
		t.Errorf("key = %q, want %q", key, want)
	}
}
