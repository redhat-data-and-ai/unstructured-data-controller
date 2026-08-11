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

package gitclient

import (
	"testing"
)

func TestMatchesPattern(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		patterns []string
		want     bool
	}{
		{"no patterns matches all", "docs/readme.md", nil, true},
		{"empty patterns matches all", "docs/readme.md", []string{}, true},
		{"md pattern matches md file", "docs/readme.md", []string{"*.md"}, true},
		{"md pattern rejects txt file", "docs/readme.txt", []string{"*.md"}, false},
		{"multiple patterns match first", "file.md", []string{"*.md", "*.rst"}, true},
		{"multiple patterns match second", "file.rst", []string{"*.md", "*.rst"}, true},
		{"multiple patterns reject other", "file.go", []string{"*.md", "*.rst"}, false},
		{"nested path matches basename", "a/b/c/file.md", []string{"*.md"}, true},
		{"exact name pattern", "Makefile", []string{"Makefile"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesPattern(tt.filePath, tt.patterns)
			if got != tt.want {
				t.Errorf("matchesPattern(%q, %v) = %v, want %v", tt.filePath, tt.patterns, got, tt.want)
			}
		})
	}
}

func TestMatchesPath(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		paths    []string
		want     bool
	}{
		{"no paths matches all", "any/file.md", nil, true},
		{"empty paths matches all", "any/file.md", []string{}, true},
		{"prefix match", "docs/guide/intro.md", []string{"docs/"}, true},
		{"prefix match without slash", "docs/guide/intro.md", []string{"docs"}, true},
		{"no match", "src/main.go", []string{"docs/"}, false},
		{"root file with docs prefix", "docs-old/file.md", []string{"docs"}, false},
		{"multiple paths match second", "guides/setup.md", []string{"docs/", "guides/"}, true},
		{"exact file path", "README.md", []string{"README.md"}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesPath(tt.filePath, tt.paths)
			if got != tt.want {
				t.Errorf("matchesPath(%q, %v) = %v, want %v", tt.filePath, tt.paths, got, tt.want)
			}
		})
	}
}

func TestNewClient(t *testing.T) {
	t.Run("with token", func(t *testing.T) {
		c := NewClient("https://github.com/org/repo", "main", "ghp_test123")
		if c.url != "https://github.com/org/repo" {
			t.Errorf("url = %q, want %q", c.url, "https://github.com/org/repo")
		}
		if c.branch != "main" {
			t.Errorf("branch = %q, want %q", c.branch, "main")
		}
		if c.auth == nil {
			t.Fatal("auth should not be nil when token is provided")
		}
		if c.auth.Username != "x-access-token" {
			t.Errorf("auth username = %q, want %q", c.auth.Username, "x-access-token")
		}
	})

	t.Run("without token", func(t *testing.T) {
		c := NewClient("https://github.com/org/repo", "main", "")
		if c.auth != nil {
			t.Error("auth should be nil when no token is provided")
		}
	})
}

func TestIsIgnoredFolder(t *testing.T) {
	tests := []struct {
		name          string
		folder        string
		ignoreFolders []string
		want          bool
	}{
		{"nil list", "vendor", nil, false},
		{"empty list", "vendor", []string{}, false},
		{"matches vendor", "vendor", []string{"vendor", "node_modules"}, true},
		{"matches node_modules", "node_modules", []string{"vendor", "node_modules"}, true},
		{"no match", "src", []string{"vendor", "node_modules"}, false},
		{"partial name not matched", "vendor-old", []string{"vendor"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isIgnoredFolder(tt.folder, tt.ignoreFolders)
			if got != tt.want {
				t.Errorf("isIgnoredFolder(%q, %v) = %v, want %v", tt.folder, tt.ignoreFolders, got, tt.want)
			}
		})
	}
}
