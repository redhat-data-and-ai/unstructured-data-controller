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
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/pathfilter"
)

func TestNewClient(t *testing.T) {
	t.Run("with token", func(t *testing.T) {
		c := NewClient("https://gitlab.cee.redhat.com/team/repo", "main", "glpat-test123", operatorv1alpha1.GitProviderGitLab)
		assert.Equal(t, "https://gitlab.cee.redhat.com/team/repo", c.url)
		assert.Equal(t, "main", c.revision)
		require.NotNil(t, c.auth)
		assert.Equal(t, "oauth2", c.auth.Username)
	})

	t.Run("without token", func(t *testing.T) {
		c := NewClient("https://gitlab.cee.redhat.com/team/repo", "main", "", operatorv1alpha1.GitProviderGitLab)
		assert.Nil(t, c.auth)
	})

	t.Run("tag revision", func(t *testing.T) {
		c := NewClient("https://gitlab.cee.redhat.com/team/repo", "v1.2.0", "", operatorv1alpha1.GitProviderGitLab)
		assert.Equal(t, "v1.2.0", c.revision)
	})
}

func TestMatchesFileFormat(t *testing.T) {
	tests := []struct {
		name     string
		filePath string
		formats  []string
		want     bool
	}{
		{"no formats matches all", "docs/readme.md", nil, true},
		{"empty formats matches all", "docs/readme.md", []string{}, true},
		{"md matches md file", "docs/readme.md", []string{"md"}, true},
		{"md rejects txt file", "docs/readme.txt", []string{"md"}, false},
		{"case insensitive", "file.MD", []string{"md"}, true},
		{"multiple formats match first", "file.md", []string{"md", "pdf"}, true},
		{"multiple formats match second", "file.pdf", []string{"md", "pdf"}, true},
		{"multiple formats reject other", "file.go", []string{"md", "pdf"}, false},
		{"nested path matches extension", "a/b/c/file.md", []string{"md"}, true},
		{"no extension rejects", "Makefile", []string{"md"}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := matchesFileFormat(tt.filePath, tt.formats)
			assert.Equal(t, tt.want, got, "matchesFileFormat(%q, %v)", tt.filePath, tt.formats)
		})
	}
}

func TestLooksLikeSHA(t *testing.T) {
	assert.True(t, looksLikeSHA("abc123def456abc123def456abc123def456abc1"))
	assert.False(t, looksLikeSHA("main"))
	assert.False(t, looksLikeSHA("v1.2.0"))
	assert.False(t, looksLikeSHA("abc123"))                                  // too short
	assert.True(t, looksLikeSHA("ABC123DEF456ABC123DEF456ABC123DEF456ABC1")) // uppercase hex is valid
	assert.True(t, looksLikeSHA("aBc123dEf456aBc123dEf456aBc123dEf456aBc1")) // mixed case
}

func TestWalkFS(t *testing.T) {
	fs := memfs.New()
	createFile(t, fs, "docs/intro.md", "# Intro")
	createFile(t, fs, "docs/guide/setup.md", "# Setup")
	createFile(t, fs, "docs/test/foo.md", "# Test")
	createFile(t, fs, "vendor/lib.go", "package lib")
	createFile(t, fs, "README.md", "# README")
	createFile(t, fs, "src/main.go", "package main")
	createFile(t, fs, "src/main_test.go", "package main")

	t.Run("with filter and formats", func(t *testing.T) {
		filter, err := pathfilter.New([]string{"docs/", "README.md", "!**/test/**"})
		require.NoError(t, err)

		entries, err := walkFS(fs, ".", filter, []string{"md"})
		require.NoError(t, err)

		paths := extractPaths(entries)
		assert.Contains(t, paths, "docs/intro.md")
		assert.Contains(t, paths, "docs/guide/setup.md")
		assert.Contains(t, paths, "README.md")
		assert.NotContains(t, paths, "docs/test/foo.md")
		assert.NotContains(t, paths, "vendor/lib.go")
		assert.NotContains(t, paths, "src/main.go")
	})

	t.Run("no filter matches all with format", func(t *testing.T) {
		filter, err := pathfilter.New(nil)
		require.NoError(t, err)

		entries, err := walkFS(fs, ".", filter, []string{"md"})
		require.NoError(t, err)

		paths := extractPaths(entries)
		assert.Contains(t, paths, "docs/intro.md")
		assert.Contains(t, paths, "docs/test/foo.md")
		assert.Contains(t, paths, "README.md")
		assert.NotContains(t, paths, "src/main.go")
	})

	t.Run("nil filter and nil formats matches everything", func(t *testing.T) {
		entries, err := walkFS(fs, ".", nil, nil)
		require.NoError(t, err)
		assert.Len(t, entries, 7)
	})

	t.Run("exclude only", func(t *testing.T) {
		filter, err := pathfilter.New([]string{"!vendor/**", "!**/test/**"})
		require.NoError(t, err)

		entries, err := walkFS(fs, ".", filter, nil)
		require.NoError(t, err)

		paths := extractPaths(entries)
		assert.NotContains(t, paths, "vendor/lib.go")
		assert.NotContains(t, paths, "docs/test/foo.md")
		assert.Contains(t, paths, "docs/intro.md")
		assert.Contains(t, paths, "src/main.go")
	})
}

func TestCloneAndWalkCommitSHA(t *testing.T) {
	dir := t.TempDir()
	repo, err := git.PlainInit(dir, false)
	require.NoError(t, err)
	wt, err := repo.Worktree()
	require.NoError(t, err)

	sig := &object.Signature{Name: "test", Email: "test@example.com", When: time.Now()}
	require.NoError(t, os.WriteFile(filepath.Join(dir, "first.md"), []byte("# First"), 0o644))
	_, err = wt.Add("first.md")
	require.NoError(t, err)
	first, err := wt.Commit("first", &git.CommitOptions{Author: sig})
	require.NoError(t, err)

	require.NoError(t, os.WriteFile(filepath.Join(dir, "second.md"), []byte("# Second"), 0o644))
	_, err = wt.Add("second.md")
	require.NoError(t, err)
	_, err = wt.Commit("second", &git.CommitOptions{Author: sig})
	require.NoError(t, err)

	c := NewClient(dir, first.String(), "", operatorv1alpha1.GitProviderGitLab)
	entries, err := c.CloneAndWalk(context.Background(), nil, []string{"md"})
	require.NoError(t, err)
	assert.Equal(t, []string{"first.md"}, extractPaths(entries))
}

func createFile(t *testing.T, fs billy.Filesystem, path string, content string) {
	t.Helper()
	parts := splitPath(path)
	for i := 1; i < len(parts); i++ {
		dir := joinPath(parts[:i])
		_ = fs.MkdirAll(dir, 0o755)
	}
	f, err := fs.Create(path)
	require.NoError(t, err)
	_, err = f.Write([]byte(content))
	require.NoError(t, err)
	require.NoError(t, f.Close())
}

func splitPath(p string) []string {
	var parts []string
	for _, s := range split(p, '/') {
		if s != "" {
			parts = append(parts, s)
		}
	}
	return parts
}

func split(s string, sep byte) []string {
	var result []string
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == sep {
			result = append(result, s[start:i])
			start = i + 1
		}
	}
	result = append(result, s[start:])
	return result
}

func joinPath(parts []string) string {
	result := ""
	for i, p := range parts {
		if i > 0 {
			result += "/"
		}
		result += p
	}
	return result
}

func extractPaths(entries []FileEntry) []string {
	paths := make([]string, 0, len(entries))
	for _, e := range entries {
		paths = append(paths, e.Path)
	}
	return paths
}
