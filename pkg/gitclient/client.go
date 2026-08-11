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
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"path/filepath"
	"slices"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/storage/memory"
)

type Client struct {
	url    string
	branch string
	auth   *http.BasicAuth
}

type FileEntry struct {
	Path    string
	Hash    string
	Content []byte
}

func NewClient(url, branch, token string) *Client {
	c := &Client{
		url:    url,
		branch: branch,
	}
	if token != "" {
		c.auth = &http.BasicAuth{
			Username: "x-access-token",
			Password: token,
		}
	}
	return c
}

// HeadHash fetches the current HEAD hash of the tracked branch via ls-remote
// (a single HTTP request, no clone required).
func (c *Client) HeadHash(ctx context.Context) (string, error) {
	remote := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: "origin",
		URLs: []string{c.url},
	})

	refs, err := remote.ListContext(ctx, &git.ListOptions{Auth: c.auth})
	if err != nil {
		return "", fmt.Errorf("ls-remote failed for %s: %w", c.url, err)
	}

	targetRef := plumbing.NewBranchReferenceName(c.branch)
	for _, ref := range refs {
		if ref.Name() == targetRef {
			return ref.Hash().String(), nil
		}
	}
	return "", fmt.Errorf("branch %q not found in %s", c.branch, c.url)
}

// CloneAndWalk performs a shallow clone (depth=1) into memory and returns
// files matching the given patterns and path prefixes.
func (c *Client) CloneAndWalk(
	ctx context.Context, patterns, paths, ignoreFolders []string,
) ([]FileEntry, error) {
	fs := memfs.New()
	_, err := git.CloneContext(ctx, memory.NewStorage(), fs, &git.CloneOptions{
		URL:           c.url,
		ReferenceName: plumbing.NewBranchReferenceName(c.branch),
		SingleBranch:  true,
		Depth:         1,
		Auth:          c.auth,
	})
	if err != nil {
		return nil, fmt.Errorf("clone failed for %s: %w", c.url, err)
	}

	return walkFS(fs, ".", patterns, paths, ignoreFolders)
}

func walkFS(
	fs billy.Filesystem, dir string, patterns, paths, ignoreFolders []string,
) ([]FileEntry, error) {
	infos, err := fs.ReadDir(dir)
	if err != nil {
		return nil, err
	}

	var entries []FileEntry
	for _, info := range infos {
		fullPath := filepath.Join(dir, info.Name())
		if fullPath == "." || strings.HasPrefix(fullPath, "."+string(filepath.Separator)) {
			fullPath = strings.TrimPrefix(fullPath, "."+string(filepath.Separator))
		}

		if info.IsDir() {
			if info.Name() == ".git" || isIgnoredFolder(info.Name(), ignoreFolders) {
				continue
			}
			sub, err := walkFS(fs, fullPath, patterns, paths, ignoreFolders)
			if err != nil {
				return nil, err
			}
			entries = append(entries, sub...)
			continue
		}

		if !matchesPath(fullPath, paths) || !matchesPattern(fullPath, patterns) {
			continue
		}

		f, err := fs.Open(fullPath)
		if err != nil {
			return nil, fmt.Errorf("failed to open %s: %w", fullPath, err)
		}
		content, err := io.ReadAll(f)
		_ = f.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read %s: %w", fullPath, err)
		}

		h := sha256.Sum256(content)
		entries = append(entries, FileEntry{
			Path:    fullPath,
			Hash:    hex.EncodeToString(h[:]),
			Content: content,
		})
	}
	return entries, nil
}

func matchesPattern(filePath string, patterns []string) bool {
	if len(patterns) == 0 {
		return true
	}
	baseName := filepath.Base(filePath)
	for _, pattern := range patterns {
		if matched, _ := filepath.Match(pattern, baseName); matched {
			return true
		}
	}
	return false
}

func isIgnoredFolder(name string, ignoreFolders []string) bool {
	return slices.Contains(ignoreFolders, name)
}

func matchesPath(filePath string, paths []string) bool {
	if len(paths) == 0 {
		return true
	}
	normalized := filepath.ToSlash(filepath.Clean(filePath))
	for _, p := range paths {
		prefix := filepath.ToSlash(filepath.Clean(p))
		if normalized == prefix {
			return true
		}
		if strings.HasPrefix(normalized, prefix+"/") {
			return true
		}
	}
	return false
}
