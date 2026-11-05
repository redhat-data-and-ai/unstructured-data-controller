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
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"strings"

	"github.com/go-git/go-billy/v5"
	"github.com/go-git/go-billy/v5/memfs"
	"github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/go-git/go-git/v5/plumbing/transport/http"
	"github.com/go-git/go-git/v5/storage/memory"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/pathfilter"
)

type Client struct {
	url      string
	revision string
	auth     *http.BasicAuth
}

type FileEntry struct {
	Path    string
	Hash    string
	Content []byte
}

func NewClient(url, revision, token string, _ operatorv1alpha1.GitProvider) *Client {
	c := &Client{
		url:      url,
		revision: revision,
	}
	if token != "" {
		c.auth = &http.BasicAuth{
			Username: "oauth2",
			Password: token,
		}
	}
	return c
}

func classifyGitError(err error, repoURL string, hasAuth bool) error {
	switch {
	case errors.Is(err, transport.ErrRepositoryNotFound):
		return fmt.Errorf("repository %q not found — verify the URL is correct", repoURL)
	case errors.Is(err, transport.ErrAuthenticationRequired):
		if hasAuth {
			return fmt.Errorf(
				"repository %q is not accessible — verify the URL is correct and that the access token has not expired",
				repoURL)
		}
		return fmt.Errorf("repository %q is private or does not exist — verify the URL is correct", repoURL)
	case errors.Is(err, transport.ErrAuthorizationFailed):
		return fmt.Errorf("access denied to %q — the access token lacks read permission on this repository", repoURL)
	default:
		return fmt.Errorf("failed to reach %q: %w", repoURL, err)
	}
}

// CommitSHA fetches the current commit SHA of the tracked revision via ls-remote.
// If the revision is already a full commit SHA, it is returned directly.
func (c *Client) CommitSHA(ctx context.Context) (string, error) {
	if looksLikeSHA(c.revision) {
		return c.revision, nil
	}

	remote := git.NewRemote(memory.NewStorage(), &config.RemoteConfig{
		Name: "origin",
		URLs: []string{c.url},
	})

	refs, err := remote.ListContext(ctx, &git.ListOptions{Auth: c.auth})
	if err != nil {
		return "", classifyGitError(err, c.url, c.auth != nil)
	}

	candidates := []plumbing.ReferenceName{
		plumbing.NewBranchReferenceName(c.revision),
		plumbing.NewTagReferenceName(c.revision),
	}
	for _, targetRef := range candidates {
		for _, ref := range refs {
			if ref.Name() == targetRef {
				return ref.Hash().String(), nil
			}
		}
	}
	return "", fmt.Errorf("revision %q not found as branch or tag in %s", c.revision, c.url)
}

func (c *Client) cloneWithRevision(ctx context.Context, auth *http.BasicAuth) (billy.Filesystem, error) {
	fs := memfs.New()
	if looksLikeSHA(c.revision) {
		repo, err := git.CloneContext(ctx, memory.NewStorage(), fs, &git.CloneOptions{
			URL:        c.url,
			Auth:       auth,
			NoCheckout: true,
		})
		if err != nil {
			return nil, err
		}
		wt, err := repo.Worktree()
		if err != nil {
			return nil, err
		}
		if err := wt.Checkout(&git.CheckoutOptions{Hash: plumbing.NewHash(c.revision)}); err != nil {
			return nil, err
		}
		return fs, nil
	}

	cloneOpts := &git.CloneOptions{
		URL:           c.url,
		ReferenceName: plumbing.NewBranchReferenceName(c.revision),
		SingleBranch:  true,
		Depth:         1,
		Auth:          auth,
	}
	_, err := git.CloneContext(ctx, memory.NewStorage(), fs, cloneOpts)
	if errors.Is(err, git.NoMatchingRefSpecError{}) {
		fs = memfs.New()
		cloneOpts.ReferenceName = plumbing.NewTagReferenceName(c.revision)
		_, err = git.CloneContext(ctx, memory.NewStorage(), fs, cloneOpts)
	}
	return fs, err
}

// CloneAndWalk performs a shallow clone (depth=1) into memory and returns
// files matching the given filter and file format extensions.
func (c *Client) CloneAndWalk(
	ctx context.Context, filter *pathfilter.Filter, fileFormats []string,
) ([]FileEntry, error) {
	fs, err := c.cloneWithRevision(ctx, c.auth)
	if err != nil {
		return nil, classifyGitError(err, c.url, c.auth != nil)
	}
	return walkFS(fs, ".", filter, fileFormats)
}

func walkFS(
	fs billy.Filesystem, dir string, filter *pathfilter.Filter, fileFormats []string,
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

		normalized := filepath.ToSlash(fullPath)

		if info.IsDir() {
			if info.Name() == ".git" {
				continue
			}
			if filter != nil && !filter.Match(normalized, true) {
				continue
			}
			sub, err := walkFS(fs, fullPath, filter, fileFormats)
			if err != nil {
				return nil, err
			}
			entries = append(entries, sub...)
			continue
		}

		if filter != nil && !filter.Match(normalized, false) {
			continue
		}

		if !matchesFileFormat(normalized, fileFormats) {
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

// matchesFileFormat checks if a file has one of the specified extensions.
// Extensions are bare strings without dots (e.g. "md", "pdf").
// An empty list matches all files.
func matchesFileFormat(filePath string, formats []string) bool {
	if len(formats) == 0 {
		return true
	}
	ext := strings.TrimPrefix(filepath.Ext(filePath), ".")
	for _, f := range formats {
		if strings.EqualFold(ext, f) {
			return true
		}
	}
	return false
}

func looksLikeSHA(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, c := range s {
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') && (c < 'A' || c > 'F') {
			return false
		}
	}
	return true
}
