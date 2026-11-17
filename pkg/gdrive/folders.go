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

package gdrive

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"

	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
)

// FolderIDFromURL parses a Google Drive folder URL and returns the folder ID.
// Accepts "https://drive.google.com/drive/folders/<id>" with optional user
// segment and query params, or a bare folder ID.
func FolderIDFromURL(rawURL string) (string, error) {
	if !strings.Contains(rawURL, "/") {
		return rawURL, nil
	}
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}
	parts := strings.Split(strings.TrimRight(parsed.Path, "/"), "/")
	for i, part := range parts {
		if part == "folders" && i+1 < len(parts) {
			return parts[i+1], nil
		}
	}
	return "", fmt.Errorf("could not extract folder ID from URL path: %s", parsed.Path)
}

// MaxScopedFolders caps how many folders one search query may name.
//
// Measured against Drive: a GET whose URL passes roughly 16KB is rejected by
// Google's frontend with an HTML 400 carrying no JSON error, which happens
// around 310 folder clauses at about 50 characters each. This cap sits under
// that so the failure is ours and explicit.
//
// It is only an early guard. The real budget is characters, and the search term
// shares it, so MaxQueryBytes is what actually decides.
const MaxScopedFolders = 250

// ErrTooManyFolders means a pipeline's folder tree cannot be expressed in one
// Drive query.
var ErrTooManyFolders = errors.New("folder tree is too large to search in one query")

// ErrFolderUnreachable means a declared folder could not be read, usually
// because nobody shared it with the service account.
var ErrFolderUnreachable = errors.New("folder is not reachable by the service account")

// SubfolderLister lists the folders directly inside a folder, and names one.
//
// FolderName exists so a declared root can be matched against the skip list.
// Subfolders arrive named from ListSubfolders, but a root is known only by the
// id parsed out of its URL.
type SubfolderLister interface {
	ListSubfolders(ctx context.Context, folderID string, maxRetries int) ([]*drive.File, error)
	FolderName(ctx context.Context, folderID string, maxRetries int) (string, error)
}

// ExpandFolderTree returns every folder in the trees under rootIDs, including
// the roots, so a search can be scoped to them.
//
// Needed because Drive's "in parents" matches direct children only, with no
// recursive operator, so scoping to a root alone would miss every file in a
// subfolder. skipNames drops a folder and its whole subtree, matching the
// crawler's exact-name comparison so search and crawl agree on the corpus.
//
// Returns ErrTooManyFolders past MaxScopedFolders rather than silently
// searching part of the tree.
func ExpandFolderTree(
	ctx context.Context,
	lister SubfolderLister,
	rootIDs []string,
	skipNames []string,
	maxRetries int,
) ([]string, error) {
	skip := make(map[string]bool, len(skipNames))
	for _, name := range skipNames {
		skip[name] = true
	}

	seen := make(map[string]bool, len(rootIDs))
	out := make([]string, 0, len(rootIDs))
	queue := make([]string, 0, len(rootIDs))

	for _, rootID := range rootIDs {
		if rootID == "" || seen[rootID] {
			continue
		}

		// Naming the root does double duty. It applies the skip list, which
		// the crawler also applies to roots, and it is the only way to notice
		// a folder that was never shared with the service account: listing
		// such a folder answers 200 with an empty list, so the search would
		// silently return nothing, while this 404s.
		name, err := lister.FolderName(ctx, rootID, maxRetries)
		if err != nil {
			// Only a refusal means unshared. Telling an admin to fix sharing
			// after a 503 sends them to the wrong place entirely.
			var apiErr *googleapi.Error
			if errors.As(err, &apiErr) && (apiErr.Code == http.StatusNotFound ||
				apiErr.Code == http.StatusForbidden) {
				return nil, fmt.Errorf("%w: %s: %w", ErrFolderUnreachable, rootID, err)
			}
			return nil, fmt.Errorf("reading folder %s: %w", rootID, err)
		}
		if skip[name] {
			continue
		}

		seen[rootID] = true
		out = append(out, rootID)
		queue = append(queue, rootID)
	}

	// Roots are appended above without a check, so a pipeline declaring more
	// than the cap would otherwise skip the guard and fail later as an opaque
	// 400 from Drive.
	if len(out) > MaxScopedFolders {
		return nil, fmt.Errorf("%w: more than %d folders", ErrTooManyFolders, MaxScopedFolders)
	}

	for len(queue) > 0 {
		folderID := queue[0]
		queue = queue[1:]

		children, err := lister.ListSubfolders(ctx, folderID, maxRetries)
		if err != nil {
			return nil, err
		}

		for _, child := range children {
			// seen also breaks cycles, which Drive allows through shortcuts.
			if child.Id == "" || seen[child.Id] || skip[child.Name] {
				continue
			}
			seen[child.Id] = true
			out = append(out, child.Id)
			queue = append(queue, child.Id)

			if len(out) > MaxScopedFolders {
				return nil, fmt.Errorf("%w: more than %d folders", ErrTooManyFolders, MaxScopedFolders)
			}
		}
	}

	return out, nil
}
