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

package google

import (
	"context"
	"fmt"
	"strings"
	"time"

	drive "google.golang.org/api/drive/v3"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// MaxSearchPageSize is the largest page Drive's files.list will return.
const MaxSearchPageSize int64 = 100

// searchFields is the metadata one search result carries, plus the two
// page-level flags a caller needs to tell "nothing matched" apart from
// "Drive stopped looking".
const searchFields = "incompleteSearch,nextPageToken," +
	"files(id,name,mimeType,modifiedTime,webViewLink,owners(emailAddress),size)"

// SearchResult is one page of a Drive search.
type SearchResult struct {
	Files []*drive.File
	// NextPageToken is empty on the last page. A caller that filters results
	// after the fact has to follow this, or it reports "nothing found" when
	// it really means "nothing on the first page".
	NextPageToken string
	// Incomplete reports Drive's incompleteSearch flag, meaning it did not
	// search everything and matches may be missing whatever the caller does.
	Incomplete bool
}

// SearchFiles runs one page of a Drive files.list query. pageToken is empty
// for the first page, and NextPageToken comes back empty on the last.
//
// query reaches Drive verbatim, so build it with KeywordQuery when the terms
// come from a user. No ordering is requested: Drive ranks a fullText query by
// relevance, and asking for any other order discards that ranking.
func (c *Client) SearchFiles(
	ctx context.Context,
	query string,
	pageSize int64,
	pageToken string,
	maxRetries int,
) (*SearchResult, error) {
	logger := log.FromContext(ctx)
	start := time.Now()

	if pageSize <= 0 || pageSize > MaxSearchPageSize {
		pageSize = MaxSearchPageSize
	}

	logger.V(1).Info("searching Drive", "query", query, "pageSize", pageSize)

	var fileList *drive.FileList
	err := withRetry(ctx, maxRetries, "SearchFiles", func() error {
		call := c.driveService.Files.List().
			Context(ctx).
			Q(query).
			Fields(searchFields).
			SupportsAllDrives(true).
			IncludeItemsFromAllDrives(true).
			PageSize(pageSize)
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		var callErr error
		fileList, callErr = call.Do()
		return callErr
	})
	if err != nil {
		logger.Error(err, "Drive search failed", "timeElapsed", time.Since(start))
		return nil, fmt.Errorf("failed to search Drive: %w", err)
	}

	logger.V(1).Info("Drive search page complete",
		"resultCount", len(fileList.Files),
		"hasMore", fileList.NextPageToken != "",
		"incompleteSearch", fileList.IncompleteSearch,
		"timeElapsed", time.Since(start))

	return &SearchResult{
		Files:         fileList.Files,
		NextPageToken: fileList.NextPageToken,
		Incomplete:    fileList.IncompleteSearch,
	}, nil
}

// KeywordQuery builds a Drive q expression matching keywords anywhere in a
// file's name or indexed content, excluding trashed files, folders and
// shortcuts.
//
// The keywords are quoted into a Drive string literal, so a value containing
// a quote cannot close it and append clauses of its own. Drive binds "and"
// tighter than "or", so an unescaped apostrophe does not merely break the
// query, it moves the clauses after it into a branch that never runs.
func KeywordQuery(keywords string) string {
	return fmt.Sprintf(
		"fullText contains '%s' and trashed = false"+
			" and mimeType != '%s'"+
			" and mimeType != '%s'",
		escapeQueryLiteral(keywords), folderMimeType, shortcutMimeType)
}

// folderMimeType is Drive's type for a folder. Folders match fullText on
// their name and have no content to fetch, so a search excludes them.
const folderMimeType = "application/vnd.google-apps.folder"

// shortcutMimeType is Drive's type for a pointer to another file. A shortcut
// matches on its own name and carries its own ID, so returning one hands the
// caller an ID that resolves to no content.
const shortcutMimeType = "application/vnd.google-apps.shortcut"

// escapeQueryLiteral escapes a value for use inside a single-quoted Drive
// query string. Backslash first, or it would double the escapes it adds.
func escapeQueryLiteral(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `'`, `\'`)
}
