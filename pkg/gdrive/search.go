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
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// MaxQueryBytes caps a built Drive query.
//
// Drive sends queries as GET parameters and its frontend rejects a URL past
// roughly 16KB with an HTML 400 that carries no JSON error, so the real limit
// is characters rather than folder count. The margin covers the rest of the
// URL and leaves room for the pageToken added on later pages.
const MaxQueryBytes = 15000

// ErrQueryTooLong means a folder tree and search term together exceed what
// Drive will accept in one query.
var ErrQueryTooLong = errors.New("search query is too long for one Drive request")

// CheckQueryLength returns ErrQueryTooLong if query would be rejected for size.
//
// Measures the percent-encoded form, which is what actually travels. Every
// apostrophe becomes %27 and a non-ASCII byte becomes %XX, so a term of 4000
// CJK characters is 4073 bytes here and 36087 on the wire.
func CheckQueryLength(query string) error {
	if encoded := len(url.QueryEscape(query)); encoded > MaxQueryBytes {
		return fmt.Errorf("%w: %d bytes encoded, limit %d", ErrQueryTooLong, encoded, MaxQueryBytes)
	}
	return nil
}

// EscapeQueryTerm makes caller-supplied text safe inside a single-quoted Drive
// query term, doubling the backslash first so the escape cannot itself be
// escaped away. An unescaped apostrophe closes the term early, which can
// detach the folder scope and search beyond the pipeline's corpus.
func EscapeQueryTerm(term string) string {
	term = strings.ReplaceAll(term, `\`, `\\`)
	return strings.ReplaceAll(term, `'`, `\'`)
}

// BuildSearchQuery returns the Drive query for a full text search over the
// documents inside folderIDs. Pass every folder in the tree, from
// ExpandFolderTree, since "in parents" matches direct children only.
//
// Folders are excluded because they are not documents and so are never crawl
// rows. Shortcuts because the row that would authorise one belongs to the file
// it points at, which is a different file from the one Drive matched.
func BuildSearchQuery(term string, folderIDs []string) (string, error) {
	if len(folderIDs) == 0 {
		return "", ErrNoFolders
	}
	return fmt.Sprintf(
		"fullText contains '%s' and trashed = false"+
			" and mimeType != 'application/vnd.google-apps.folder'"+
			" and mimeType != 'application/vnd.google-apps.shortcut'"+
			" and (%s)",
		EscapeQueryTerm(term),
		parentsClause(folderIDs),
	), nil
}

// ErrNoFolders means the search was asked to cover no folders at all.
//
// Separate from an empty result: Drive rejects a query with no parent clause
// as a 400, and one scoped to everything the service account can see would be
// far worse, so the caller must stop rather than ask.
var ErrNoFolders = errors.New("no folders to search")

// parentsClause builds the folder filter. Never called with an empty set,
// which BuildSearchQuery refuses before reaching here.
func parentsClause(folderIDs []string) string {
	clauses := make([]string, 0, len(folderIDs))
	for _, folderID := range folderIDs {
		clauses = append(clauses, fmt.Sprintf("'%s' in parents", EscapeQueryTerm(folderID)))
	}
	return strings.Join(clauses, " or ")
}
