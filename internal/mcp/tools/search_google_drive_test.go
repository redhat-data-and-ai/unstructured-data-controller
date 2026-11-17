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

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	drive "google.golang.org/api/drive/v3"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/google"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

// testFolderIDs stands in for a pipeline's expanded folder tree.
var testFolderIDs = []string{"root-folder", "sub-folder"}

// fakeDriveSearcher serves fixed pages and records what was asked of it.
type fakeDriveSearcher struct {
	pages      [][]*drive.File
	searchErr  error
	incomplete bool

	mu         sync.Mutex
	queriesRun []string
	pagesRead  int
}

func (f *fakeDriveSearcher) SearchFiles(
	_ context.Context, query string, _ int, pageToken string, _ int,
) (*google.SearchResult, error) {
	f.mu.Lock()
	f.queriesRun = append(f.queriesRun, query)
	f.pagesRead++
	f.mu.Unlock()

	if f.searchErr != nil {
		return nil, f.searchErr
	}

	index := 0
	if pageToken != "" {
		parsed, err := strconv.Atoi(pageToken)
		if err != nil {
			return nil, fmt.Errorf("bad page token %q", pageToken)
		}
		index = parsed
	}
	if index >= len(f.pages) {
		return &google.SearchResult{Incomplete: f.incomplete}, nil
	}

	next := ""
	if index+1 < len(f.pages) {
		next = strconv.Itoa(index + 1)
	}
	return &google.SearchResult{
		Files:         f.pages[index],
		NextPageToken: next,
		Incomplete:    f.incomplete,
	}, nil
}

func (*fakeDriveSearcher) Close() {}

// allowAll stands in for a caller who may read everything Drive returned.
func allowAll(_ context.Context, fileIDs []string) ([]snowflake.ReadableFile, error) {
	out := make([]snowflake.ReadableFile, 0, len(fileIDs))
	for _, fileID := range fileIDs {
		out = append(out, snowflake.ReadableFile{
			FileID:   fileID,
			FilePath: "pipelines/p/stages/crawl/" + fileID + ".pdf",
		})
	}
	return out, nil
}

// allowOnly stands in for a row access policy that permits just these files.
func allowOnly(permitted ...string) accessFilter {
	return func(_ context.Context, fileIDs []string) ([]snowflake.ReadableFile, error) {
		out := []snowflake.ReadableFile{}
		for _, fileID := range fileIDs {
			for _, want := range permitted {
				if fileID == want {
					out = append(out, snowflake.ReadableFile{FileID: fileID})
				}
			}
		}
		return out, nil
	}
}

func file(id, name string) *drive.File {
	return &drive.File{Id: id, Name: name, MimeType: "application/pdf"}
}

// The service account can read the whole corpus, so anything Snowflake does
// not return must not reach the caller.
func TestSearchDriveForUserReturnsOnlyPermittedFiles(t *testing.T) {
	searcher := &fakeDriveSearcher{pages: [][]*drive.File{{
		file("a", "alpha.pdf"),
		file("b", "beta.pdf"),
		file("c", "gamma.pdf"),
	}}}

	results, _, err := searchDriveForUser(context.Background(), searcher,
		searchGoogleDriveArgs{Query: "q"}, testFolderIDs, allowOnly("a", "c"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := make([]string, 0, len(results))
	for _, result := range results {
		got = append(got, result.FileID)
	}
	if len(got) != 2 || got[0] != "a" || got[1] != "c" {
		t.Errorf("returned %v, want [a c]", got)
	}
}

// Drive ranks the results, so the filter must not reorder them.
func TestSearchDriveForUserKeepsDriveRanking(t *testing.T) {
	searcher := &fakeDriveSearcher{pages: [][]*drive.File{{
		file("first", "1.pdf"), file("second", "2.pdf"), file("third", "3.pdf"),
	}}}

	// Permitted in a deliberately different order.
	results, _, err := searchDriveForUser(context.Background(), searcher,
		searchGoogleDriveArgs{Query: "q"}, testFolderIDs, allowOnly("third", "first"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 2 || results[0].FileID != "first" || results[1].FileID != "third" {
		t.Errorf("got %v, want Drive order [first third]", results)
	}
}

// Failing closed matters most here: an access check that errors must not be
// read as "this caller may see nothing", nor as "may see everything".
func TestSearchDriveForUserFailsClosedWhenAccessCannotBeDecided(t *testing.T) {
	searcher := &fakeDriveSearcher{pages: [][]*drive.File{{file("a", "alpha.pdf")}}}
	failing := func(_ context.Context, _ []string) ([]snowflake.ReadableFile, error) {
		return nil, errors.New("snowflake is unreachable")
	}

	results, _, err := searchDriveForUser(context.Background(), searcher,
		searchGoogleDriveArgs{Query: "q"}, testFolderIDs, failing)

	if err == nil {
		t.Fatal("an undecidable access check returned success")
	}
	if results != nil {
		t.Errorf("results = %v, want none alongside an error", results)
	}
}

// The stored path comes from the warehouse, not from Drive, and is the only
// thing a download link can be signed against.
func TestSearchDriveForUserCarriesTheStoredPath(t *testing.T) {
	searcher := &fakeDriveSearcher{pages: [][]*drive.File{{file("a", "alpha.pdf")}}}

	results, _, err := searchDriveForUser(context.Background(), searcher,
		searchGoogleDriveArgs{Query: "q"}, testFolderIDs, allowAll)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 1 {
		t.Fatalf("got %d results, want 1", len(results))
	}
	if results[0].storedPath != "pipelines/p/stages/crawl/a.pdf" {
		t.Errorf("storedPath = %q, want the warehouse value", results[0].storedPath)
	}
	// Signing happens in the handler, after filtering, so nothing is set here.
	if results[0].DownloadURL != "" {
		t.Errorf("DownloadURL = %q, want empty until signed", results[0].DownloadURL)
	}
}

func TestSearchDriveForUserPagesUntilItHasEnough(t *testing.T) {
	searcher := &fakeDriveSearcher{pages: [][]*drive.File{
		{file("a", "a.pdf")},
		{file("b", "b.pdf")},
		{file("c", "c.pdf")},
	}}

	results, outcome, err := searchDriveForUser(context.Background(), searcher,
		searchGoogleDriveArgs{Query: "q", Limit: 3}, testFolderIDs, allowAll)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(results) != 3 {
		t.Errorf("got %d results, want 3 across three pages", len(results))
	}
	if !outcome.Incomplete && outcome.Partial {
		t.Error("a search that read every page should not report partial coverage")
	}
}

// A term matching far more than the caller asked for must not walk the corpus.
func TestSearchDriveForUserStopsAtThePageBudget(t *testing.T) {
	pages := make([][]*drive.File, maxDriveSearchPages+5)
	for i := range pages {
		pages[i] = []*drive.File{file(fmt.Sprintf("f%d", i), "f.pdf")}
	}
	searcher := &fakeDriveSearcher{pages: pages}

	// A limit high enough that the budget, not the limit, is what stops it.
	_, _, err := searchDriveForUser(context.Background(), searcher,
		searchGoogleDriveArgs{Query: "q", Limit: maxDriveSearchLimit}, testFolderIDs, allowAll)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if searcher.pagesRead > maxDriveSearchPages {
		t.Errorf("read %d pages, want at most %d", searcher.pagesRead, maxDriveSearchPages)
	}
}

func TestSearchDriveForUserReportsIncompleteSearch(t *testing.T) {
	searcher := &fakeDriveSearcher{
		pages:      [][]*drive.File{{file("a", "a.pdf")}},
		incomplete: true,
	}

	_, outcome, err := searchDriveForUser(context.Background(), searcher,
		searchGoogleDriveArgs{Query: "q"}, testFolderIDs, allowAll)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !outcome.Incomplete {
		t.Error("Drive said it did not search everything and that was not reported")
	}
}

// One query now, not two. A shortcut's ACL is its parent folder's, so it says
// nothing about its target and must stay out.
func TestSearchDriveForUserRunsOneQueryExcludingShortcuts(t *testing.T) {
	searcher := &fakeDriveSearcher{pages: [][]*drive.File{{file("a", "a.pdf")}}}

	if _, _, err := searchDriveForUser(context.Background(), searcher,
		searchGoogleDriveArgs{Query: "q"}, testFolderIDs, allowAll); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(searcher.queriesRun) != 1 {
		t.Fatalf("ran %d queries, want 1", len(searcher.queriesRun))
	}
	if !strings.Contains(searcher.queriesRun[0], "application/vnd.google-apps.shortcut") {
		t.Errorf("query does not exclude shortcuts: %q", searcher.queriesRun[0])
	}
	// The caller's identity is no longer part of the Drive query at all.
	if strings.Contains(searcher.queriesRun[0], "in readers") {
		t.Errorf("query still scopes by reader, which Snowflake now decides: %q", searcher.queriesRun[0])
	}
}

func TestCandidateBudget(t *testing.T) {
	if got := candidateBudget(10); got <= 10 {
		t.Errorf("candidateBudget(10) = %d, want more than the limit so filtering has slack", got)
	}
	if got := candidateBudget(maxDriveSearchLimit); got > maxReadableCandidates {
		t.Errorf("candidateBudget(%d) = %d, want at most %d",
			maxDriveSearchLimit, got, maxReadableCandidates)
	}
}

func TestResolveDriveLimit(t *testing.T) {
	tests := []struct {
		name      string
		requested int
		want      int
	}{
		{"unset uses the default", 0, defaultDriveSearchLimit},
		{"negative uses the default", -5, defaultDriveSearchLimit},
		{"a sane value is honoured", 25, 25},
		{"above the maximum is clamped", 5000, maxDriveSearchLimit},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resolveDriveLimit(tt.requested); got != tt.want {
				t.Errorf("resolveDriveLimit(%d) = %d, want %d", tt.requested, got, tt.want)
			}
		})
	}
}

// Reporting the shortcut's target would name a file whose access was never
// checked, since the row access policy was asked about the shortcut.
func TestToDriveSearchResultNeverFollowsAShortcut(t *testing.T) {
	shortcut := &drive.File{
		Id:              "shortcut-id",
		Name:            "pointer",
		MimeType:        "application/vnd.google-apps.shortcut",
		ShortcutDetails: &drive.FileShortcutDetails{TargetId: "target-id"},
	}

	got := toDriveSearchResult(shortcut)

	if got.FileID != "shortcut-id" {
		t.Errorf("FileID = %q, want the matched file and not its target", got.FileID)
	}
}

func TestCoverageNote(t *testing.T) {
	tests := []struct {
		name         string
		outcome      searchOutcome
		wantContains string
	}{
		{"complete search says nothing", searchOutcome{}, ""},
		{
			name:         "Drive admitting an incomplete search is reported",
			outcome:      searchOutcome{Incomplete: true},
			wantContains: "did not search everything",
		},
		{
			name:         "stopping early is reported",
			outcome:      searchOutcome{Partial: true},
			wantContains: "stopped before reading every match",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := coverageNote(tt.outcome)
			if tt.wantContains == "" {
				if got != "" {
					t.Errorf("coverageNote = %q, want empty", got)
				}
				return
			}
			if !strings.Contains(got, tt.wantContains) {
				t.Errorf("coverageNote = %q, want it to mention %q", got, tt.wantContains)
			}
		})
	}
}

func TestClampNote(t *testing.T) {
	if got := clampNote(10); got != "" {
		t.Errorf("clampNote(10) = %q, want empty below the cap", got)
	}
	got := clampNote(5000)
	if !strings.Contains(got, strconv.Itoa(maxDriveSearchLimit)) {
		t.Errorf("clampNote(5000) = %q, want it to name the maximum", got)
	}
}

// countingLister records how often the tree is walked, so the cache can be
// shown to prevent a walk per request.
type countingLister struct {
	tree  map[string][]*drive.File
	walks int
}

func (c *countingLister) ListSubfolders(_ context.Context, folderID string, _ int) ([]*drive.File, error) {
	c.walks++
	return c.tree[folderID], nil
}

func (*countingLister) FolderName(_ context.Context, folderID string, _ int) (string, error) {
	return folderID, nil
}

func TestFolderTreeCache(t *testing.T) {
	tree := map[string][]*drive.File{"root": {{Id: "sub", Name: "subsubsub"}}}
	base := time.Date(2026, 11, 11, 12, 0, 0, 0, time.UTC)

	t.Run("expands once and reuses the answer", func(t *testing.T) {
		lister := &countingLister{tree: tree}
		cache := newFolderTreeCache()

		for range 5 {
			got, err := cache.get(context.Background(), "p", lister, []string{"root"}, nil, base)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if len(got) != 2 {
				t.Fatalf("folders = %v, want root and sub", got)
			}
		}
		// One walk per folder in the tree, for a single expansion.
		if lister.walks != 2 {
			t.Errorf("walked %d times, want 2 for one expansion", lister.walks)
		}
	})

	t.Run("re-expands once the entry expires", func(t *testing.T) {
		lister := &countingLister{tree: tree}
		cache := newFolderTreeCache()

		if _, err := cache.get(context.Background(), "p", lister, []string{"root"}, nil, base); err != nil {
			t.Fatal(err)
		}
		if _, err := cache.get(context.Background(), "p", lister, []string{"root"}, nil,
			base.Add(folderTreeTTL+time.Second)); err != nil {
			t.Fatal(err)
		}
		if lister.walks != 4 {
			t.Errorf("walked %d times, want 4 for two expansions", lister.walks)
		}
	})

	t.Run("pipelines do not share an entry", func(t *testing.T) {
		lister := &countingLister{tree: tree}
		cache := newFolderTreeCache()

		if _, err := cache.get(context.Background(), "one", lister, []string{"root"}, nil, base); err != nil {
			t.Fatal(err)
		}
		if _, err := cache.get(context.Background(), "two", lister, []string{"root"}, nil, base); err != nil {
			t.Fatal(err)
		}
		if lister.walks != 4 {
			t.Errorf("walked %d times, want 4 for one expansion each", lister.walks)
		}
	})
}
