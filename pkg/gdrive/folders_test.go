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
	"slices"
	"strings"
	"testing"

	drive "google.golang.org/api/drive/v3"
)

// stubLister serves a fixed folder tree, parent ID to children.
type stubLister struct {
	tree     map[string][]*drive.File
	names    map[string]string
	nameErrs map[string]error
	err      error
	calls    int
}

func (s *stubLister) FolderName(_ context.Context, folderID string, _ int) (string, error) {
	if err, ok := s.nameErrs[folderID]; ok {
		return "", err
	}
	if s.err != nil {
		return "", s.err
	}
	if name, ok := s.names[folderID]; ok {
		return name, nil
	}
	return folderID, nil
}

func (s *stubLister) ListSubfolders(_ context.Context, folderID string, _ int) ([]*drive.File, error) {
	s.calls++
	if s.err != nil {
		return nil, s.err
	}
	return s.tree[folderID], nil
}

func folder(id, name string) *drive.File { return &drive.File{Id: id, Name: name} }

func TestExpandFolderTree(t *testing.T) {
	tests := []struct {
		name      string
		tree      map[string][]*drive.File
		roots     []string
		skipNames []string
		want      []string
	}{
		{
			name:  "a flat folder expands to itself",
			tree:  map[string][]*drive.File{},
			roots: []string{"root"},
			want:  []string{"root"},
		},
		{
			// The case that proved this was needed: Drive's "in parents" is
			// not recursive, so a file in a subfolder is missed without this.
			name:  "one subfolder is included",
			tree:  map[string][]*drive.File{"root": {folder("sub", "subsubsub")}},
			roots: []string{"root"},
			want:  []string{"root", "sub"},
		},
		{
			name: "nesting is followed all the way down",
			tree: map[string][]*drive.File{
				"root": {folder("a", "a")},
				"a":    {folder("b", "b")},
				"b":    {folder("c", "c")},
			},
			roots: []string{"root"},
			want:  []string{"root", "a", "b", "c"},
		},
		{
			name: "several roots are each expanded",
			tree: map[string][]*drive.File{
				"r1": {folder("a", "a")},
				"r2": {folder("b", "b")},
			},
			roots: []string{"r1", "r2"},
			want:  []string{"r1", "r2", "a", "b"},
		},
		{
			name: "a skipped folder takes its whole subtree with it",
			tree: map[string][]*drive.File{
				"root":    {folder("keep", "keep"), folder("archive", ".archive")},
				"archive": {folder("buried", "buried")},
			},
			roots:     []string{"root"},
			skipNames: []string{".archive"},
			want:      []string{"root", "keep"},
		},
		{
			name: "a cycle terminates instead of looping",
			tree: map[string][]*drive.File{
				"root": {folder("a", "a")},
				"a":    {folder("root", "root")},
			},
			roots: []string{"root"},
			want:  []string{"root", "a"},
		},
		{
			name:  "a repeated root appears once",
			tree:  map[string][]*drive.File{},
			roots: []string{"root", "root"},
			want:  []string{"root"},
		},
		{
			name:  "an empty root id is ignored",
			tree:  map[string][]*drive.File{},
			roots: []string{"", "root"},
			want:  []string{"root"},
		},
		{
			name:  "no roots yields nothing",
			tree:  map[string][]*drive.File{},
			roots: nil,
			want:  []string{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ExpandFolderTree(context.Background(), &stubLister{tree: tt.tree}, tt.roots, tt.skipNames, 1)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			slices.Sort(got)
			want := slices.Clone(tt.want)
			slices.Sort(want)
			if !slices.Equal(got, want) {
				t.Errorf("folders = %v, want %v", got, want)
			}
		})
	}
}

// The crawler applies the skip list to a declared root as well as to
// subfolders, so search has to, or an excluded root stays searchable while
// never being crawled.
func TestExpandFolderTreeSkipsAnExcludedRoot(t *testing.T) {
	lister := &stubLister{
		tree: map[string][]*drive.File{
			"keep-root":    {folder("keep-child", "child")},
			"archive-root": {folder("buried", "buried")},
		},
		names: map[string]string{
			"keep-root":    "live",
			"archive-root": ".archive",
		},
	}

	got, err := ExpandFolderTree(context.Background(), lister,
		[]string{"keep-root", "archive-root"}, []string{".archive"}, 1)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	slices.Sort(got)
	want := []string{"keep-child", "keep-root"}
	if !slices.Equal(got, want) {
		t.Errorf("folders = %v, want %v with the excluded root and its subtree gone", got, want)
	}
}

// A folder nobody shared with the service account must fail loudly. Listing
// such a folder answers 200 with an empty list, verified against the live API,
// so without naming the root the search would quietly return nothing and the
// caller would read that as "no matches".
func TestExpandFolderTreeReportsAnUnreachableRoot(t *testing.T) {
	lister := &stubLister{
		tree: map[string][]*drive.File{},
		nameErrs: map[string]error{
			"unshared-root": errors.New("googleapi: Error 404: File not found"),
		},
	}

	_, err := ExpandFolderTree(context.Background(), lister, []string{"unshared-root"}, nil, 1)

	if !errors.Is(err, ErrFolderUnreachable) {
		t.Fatalf("error = %v, want one wrapping ErrFolderUnreachable", err)
	}
	if !strings.Contains(err.Error(), "unshared-root") {
		t.Errorf("error %q does not name the folder at fault", err)
	}
}

// Every root is named, with or without a skip list, because the lookup is the
// only thing that distinguishes an unshared folder from an empty one.
func TestExpandFolderTreeNamesEveryRoot(t *testing.T) {
	lister := &nameCountingLister{stubLister: stubLister{tree: map[string][]*drive.File{}}}

	if _, err := ExpandFolderTree(context.Background(), lister, []string{"a", "b"}, nil, 1); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if lister.nameCalls != 2 {
		t.Errorf("named %d roots, want 2", lister.nameCalls)
	}
}

type nameCountingLister struct {
	stubLister
	nameCalls int
}

func (n *nameCountingLister) FolderName(ctx context.Context, folderID string, maxRetries int) (string, error) {
	n.nameCalls++
	return n.stubLister.FolderName(ctx, folderID, maxRetries)
}

// A tree too big for one query must say so, since Drive would otherwise reject
// the query with an opaque "too complex" or we would search only part of it.
func TestExpandFolderTreeRefusesAnOversizedTree(t *testing.T) {
	children := make([]*drive.File, 0, MaxScopedFolders+10)
	for i := range MaxScopedFolders + 10 {
		children = append(children, folder(fmt.Sprintf("f%d", i), fmt.Sprintf("name%d", i)))
	}
	lister := &stubLister{tree: map[string][]*drive.File{"root": children}}

	_, err := ExpandFolderTree(context.Background(), lister, []string{"root"}, nil, 1)

	if !errors.Is(err, ErrTooManyFolders) {
		t.Fatalf("error = %v, want one wrapping ErrTooManyFolders", err)
	}
}

// The cap must hold exactly where it says, since it is the only thing standing
// between a large tree and an opaque HTML 400 from Drive's frontend.
func TestExpandFolderTreeCapIsOffByNothing(t *testing.T) {
	childrenUnder := func(count int) []*drive.File {
		children := make([]*drive.File, 0, count)
		for i := range count {
			children = append(children, folder(fmt.Sprintf("f%d", i), fmt.Sprintf("name%d", i)))
		}
		return children
	}

	// One root plus this many children lands on the cap exactly.
	atCap := childrenUnder(MaxScopedFolders - 1)
	got, err := ExpandFolderTree(context.Background(), &stubLister{
		tree: map[string][]*drive.File{"root": atCap},
	}, []string{"root"}, nil, 1)
	if err != nil {
		t.Fatalf("a tree of exactly %d folders was refused: %v", MaxScopedFolders, err)
	}
	if len(got) != MaxScopedFolders {
		t.Errorf("got %d folders, want %d", len(got), MaxScopedFolders)
	}

	overCap := childrenUnder(MaxScopedFolders)
	if _, err := ExpandFolderTree(context.Background(), &stubLister{
		tree: map[string][]*drive.File{"root": overCap},
	}, []string{"root"}, nil, 1); !errors.Is(err, ErrTooManyFolders) {
		t.Fatalf("a tree of %d folders gave %v, want ErrTooManyFolders", MaxScopedFolders+1, err)
	}
}

// Roots are appended before the walk begins, so they need their own check or a
// pipeline declaring hundreds of folders slips past the cap entirely.
func TestExpandFolderTreeCountsRootsTowardTheCap(t *testing.T) {
	roots := make([]string, 0, MaxScopedFolders+1)
	tree := map[string][]*drive.File{}
	for i := range MaxScopedFolders + 1 {
		id := fmt.Sprintf("root%d", i)
		roots = append(roots, id)
		tree[id] = nil
	}

	_, err := ExpandFolderTree(context.Background(), &stubLister{tree: tree}, roots, nil, 1)

	if !errors.Is(err, ErrTooManyFolders) {
		t.Fatalf("error = %v, want one wrapping ErrTooManyFolders", err)
	}
}

func TestExpandFolderTreeSurfacesAListingFailure(t *testing.T) {
	lister := &stubLister{err: errors.New("drive 503")}

	_, err := ExpandFolderTree(context.Background(), lister, []string{"root"}, nil, 1)

	if err == nil {
		t.Fatal("expected the listing failure to be reported, not swallowed")
	}
}

func TestFolderIDFromURL(t *testing.T) {
	tests := []struct {
		name    string
		in      string
		want    string
		wantErr bool
	}{
		{"a folder URL", "https://drive.google.com/drive/folders/abc123", "abc123", false},
		{"with a user segment", "https://drive.google.com/drive/u/0/folders/abc123", "abc123", false},
		{"with query params", "https://drive.google.com/drive/folders/abc123?usp=sharing", "abc123", false},
		{"with a trailing slash", "https://drive.google.com/drive/folders/abc123/", "abc123", false},
		{"a bare id passes through", "abc123", "abc123", false},
		{"a URL with no folders segment", "https://drive.google.com/drive/u/0/my-drive", "", true},
		{"a file URL is not a folder URL", "https://drive.google.com/file/d/abc123/view", "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := FolderIDFromURL(tt.in)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("FolderIDFromURL(%q) = %q, want an error", tt.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("FolderIDFromURL(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestBuildSearchQueryScopesToFolders(t *testing.T) {
	query := BuildSearchQuery("report", []string{"root", "sub"})

	for _, want := range []string{"'root' in parents", "'sub' in parents", " or "} {
		if !strings.Contains(query, want) {
			t.Errorf("query %q is missing %q", query, want)
		}
	}
}

// An empty folder set must match nothing. Omitting the clause instead would
// quietly search everything the service account can see, which is wider than
// the pipeline.
func TestBuildSearchQueryWithNoFoldersMatchesNothing(t *testing.T) {
	query := BuildSearchQuery("report", nil)

	if !strings.Contains(query, "(false)") {
		t.Errorf("query %q should be unsatisfiable when no folders are given", query)
	}
	if strings.Contains(query, "in parents") {
		t.Errorf("query %q should name no parents", query)
	}
}

func TestBuildSearchQueryEscapesFolderIDs(t *testing.T) {
	query := BuildSearchQuery("report", []string{"a'b"})

	if strings.Contains(query, "'a'b' in parents") {
		t.Errorf("folder id was not escaped in %q", query)
	}
	if !strings.Contains(query, `'a\'b' in parents`) {
		t.Errorf("query %q does not carry the escaped folder id", query)
	}
}
