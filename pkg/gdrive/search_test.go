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
	"strings"
	"testing"
)

func TestEscapeQueryTerm(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"plain text is untouched", "quarterly report", "quarterly report"},
		{"apostrophe is escaped", "quinn's paper", `quinn\'s paper`},
		{"backslash is escaped", `a\b`, `a\\b`},
		{
			// The backslash must be doubled first. Escaping the apostrophe
			// first would let a trailing backslash escape our own escape.
			name: "backslash before apostrophe cannot escape the escape",
			in:   `a\'b`,
			want: `a\\\'b`,
		},
		{"empty stays empty", "", ""},
		{
			name: "the injection payload is neutralised",
			in:   `benchmark' or name contains 'Summary`,
			want: `benchmark\' or name contains \'Summary`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EscapeQueryTerm(tt.in); got != tt.want {
				t.Errorf("EscapeQueryTerm(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

func TestBuildSearchQueryExcludesFoldersAndTrash(t *testing.T) {
	query := BuildSearchQuery("report", []string{"folder1"})

	for _, want := range []string{
		"fullText contains 'report'",
		"trashed = false",
		"mimeType != 'application/vnd.google-apps.folder'",
	} {
		if !strings.Contains(query, want) {
			t.Errorf("query %q is missing %q", query, want)
		}
	}
}

func TestCheckQueryLength(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		wantErr bool
	}{
		{"an ordinary query passes", "fullText contains 'budget'", false},
		{"exactly at the limit passes", strings.Repeat("a", MaxQueryBytes), false},
		{"one byte over is refused", strings.Repeat("a", MaxQueryBytes+1), true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := CheckQueryLength(tt.query)
			if tt.wantErr != (err != nil) {
				t.Fatalf("CheckQueryLength(%d bytes) = %v, wantErr %v", len(tt.query), err, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, ErrQueryTooLong) {
				t.Errorf("error = %v, want one wrapping ErrQueryTooLong", err)
			}
		})
	}
}

func TestCheckQueryLengthCatchesALongTermUnderTheFolderCap(t *testing.T) {
	folderIDs := make([]string, 0, MaxScopedFolders)
	for i := range MaxScopedFolders {
		folderIDs = append(folderIDs, fmt.Sprintf("1zzzTESTFOLDERIDPADDINGPAD%07d", i))
	}

	if err := CheckQueryLength(BuildSearchQuery("budget", folderIDs)); err != nil {
		t.Fatalf("a full tree with a short term should pass: %v", err)
	}

	longTerm := strings.Repeat("x", 4000)
	err := CheckQueryLength(BuildSearchQuery(longTerm, folderIDs))
	if !errors.Is(err, ErrQueryTooLong) {
		t.Fatalf("error = %v, want one wrapping ErrQueryTooLong", err)
	}
}
