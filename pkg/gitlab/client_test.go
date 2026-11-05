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

package gitlab

import (
	"testing"
)

type urlParseTest struct {
	name    string
	repoURL string
	want    string
	wantErr bool
}

func runURLParseTests(t *testing.T, parseFn func(string) (string, error), tests []urlParseTest) {
	t.Helper()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFn(tt.repoURL)
			if tt.wantErr {
				if err == nil {
					t.Errorf("expected error for %q, got %q", tt.repoURL, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", tt.repoURL, err)
			}
			if got != tt.want {
				t.Errorf("for %q: got %q, want %q", tt.repoURL, got, tt.want)
			}
		})
	}
}

func TestBaseURLFromRepoURL(t *testing.T) {
	runURLParseTests(t, BaseURLFromRepoURL, []urlParseTest{
		{name: "standard gitlab.com URL", repoURL: "https://gitlab.com/group/project", want: "https://gitlab.com"},
		{name: "URL with .git suffix", repoURL: "https://gitlab.com/group/project.git", want: "https://gitlab.com"},
		{
			name: "self-hosted with subgroups", repoURL: "https://gitlab.example.com/org/team/subgroup/project",
			want: "https://gitlab.example.com",
		},
		{name: "URL with port", repoURL: "https://gitlab.internal:8443/group/project", want: "https://gitlab.internal:8443"},
		{name: "HTTP URL", repoURL: "http://gitlab.local/group/project", want: "http://gitlab.local"},
		{name: "missing scheme", repoURL: "gitlab.com/group/project", wantErr: true},
		{name: "empty string", repoURL: "", wantErr: true},
	})
}

func TestProjectPathFromURL(t *testing.T) {
	runURLParseTests(t, ProjectPathFromURL, []urlParseTest{
		{name: "simple group/project", repoURL: "https://gitlab.com/group/project", want: "group/project"},
		{name: "with .git suffix", repoURL: "https://gitlab.com/group/project.git", want: "group/project"},
		{
			name: "nested subgroups", repoURL: "https://gitlab.com/org/team/subgroup/project",
			want: "org/team/subgroup/project",
		},
		{name: "trailing slash", repoURL: "https://gitlab.com/group/project/", want: "group/project"},
		{name: "trailing slash with .git", repoURL: "https://gitlab.com/group/project.git/", want: "group/project"},
		{name: "host only - no project path", repoURL: "https://gitlab.com/", wantErr: true},
		{name: "host only without slash", repoURL: "https://gitlab.com", wantErr: true},
	})
}
