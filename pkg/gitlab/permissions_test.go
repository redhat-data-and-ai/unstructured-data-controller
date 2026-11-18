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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func writeJSON(t *testing.T, w http.ResponseWriter, v any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(v); err != nil {
		t.Fatalf("failed to encode JSON response: %v", err)
	}
}

func TestGetProjectMembers(t *testing.T) {
	tests := []struct {
		name           string
		handler        http.HandlerFunc
		repoURL        string
		wantCount      int
		wantErr        bool
		wantErrContain string
		checkPerms     func(t *testing.T, perms []Permission)
	}{
		{
			name: "maps access levels to roles correctly",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				members := []gitlabMember{
					{ID: 1, Username: "guest-user", State: "active", AccessLevel: 10},
					{ID: 2, Username: "reporter-user", State: "active", AccessLevel: 20},
					{ID: 3, Username: "dev-user", State: "active", AccessLevel: 30, Email: "dev@example.com"},
					{ID: 4, Username: "maintainer-user", State: "active", AccessLevel: 40},
					{ID: 5, Username: "owner-user", State: "active", AccessLevel: 50},
				}
				writeJSON(t, w, members)
			},
			repoURL:   "https://gitlab.com/group/project",
			wantCount: 5,
			checkPerms: func(t *testing.T, perms []Permission) {
				t.Helper()
				expectedRoles := map[string]string{
					"guest-user":      "guest",
					"reporter-user":   "reporter",
					"dev-user":        "developer",
					"maintainer-user": "maintainer",
					"owner-user":      "owner",
				}
				for _, p := range perms {
					if p.Type != "user" {
						t.Errorf("permission for %s has type %q, want \"user\"", p.UID, p.Type)
					}
					expectedRole, ok := expectedRoles[p.UID]
					if !ok {
						t.Errorf("unexpected member %q in results", p.UID)
						continue
					}
					if p.Role != expectedRole {
						t.Errorf("member %q has role %q, want %q", p.UID, p.Role, expectedRole)
					}
				}
				for _, p := range perms {
					if p.UID == "dev-user" && p.Email != "dev@example.com" {
						t.Errorf("dev-user email = %q, want \"dev@example.com\"", p.Email)
					}
				}
			},
		},
		{
			name: "filters out inactive members",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				members := []gitlabMember{
					{ID: 1, Username: "active-dev", State: "active", AccessLevel: 30},
					{ID: 2, Username: "blocked-dev", State: "blocked", AccessLevel: 30},
					{ID: 3, Username: "deactivated-dev", State: "deactivated", AccessLevel: 40},
				}
				writeJSON(t, w, members)
			},
			repoURL:   "https://gitlab.com/group/project",
			wantCount: 1,
			checkPerms: func(t *testing.T, perms []Permission) {
				t.Helper()
				if perms[0].UID != "active-dev" {
					t.Errorf("expected only active-dev, got %q", perms[0].UID)
				}
			},
		},
		{
			name: "handles pagination via X-Next-Page header",
			handler: func() http.HandlerFunc {
				callCount := 0
				return func(w http.ResponseWriter, _ *http.Request) {
					callCount++
					switch callCount {
					case 1:
						w.Header().Set("X-Next-Page", "2")
						writeJSON(t, w, []gitlabMember{
							{ID: 1, Username: "user-page1", State: "active", AccessLevel: 30},
						})
					case 2:
						writeJSON(t, w, []gitlabMember{
							{ID: 2, Username: "user-page2", State: "active", AccessLevel: 40},
						})
					default:
						t.Error("unexpected third API call during pagination")
						writeJSON(t, w, []gitlabMember{})
					}
				}
			}(),
			repoURL:   "https://gitlab.com/group/project",
			wantCount: 2,
		},
		{
			name: "returns empty list for project with no members",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				writeJSON(t, w, []gitlabMember{})
			},
			repoURL:   "https://gitlab.com/group/project",
			wantCount: 0,
		},
		{
			name: "returns error on 403 forbidden",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusForbidden)
				_, _ = fmt.Fprint(w, `{"message":"403 Forbidden"}`)
			},
			repoURL:        "https://gitlab.com/group/project",
			wantErr:        true,
			wantErrContain: "status 403",
		},
		{
			name: "returns error on 404 not found",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(http.StatusNotFound)
				_, _ = fmt.Fprint(w, `{"message":"404 Project Not Found"}`)
			},
			repoURL:        "https://gitlab.com/group/project",
			wantErr:        true,
			wantErrContain: "status 404",
		},
		{
			name: "returns error on invalid JSON response",
			handler: func(w http.ResponseWriter, _ *http.Request) {
				_, _ = fmt.Fprint(w, `not json`)
			},
			repoURL:        "https://gitlab.com/group/project",
			wantErr:        true,
			wantErrContain: "failed to parse",
		},
		{
			name:           "returns error for invalid repo URL",
			handler:        func(_ http.ResponseWriter, _ *http.Request) {},
			repoURL:        "https://gitlab.com/",
			wantErr:        true,
			wantErrContain: "could not extract project path",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := httptest.NewServer(tt.handler)
			defer server.Close()

			client := NewClient(server.URL, "test-token")
			ctx := context.Background()

			perms, err := client.GetProjectMembers(ctx, tt.repoURL)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErrContain)
				}
				if tt.wantErrContain != "" && !strings.Contains(err.Error(), tt.wantErrContain) {
					t.Errorf("error %q does not contain %q", err.Error(), tt.wantErrContain)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if len(perms) != tt.wantCount {
				t.Errorf("got %d permissions, want %d", len(perms), tt.wantCount)
			}

			if tt.checkPerms != nil {
				tt.checkPerms(t, perms)
			}
		})
	}
}

func TestGetProjectMembers_SendsAuthHeader(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token := r.Header.Get("PRIVATE-TOKEN")
		if token != "my-secret-token" {
			t.Errorf("PRIVATE-TOKEN header = %q, want \"my-secret-token\"", token)
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		writeJSON(t, w, []gitlabMember{})
	}))
	defer server.Close()

	client := NewClient(server.URL, "my-secret-token")
	_, err := client.GetProjectMembers(context.Background(), "https://gitlab.com/g/p")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAccessLevelToRole(t *testing.T) {
	tests := []struct {
		level int
		want  string
	}{
		{5, "guest"},
		{10, "guest"},
		{20, "reporter"},
		{25, "reporter"},
		{30, "developer"},
		{35, "developer"},
		{40, "maintainer"},
		{45, "maintainer"},
		{50, "owner"},
		{60, "owner"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("level_%d", tt.level), func(t *testing.T) {
			got := accessLevelToRole(tt.level)
			if got != tt.want {
				t.Errorf("accessLevelToRole(%d) = %q, want %q", tt.level, got, tt.want)
			}
		})
	}
}
