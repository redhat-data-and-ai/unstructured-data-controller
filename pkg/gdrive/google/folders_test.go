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

import "testing"

// A folder ID goes into a single-quoted query term. An unescaped apostrophe
// closes the term early and detaches the folder scope, which is how a listing
// reaches past the pipeline's corpus.
func TestEscapeQueryValue(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{"an ordinary folder id is untouched", "1aBcD-eF_gh", "1aBcD-eF_gh"},
		{"apostrophe is escaped", `a'b`, `a\'b`},
		{"backslash is escaped", `a\b`, `a\\b`},
		{
			// The backslash must be doubled first, or a trailing one escapes
			// our own escape and the apostrophe closes the term after all.
			name: "backslash before apostrophe cannot escape the escape",
			in:   `a\'b`,
			want: `a\\\'b`,
		},
		{"empty stays empty", "", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := escapeQueryValue(tt.in); got != tt.want {
				t.Errorf("escapeQueryValue(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}
