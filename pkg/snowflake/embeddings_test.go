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

package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildSearchPattern(t *testing.T) {
	tests := []struct {
		name    string
		query   string
		want    string
		wantErr bool
	}{
		{
			name:  "multiple terms",
			query: "kubernetes pod scheduling",
			want:  "kubernetes|pod|scheduling",
		},
		{
			name:  "filters single-character terms",
			query: "a kubernetes b pod",
			want:  "kubernetes|pod",
		},
		{
			name:  "escapes regex special characters",
			query: "file.txt path/to/doc",
			want:  `file\.txt|path/to/doc`,
		},
		{
			name:  "escapes parentheses and brackets",
			query: "func() [index]",
			want:  `func\(\)|\[index\]`,
		},
		{
			name:  "empty query returns empty",
			query: "",
			want:  "",
		},
		{
			name:  "only single-char terms returns empty",
			query: "a b c",
			want:  "",
		},
		{
			name:  "extra whitespace handled",
			query: "  kubernetes   pod  ",
			want:  "kubernetes|pod",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := buildSearchPattern(tt.query)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}
