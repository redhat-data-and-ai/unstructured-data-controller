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

package unstructured

import (
	"errors"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateChunks(t *testing.T) {
	tests := []struct {
		name    string
		content string
		chunks  []string
		wantErr bool
	}{
		{
			name:    "content that chunked normally",
			content: "a paragraph of text",
			chunks:  []string{"a paragraph of text"},
			wantErr: false,
		},
		{
			name:    "content that chunked to nothing",
			content: "a slide deck the parser returned no text for",
			chunks:  nil,
			wantErr: true,
		},
		{
			name:    "content that chunked to an empty, non-nil slice",
			content: "a slide deck the parser returned no text for",
			chunks:  []string{},
			wantErr: true,
		},
		{
			name:    "a genuinely empty document is allowed through",
			content: "",
			chunks:  nil,
			wantErr: false,
		},
		{
			// Whitespace counts as empty: a document of blank lines has
			// nothing to lose, and failing it would wedge the stage on input
			// that can never chunk.
			name:    "a whitespace-only document is allowed through",
			content: "   \n\t\n  ",
			chunks:  nil,
			wantErr: false,
		},
		{
			// The guard is about losing a document wholesale, not about the
			// chunker returning blanks, which is a separate concern.
			name:    "an empty chunk still counts as a chunk",
			content: "some text",
			chunks:  []string{""},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := ValidateChunks(tt.content, tt.chunks)

			if !tt.wantErr {
				assert.NoError(t, err)
				return
			}

			require.Error(t, err)
			assert.ErrorIs(t, err, ErrNoChunksProduced)
		})
	}
}

// Callers wrap this with the file path, so the sentinel has to survive
// wrapping for the reconcile loop to tell it apart from any other failure.
func TestErrNoChunksProducedSurvivesWrapping(t *testing.T) {
	wrapped := errors.Join(ErrNoChunksProduced, errors.New("some other failure"))

	assert.True(t, errors.Is(wrapped, ErrNoChunksProduced))
	assert.True(t, strings.Contains(ErrNoChunksProduced.Error(), "no chunks"),
		"the message is what an operator reads off the CR condition")
}
