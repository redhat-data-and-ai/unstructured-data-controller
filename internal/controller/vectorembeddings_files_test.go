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

package controller

import (
	"context"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
)

func TestEmbedFiles(t *testing.T) {
	outage := &embedding.APIError{StatusCode: http.StatusServiceUnavailable}
	corrupt := errors.New("decoding chunks file")
	files := []string{"a", "b", "c"}

	tests := []struct {
		name          string
		results       map[string]error
		skipped       map[string]bool // processed=false without error
		wantVisited   []string
		wantProcessed int64
		wantErrors    int
	}{
		{name: "all succeed", wantVisited: files, wantProcessed: 3},
		{name: "an outage stops the loop at the first file", results: map[string]error{"a": outage},
			wantVisited: []string{"a"}, wantErrors: 1},
		{name: "an outage mid-way keeps what already succeeded", results: map[string]error{"b": outage},
			wantVisited: []string{"a", "b"}, wantProcessed: 1, wantErrors: 1},
		{name: "a corrupt file does not stop the files after it", results: map[string]error{"a": corrupt},
			wantVisited: files, wantProcessed: 2, wantErrors: 1},
		{name: "a skipped file is visited but not counted", skipped: map[string]bool{"b": true},
			wantVisited: files, wantProcessed: 2},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var visited []string
			processed, errs := embedFiles(context.Background(), files, func(path string) (bool, error) {
				visited = append(visited, path)
				if err := test.results[path]; err != nil {
					return false, err
				}
				return !test.skipped[path], nil
			})

			assert.Equal(t, test.wantVisited, visited)
			assert.Equal(t, test.wantProcessed, processed)
			assert.Len(t, errs, test.wantErrors)
		})
	}
}
