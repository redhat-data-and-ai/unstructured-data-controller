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
	"context"
	"strings"
	"testing"
)

// An empty ask must not reach Snowflake at all. Without this guard the query
// becomes "IN ()", which is a syntax error rather than an empty result.
func TestFilterReadableFilesSkipsAnEmptyAsk(t *testing.T) {
	got, err := FilterReadableFiles(context.Background(), "token", "db", "marts", "crawl_mv", nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != nil {
		t.Errorf("got %v, want nil without querying", got)
	}
}

// The cap exists so one request cannot turn into an unbounded IN list.
func TestFilterReadableFilesRefusesTooManyIDs(t *testing.T) {
	fileIDs := make([]string, maxReadableFileIDs+1)
	for i := range fileIDs {
		fileIDs[i] = "file"
	}

	_, err := FilterReadableFiles(context.Background(), "token", "db", "marts", "crawl_mv", fileIDs)

	if err == nil {
		t.Fatal("an oversized ask was accepted")
	}
	if !strings.Contains(err.Error(), "limit is") {
		t.Errorf("error %q does not say what the limit is", err)
	}
}
