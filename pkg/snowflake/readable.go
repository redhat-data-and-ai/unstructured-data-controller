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
	"fmt"
	"strings"
)

// maxReadableFileIDs bounds one FilterReadableFiles call. Snowflake accepts
// far more, but a caller asking about thousands of files at once is paging
// through a corpus rather than filtering a page of search results.
const maxReadableFileIDs = 1000

// ReadableFile is one file the caller is allowed to read, with where the
// crawler stored it.
type ReadableFile struct {
	FileID string `json:"file_id" db:"file_id"`
	// FilePath is the crawler's path for the stored copy, which locates the
	// object in the destination bucket. Empty only for a row written before
	// the sidecar carried one.
	FilePath string `json:"file_path" db:"file_path"`
}

// FilterReadableFiles returns the subset of fileIDs the holder of oauthToken is
// allowed to read, according to the row access policy on the given table.
//
// The query runs as the caller, so Snowflake drops rows their policy excludes
// before we ever see them. That makes this the access control decision and not
// a convenience filter: a caller must never be shown a file this omits. An
// error means the decision could not be made, never that nothing matched.
//
// The session is qualified with database and schema because the policy compares
// CURRENT_SCHEMA(), and an unqualified session matches nothing at all.
//
// Returns nil for an empty input without querying.
func FilterReadableFiles(
	ctx context.Context, oauthToken, database, schema, table string, fileIDs []string,
) ([]ReadableFile, error) {
	if len(fileIDs) == 0 {
		return nil, nil
	}
	if len(fileIDs) > maxReadableFileIDs {
		return nil, fmt.Errorf("asked about %d files, limit is %d", len(fileIDs), maxReadableFileIDs)
	}

	// Placeholders rather than interpolation: a Drive file ID reaches us from
	// the Drive API, but the surrounding query names a table we built from
	// config, and mixing the two styles is how injection gets in later.
	placeholders := strings.TrimSuffix(strings.Repeat("?,", len(fileIDs)), ",")
	query := fmt.Sprintf(
		`SELECT FILE_ID, FILE_PATH FROM %s.%s.%s WHERE FILE_ID IN (%s)`,
		database, schema, table, placeholders,
	)

	args := make([]any, 0, len(fileIDs))
	for _, fileID := range fileIDs {
		args = append(args, fileID)
	}

	rows, err := queryRowsIn[ReadableFile](ctx, oauthToken, database, schema, query, args...)
	if err != nil {
		return nil, fmt.Errorf("checking which files the caller may read: %w", err)
	}

	// A rewritten file has more than one row, and the query has no ORDER BY,
	// so collapse to one. Which row wins only decides the download path, and
	// every row for an ID names the same object.
	seen := make(map[string]bool, len(rows))
	readable := make([]ReadableFile, 0, len(rows))
	for _, row := range rows {
		if seen[row.FileID] {
			continue
		}
		seen[row.FileID] = true
		readable = append(readable, row)
	}
	return readable, nil
}
