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
	"strconv"
)

type ProcessedDocumentResult struct {
	FileID          string `json:"file_id" db:"file_id"`
	MarkdownContent string `json:"markdown_content" db:"markdown_content"`
}

func GetProcessedDocument(
	ctx context.Context, oauthToken, database, schema, table, fileID string,
) (*ProcessedDocumentResult, error) {
	query := fmt.Sprintf(
		`SELECT FILE_ID, MARKDOWN_CONTENT FROM %s.%s.%s WHERE FILE_ID = ? LIMIT 1`,
		database, schema, table,
	)

	results, err := queryRows[ProcessedDocumentResult](ctx, oauthToken, query, fileID)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no processed document found for file_id %q", fileID)
	}
	return &results[0], nil
}

// CountProcessedDocumentsByFilePath returns the number of documents matching the given LIKE pattern.
func CountProcessedDocumentsByFilePath(
	ctx context.Context, oauthToken, database, schema, table, filePattern string,
) (int, error) {
	query := fmt.Sprintf(
		`SELECT COUNT(*) AS count FROM %s.%s.%s WHERE FILE_ID LIKE ? ESCAPE '\\'`,
		database, schema, table,
	)
	type countResult struct {
		Count string `db:"count"`
	}
	results, err := queryRows[countResult](ctx, oauthToken, query, filePattern)
	if err != nil {
		return 0, err
	}
	if len(results) == 0 {
		return 0, nil
	}
	return strconv.Atoi(results[0].Count)
}

// ListFileIDsByFilePath returns just the file_id values matching the given LIKE pattern.
func ListFileIDsByFilePath(
	ctx context.Context, oauthToken, database, schema, table, filePattern string,
) ([]string, error) {
	query := fmt.Sprintf(
		`SELECT FILE_ID FROM %s.%s.%s WHERE FILE_ID LIKE ? ESCAPE '\\'`,
		database, schema, table,
	)
	type fileIDResult struct {
		FileID string `db:"file_id"`
	}
	results, err := queryRows[fileIDResult](ctx, oauthToken, query, filePattern)
	if err != nil {
		return nil, err
	}
	ids := make([]string, len(results))
	for i, r := range results {
		ids[i] = r.FileID
	}
	return ids, nil
}

// GetProcessedDocumentsByFilePath retrieves processed documents matching
// the given LIKE pattern, up to the specified limit.
func GetProcessedDocumentsByFilePath(
	ctx context.Context, oauthToken, database, schema, table, filePattern string, limit int,
) ([]ProcessedDocumentResult, error) {
	query := fmt.Sprintf(
		`SELECT FILE_ID, MARKDOWN_CONTENT FROM %s.%s.%s WHERE FILE_ID LIKE ? ESCAPE '\\' LIMIT %d`,
		database, schema, table, limit,
	)

	return queryRows[ProcessedDocumentResult](ctx, oauthToken, query, filePattern)
}
