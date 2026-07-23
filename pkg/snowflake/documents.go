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
)

type ProcessedDocumentResult struct {
	FileID          string `json:"file_id" db:"file_id"`
	MarkdownContent string `json:"markdown_content" db:"markdown_content"`
}

func GetProcessedDocument(
	ctx context.Context, oauthToken, database, schema, table, fileID string,
) (*ProcessedDocumentResult, error) {
	db, err := openConnection(oauthToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	query := fmt.Sprintf(
		`SELECT FILE_ID, MARKDOWN_CONTENT FROM %s.%s.%s WHERE FILE_ID = ? LIMIT 1`,
		database, schema, table,
	)

	rows, err := db.QueryContext(ctx, query, fileID)
	if err != nil {
		return nil, fmt.Errorf("failed to query processed document: %w", err)
	}
	defer func() { _ = rows.Close() }()

	results, err := scanRows[ProcessedDocumentResult](rows)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, fmt.Errorf("no processed document found for file_id %q", fileID)
	}
	return &results[0], nil
}
