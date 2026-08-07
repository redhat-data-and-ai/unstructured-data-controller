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
	"errors"
	"fmt"
	"io"
	"strings"
	"time"
)

const maxQueryRetries = 3

type ProcessedDocumentResult struct {
	FileID          string `json:"file_id" db:"file_id"`
	MarkdownContent string `json:"markdown_content" db:"markdown_content"`
}

// isRetryableError returns true for network-level errors that indicate a
// Snowflake S3 chunk download connection was dropped (e.g. by an intermediate
// proxy), as opposed to query-level errors that would fail again on retry.
func isRetryableError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return errors.Is(err, io.EOF) ||
		strings.Contains(msg, "EOF") ||
		strings.Contains(msg, "connection reset") ||
		strings.Contains(msg, "broken pipe")
}

// GetProcessedDocument fetches a single document's markdown content from Snowflake.
//
// When MARKDOWN_CONTENT is large, Snowflake stores query results as chunks in S3
// and returns pre-signed URLs to download them. If those URLs consistently fail
// (e.g. the network path to that S3 node is blocked by a proxy), the gosnowflake
// driver's internal chunk-level retries (up to 5) won't help because they reuse
// the same pre-signed URLs. This function retries the entire query with a fresh
// Snowflake connection, which obtains new pre-signed S3 URLs that may route
// through a different network path.
func GetProcessedDocument(
	ctx context.Context, oauthToken, database, schema, table, fileID string,
) (*ProcessedDocumentResult, error) {
	var lastErr error
	for attempt := range maxQueryRetries {
		result, err := queryProcessedDocument(ctx, oauthToken, database, schema, table, fileID)
		if err == nil {
			return result, nil
		}
		lastErr = err
		if !isRetryableError(err) {
			return nil, err
		}
		if attempt < maxQueryRetries-1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(time.Duration(attempt+1) * 3 * time.Second):
			}
		}
	}
	return nil, fmt.Errorf("failed after %d attempts: %w", maxQueryRetries, lastErr)
}

func queryProcessedDocument(
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
