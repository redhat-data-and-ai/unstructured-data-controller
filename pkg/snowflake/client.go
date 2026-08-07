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
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/snowflakedb/gosnowflake"
)

const maxQueryRetries = 3

func openConnection(oauthToken string) (*sql.DB, error) {
	account := os.Getenv("SNOWFLAKE_ACCOUNT")
	if account == "" {
		return nil, errors.New("SNOWFLAKE_ACCOUNT environment variable not set")
	}

	warehouse := os.Getenv("SNOWFLAKE_WAREHOUSE")
	if warehouse == "" {
		warehouse = "DEFAULT"
	}

	cfg := &gosnowflake.Config{
		Account:       account,
		Authenticator: gosnowflake.AuthTypeOAuth,
		Token:         oauthToken,
		Role:          "PUBLIC",
		Warehouse:     warehouse,
		OCSPFailOpen:  gosnowflake.OCSPFailOpenTrue,
	}

	dsn, err := gosnowflake.DSN(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build snowflake DSN: %w", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create snowflake connection: %w", err)
	}

	return db, nil
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

// queryRows executes a Snowflake query and scans the results into a slice of T.
//
// When query results are large, Snowflake stores them as chunks in S3 and returns
// pre-signed URLs to download them. If those URLs consistently fail (e.g. the
// network path to that S3 node is blocked by a proxy), the gosnowflake driver's
// internal chunk-level retries (up to 5) won't help because they reuse the same
// pre-signed URLs. This function retries the entire query with a fresh Snowflake
// connection, which obtains new pre-signed S3 URLs that may route through a
// different network path.
func queryRows[T any](ctx context.Context, oauthToken, query string, args ...any) ([]T, error) {
	var lastErr error
	for attempt := range maxQueryRetries {
		results, err := executeQuery[T](ctx, oauthToken, query, args...)
		if err == nil {
			return results, nil
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

func executeQuery[T any](ctx context.Context, oauthToken, query string, args ...any) ([]T, error) {
	db, err := openConnection(oauthToken)
	if err != nil {
		return nil, err
	}
	defer func() { _ = db.Close() }()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer func() { _ = rows.Close() }()

	return scanRows[T](rows)
}
