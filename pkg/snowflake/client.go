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
	msg := strings.ToLower(err.Error())
	return errors.Is(err, io.EOF) ||
		// wrapped EOF from gosnowflake chunk downloader error chains (also covers "unexpected EOF")
		strings.Contains(msg, "eof") ||
		// TCP RST from server or intermediate proxy
		strings.Contains(msg, "connection reset") ||
		// write to a connection closed by the remote side
		strings.Contains(msg, "broken pipe") ||
		// stale connection from the pool reused after server closed it
		strings.Contains(msg, "use of closed network connection") ||
		// TCP/TLS dial or read deadline exceeded
		strings.Contains(msg, "i/o timeout") ||
		// transient DNS or TCP connect failure
		strings.Contains(msg, "connection refused") ||
		// HTTP/2 GOAWAY from server or proxy during download
		strings.Contains(msg, "server closed connection") ||
		// gosnowflake retryHTTP exhausted its own retries on 5xx or timeout
		strings.Contains(msg, "hanging?")
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
		// each attempt opens a fresh connection, which gets new pre-signed S3 URLs
		results, err := executeQuery[T](ctx, oauthToken, query, args...)
		if err == nil {
			return results, nil
		}
		lastErr = err
		// non-network errors (e.g. syntax, auth) won't benefit from retry
		if !isRetryableError(err) {
			return nil, err
		}
		// backoff before next attempt: 3s, 6s
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

// executeQuery runs a single query attempt: open connection, query, scan, close.
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
