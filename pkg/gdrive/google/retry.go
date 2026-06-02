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

package google

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"

	"google.golang.org/api/googleapi"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// withRetry retries the operation with exponential backoff on
// retryable errors (429, 5xx). It respects the Retry-After header.
func withRetry(
	ctx context.Context,
	maxRetries int,
	operation string,
	fn func() error,
) error {
	logger := log.FromContext(ctx)

	var lastErr error
	for attempt := 0; attempt <= maxRetries; attempt++ {
		err := fn()
		if err == nil {
			return nil
		}
		lastErr = err

		if attempt == maxRetries {
			break
		}

		if !isRetryable(err) {
			return err
		}

		delay := retryDelay(err, attempt)
		logger.Info("retryable error, backing off",
			"attempt", attempt+1,
			"maxRetries", maxRetries,
			"delay", delay,
			"operation", operation,
			"error", err,
		)

		timer := time.NewTimer(delay)
		select {
		case <-timer.C:
		case <-ctx.Done():
			timer.Stop()
			return fmt.Errorf(
				"%s: context cancelled during retry: %w",
				operation, ctx.Err(),
			)
		}
	}

	return fmt.Errorf(
		"%s: exhausted %d retries: %w",
		operation, maxRetries, lastErr,
	)
}

// isRetryable returns true for HTTP 429 and 5xx errors.
func isRetryable(err error) bool {
	var apiErr *googleapi.Error
	if !errors.As(err, &apiErr) {
		return false
	}
	if apiErr.Code == 429 {
		return true
	}
	return apiErr.Code >= 500 && apiErr.Code < 600
}

// retryDelay computes the backoff, respecting Retry-After if present.
func retryDelay(err error, attempt int) time.Duration {
	var apiErr *googleapi.Error
	if errors.As(err, &apiErr) && apiErr.Code == 429 {
		if ra := apiErr.Header.Get("Retry-After"); ra != "" {
			if secs, e := strconv.Atoi(ra); e == nil {
				return time.Duration(secs) * time.Second
			}
		}
	}
	// Exponential backoff: 1s, 2s, 4s, 8s, ...
	return time.Duration(math.Pow(2, float64(attempt))) * time.Second
}
