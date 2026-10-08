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

package httpclient

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ExternalServiceBackoff defines capped exponential backoff for external HTTP
// service calls (docling, embedding, etc.). Retries up to 7 times with
// exponential growth (6 waits between attempts, ~63s total), then fails
// so the reconcile requeue can take over.
var ExternalServiceBackoff = wait.Backoff{
	Duration: 1 * time.Second,
	Factor:   2.0,
	Cap:      2 * time.Minute,
	Steps:    7,   // max 7 retries, then fail
	Jitter:   0.2, // 20% jitter to avoid thundering herd
}

// RetryableHTTPError represents a transient HTTP error that can be retried.
type RetryableHTTPError struct {
	StatusCode int
}

func (e *RetryableHTTPError) Error() string {
	return fmt.Sprintf("retryable HTTP status %d: %s", e.StatusCode, http.StatusText(e.StatusCode))
}

// IsRetryableHTTPError returns true for transient HTTP errors that are worth
// retrying: rate limits (429) and server errors (500, 502, 503, 504).
func IsRetryableHTTPError(err error) bool {
	var retryable *RetryableHTTPError
	if !errors.As(err, &retryable) {
		return false
	}
	switch retryable.StatusCode {
	case http.StatusTooManyRequests,
		http.StatusInternalServerError,
		http.StatusBadGateway,
		http.StatusServiceUnavailable,
		http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

// RetryWithContext retries fn with capped exponential backoff, respecting
// context cancellation. Unlike retry.OnError which uses time.Sleep,
// ExponentialBackoffWithContext interrupts the backoff sleep when the context
// is cancelled — preventing a controller worker from being blocked during shutdown.
func RetryWithContext(ctx context.Context, backoff wait.Backoff, isRetryable func(error) bool, fn func() error) error {
	return wait.ExponentialBackoffWithContext(ctx, backoff, func(_ context.Context) (bool, error) {
		err := fn()
		if err != nil {
			if isRetryable(err) {
				return false, nil
			}
			return false, err
		}
		return true, nil
	})
}

// RetryTransport wraps an http.RoundTripper with automatic retry on transient
// HTTP errors (429, 5xx) using capped exponential backoff. Use it as the
// Transport on any http.Client to get retry behavior transparently.
type RetryTransport struct {
	Base http.RoundTripper
}

// NewRetryTransport creates a RetryTransport wrapping the given base transport.
func NewRetryTransport(base http.RoundTripper) *RetryTransport {
	return &RetryTransport{Base: base}
}

func (t *RetryTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	// Reject requests with non-replayable bodies upfront. If req.Body is set
	// but GetBody is nil, we cannot reset the body for retries, which would
	// cause POST/PUT retries to send empty payloads silently.
	if req.Body != nil && req.GetBody == nil {
		return nil, errors.New("RetryTransport requires a replayable body (GetBody must be set)")
	}

	var resp *http.Response
	err := RetryWithContext(req.Context(), ExternalServiceBackoff, IsRetryableHTTPError, func() error {
		// Reset the request body for retries so POST/PUT don't send empty bodies.
		if req.GetBody != nil {
			body, bodyErr := req.GetBody()
			if bodyErr != nil {
				return fmt.Errorf("failed to reset request body for retry: %w", bodyErr)
			}
			req.Body = body
		}

		var reqErr error
		resp, reqErr = t.Base.RoundTrip(req)
		if reqErr != nil {
			return reqErr
		}
		if retryableErr := CheckResponseForRetryableError(resp.StatusCode); retryableErr != nil {
			// Drain up to 64KB before closing so the HTTP/1.x transport can
			// reuse the connection instead of opening a new one.
			logger := log.FromContext(req.Context())
			if _, drainErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 64*1024)); drainErr != nil {
				logger.Error(drainErr, "failed to drain response body before retry")
			}
			if closeErr := resp.Body.Close(); closeErr != nil {
				logger.Error(closeErr, "failed to close response body before retry")
			}
			resp = nil
			return retryableErr
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return resp, nil
}

// CheckResponseForRetryableError returns a RetryableHTTPError if the HTTP
// status code indicates a transient failure, or nil if the response is OK
// or a non-retryable error.
func CheckResponseForRetryableError(statusCode int) error {
	if statusCode >= 200 && statusCode < 300 {
		return nil
	}
	retryableErr := &RetryableHTTPError{StatusCode: statusCode}
	if IsRetryableHTTPError(retryableErr) {
		return retryableErr
	}
	return nil
}
