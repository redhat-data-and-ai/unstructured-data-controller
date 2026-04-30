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

package http

import (
	"errors"
	"fmt"
	"net/http"
)

// HTTPError represents any non-200 HTTP response
type HTTPError struct {
	StatusCode int
	Body       []byte
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("HTTP %d: %s", e.StatusCode, string(e.Body))
}

// Status code 413 - Batch size error (embeddings)
func IsStatusPayloadTooLarge(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusRequestEntityTooLarge
	}
	return false
}

// Status code 422 - Tokenization error (embeddings) OR Validation error (docling)
func IsStatusUnprocessableEntity(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusUnprocessableEntity
	}
	return false
}

// Status code 424 - Embedding error / Inference failed (embeddings)
func IsStatusFailedDependency(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusFailedDependency
	}
	return false
}

// Status code 429 - Rate limit / Model overloaded
func IsStatusTooManyRequests(err error) bool {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.StatusCode == http.StatusTooManyRequests
	}
	return false
}
