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
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

const (
	// Default retry configuration for external service calls.
	// Retries up to 7 times with exponential backoff (1s to 2min),
	// then fails so the reconcile requeue can take over.
	DefaultRetryMax         = 7
	DefaultRetryInitialWait = 1 * time.Second
	DefaultRetryMaxWait     = 2 * time.Minute
)

// NewWithRetry creates an *http.Client that automatically retries on transient
// HTTP errors (429, 5xx) using hashicorp/go-retryablehttp. The timeout applies
// to each individual attempt, not the total time across all retries.
//
// Total wall time across all retries is bounded by:
// (RetryMax+1)*timeout + sum(backoff waits). With defaults and a 2min timeout,
// worst case is 8*120s + 127s backoff = ~18min. This is acceptable because
// controller-runtime will cancel the context on shutdown, and the reconcile
// will be retried with backoff if the external service is persistently down.
func NewWithRetry(timeout time.Duration) *http.Client {
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = DefaultRetryMax
	retryClient.RetryWaitMin = DefaultRetryInitialWait
	retryClient.RetryWaitMax = DefaultRetryMaxWait
	// Suppress go-retryablehttp's default logger. The controller already logs
	// via controller-runtime's structured logger.
	retryClient.Logger = nil
	retryClient.HTTPClient.Timeout = timeout
	return retryClient.StandardClient()
}
