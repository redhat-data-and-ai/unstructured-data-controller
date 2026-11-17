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
	"math/rand/v2"
	"net/http"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// Default retry settings for external service calls.
const (
	DefaultRetryMax         = 7
	DefaultRetryInitialWait = 1 * time.Second
	DefaultRetryMaxWait     = 2 * time.Minute
)

// Policy sets how many times a request is retried and the bounds on the wait
// between attempts.
type Policy struct {
	RetryMax int
	WaitMin  time.Duration
	WaitMax  time.Duration
}

// DefaultPolicy returns the policy NewWithRetry uses.
func DefaultPolicy() Policy {
	return Policy{RetryMax: DefaultRetryMax, WaitMin: DefaultRetryInitialWait, WaitMax: DefaultRetryMaxWait}
}

// NewWithRetry is New with DefaultPolicy.
func NewWithRetry(timeout time.Duration) *http.Client {
	return New(timeout, DefaultPolicy())
}

// New returns an *http.Client that retries transient failures (429, 5xx) under
// policy. timeout applies to each attempt, not to the retries as a whole.
//
// Once retries run out the final response is returned rather than an error,
// so callers read its status and body exactly as they would without retry.
func New(timeout time.Duration, policy Policy) *http.Client {
	retryClient := retryablehttp.NewClient()
	retryClient.RetryMax = policy.RetryMax
	retryClient.RetryWaitMin = policy.WaitMin
	retryClient.RetryWaitMax = policy.WaitMax
	retryClient.Backoff = cappedJitterBackoff
	retryClient.ErrorHandler = retryablehttp.PassthroughErrorHandler
	// Callers log through controller-runtime's logger.
	retryClient.Logger = nil
	retryClient.HTTPClient.Timeout = timeout
	return retryClient.StandardClient()
}

// cappedJitterBackoff is exponential backoff with up to 50% added jitter, so
// concurrent reconcilers do not retry in lockstep. The cap is applied last,
// because the library's own backoff honours Retry-After without one.
func cappedJitterBackoff(minWait, maxWait time.Duration, attempt int, resp *http.Response) time.Duration {
	wait := retryablehttp.DefaultBackoff(minWait, maxWait, attempt, resp)
	if wait > 0 {
		wait += time.Duration(rand.Int64N(int64(wait)/2 + 1))
	}
	return min(wait, maxWait)
}
