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
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/hashicorp/go-retryablehttp"
)

// fastPolicy keeps retry tests to milliseconds.
var fastPolicy = Policy{RetryMax: 2, WaitMin: time.Millisecond, WaitMax: 5 * time.Millisecond}

func TestPolicyOrDefault(t *testing.T) {
	tests := []struct {
		name   string
		policy *Policy
		want   Policy
	}{
		{name: "nil falls back to the default", policy: nil, want: DefaultPolicy()},
		{name: "an explicit policy is used as given", policy: &fastPolicy, want: fastPolicy},
		{name: "an explicit zero policy means no retries, not the default", policy: &Policy{}, want: Policy{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := PolicyOrDefault(test.policy); got != test.want {
				t.Errorf("PolicyOrDefault = %+v, want %+v", got, test.want)
			}
		})
	}
}

func TestNewSharesOneTransport(t *testing.T) {
	transportOf := func(client *http.Client) http.RoundTripper {
		roundTripper, ok := client.Transport.(*retryablehttp.RoundTripper)
		if !ok {
			t.Fatalf("Transport is %T, want *retryablehttp.RoundTripper", client.Transport)
		}
		return roundTripper.Client.HTTPClient.Transport
	}

	first, second := transportOf(New(time.Second, fastPolicy)), transportOf(New(time.Minute, DefaultPolicy()))
	if first != second {
		t.Error("clients built separately do not share a transport, so connections are not reused")
	}
}

func TestNewDoesNotRetryATimedOutAttempt(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	resp, err := New(20*time.Millisecond, fastPolicy).Get(server.URL)
	if err == nil {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Errorf("closing response body: %v", closeErr)
		}
		t.Fatal("Get returned no error, want the attempt's timeout")
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1: a timed-out request must not be sent again", got)
	}
}

func TestNewRetries(t *testing.T) {
	tests := []struct {
		name         string
		statuses     []int // served in order; the last repeats
		wantStatus   int
		wantAttempts int32
	}{
		{
			name:         "transient failure then success returns the success",
			statuses:     []int{http.StatusServiceUnavailable, http.StatusOK},
			wantStatus:   http.StatusOK,
			wantAttempts: 2,
		},
		{
			name:         "exhausted retries return the final response, not an error",
			statuses:     []int{http.StatusTooManyRequests},
			wantStatus:   http.StatusTooManyRequests,
			wantAttempts: int32(fastPolicy.RetryMax + 1),
		},
		{
			name:         "non-retryable client error is returned without retrying",
			statuses:     []int{http.StatusBadRequest},
			wantStatus:   http.StatusBadRequest,
			wantAttempts: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var attempts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				served := int(attempts.Add(1)) - 1
				w.WriteHeader(test.statuses[min(served, len(test.statuses)-1)])
			}))
			defer server.Close()

			resp, err := New(time.Second, fastPolicy).Get(server.URL)
			if err != nil {
				t.Fatalf("Get returned error %v, want the final response", err)
			}
			defer func() {
				if closeErr := resp.Body.Close(); closeErr != nil {
					t.Errorf("closing response body: %v", closeErr)
				}
			}()

			if resp.StatusCode != test.wantStatus {
				t.Errorf("status = %d, want %d", resp.StatusCode, test.wantStatus)
			}
			if got := attempts.Load(); got != test.wantAttempts {
				t.Errorf("attempts = %d, want %d", got, test.wantAttempts)
			}
		})
	}
}

func TestNewReturnsTransportFailureAsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	url := server.URL
	server.Close()

	resp, err := New(time.Second, fastPolicy).Get(url)
	if err == nil {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Errorf("closing response body: %v", closeErr)
		}
		t.Fatal("Get against a closed server returned no error")
	}
}

func TestCappedJitterBackoff(t *testing.T) {
	const (
		minWait = time.Second
		maxWait = 2 * time.Minute
	)
	retryAfter := func(seconds string) *http.Response {
		return &http.Response{
			StatusCode: http.StatusTooManyRequests,
			Header:     http.Header{"Retry-After": []string{seconds}},
		}
	}

	tests := []struct {
		name    string
		attempt int
		resp    *http.Response
		atLeast time.Duration
		atMost  time.Duration
	}{
		{
			name:    "first retry waits the base plus up to half again",
			attempt: 0,
			atLeast: minWait,
			atMost:  minWait + minWait/2,
		},
		{
			name:    "growth stops at the cap",
			attempt: 20,
			atLeast: maxWait,
			atMost:  maxWait,
		},
		{
			name:    "a Retry-After beyond the cap is clamped to it",
			attempt: 0,
			resp:    retryAfter("3600"),
			atLeast: maxWait,
			atMost:  maxWait,
		},
		{
			name:    "a Retry-After under the cap is honoured",
			attempt: 0,
			resp:    retryAfter("10"),
			atLeast: 10 * time.Second,
			atMost:  15 * time.Second,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			// Jitter is random, so sample enough to catch an out-of-range draw.
			for range 200 {
				got := cappedJitterBackoff(minWait, maxWait, test.attempt, test.resp)
				if got < test.atLeast || got > test.atMost {
					t.Fatalf("backoff = %v, want within [%v, %v]", got, test.atLeast, test.atMost)
				}
			}
		})
	}
}

func TestNewStopsOnAStalledErrorBody(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		time.Sleep(200 * time.Millisecond)
	}))
	defer server.Close()

	resp, err := New(50*time.Millisecond, fastPolicy).Get(server.URL)
	if err == nil {
		if closeErr := resp.Body.Close(); closeErr != nil {
			t.Errorf("closing response body: %v", closeErr)
		}
		t.Fatal("Get returned no error, want the attempt's timeout")
	}
	if got := attempts.Load(); got != 1 {
		t.Errorf("attempts = %d, want 1: a 503 whose body stalled past the timeout must not be retried", got)
	}
}

func TestNewKeepsTheFinalErrorBody(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		attempts.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		if _, err := w.Write([]byte("try later")); err != nil {
			t.Errorf("writing body: %v", err)
		}
	}))
	defer server.Close()

	resp, err := New(time.Second, fastPolicy).Get(server.URL)
	if err != nil {
		t.Fatalf("Get returned error %v, want the final response", err)
	}
	body, readErr := io.ReadAll(resp.Body)
	if closeErr := resp.Body.Close(); closeErr != nil {
		t.Errorf("closing response body: %v", closeErr)
	}
	if readErr != nil {
		t.Fatalf("reading body: %v", readErr)
	}
	if string(body) != "try later" {
		t.Errorf("body = %q, want the server's error body", body)
	}
	if got := attempts.Load(); got != int32(fastPolicy.RetryMax+1) {
		t.Errorf("attempts = %d, want %d: a complete 503 is still retried", got, fastPolicy.RetryMax+1)
	}
}
