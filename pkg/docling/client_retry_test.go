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

package docling

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/httpclient"
)

// fastRetry retries like production but in milliseconds, so a test can tell a
// retrying client from a plain one.
var fastRetry = httpclient.Policy{RetryMax: 2, WaitMin: time.Millisecond, WaitMax: 2 * time.Millisecond}

func writeStatus(t *testing.T, w http.ResponseWriter, status TaskStatus) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(TaskStatusResponse{TaskID: "task-1", TaskStatus: status}); err != nil {
		t.Errorf("encoding status response: %v", err)
	}
}

// requireSlotFree fails unless the client's single semaphore slot can be taken.
func requireSlotFree(t *testing.T, client *Client) {
	t.Helper()
	require.True(t, client.ClientConfig.sem.TryAcquire(1), "the semaphore slot was not released")
	client.ClientConfig.sem.Release(1)
}

func TestGetConvertedFileMapsOnlyAPoll404ToTaskNotFound(t *testing.T) {
	tests := []struct {
		name           string
		pollStatus     int
		resultStatus   int
		wantNotFound   bool
		wantHTTPStatus int
	}{
		{name: "poll 404 means docling lost the task", pollStatus: http.StatusNotFound, wantNotFound: true},
		{name: "poll 500 is a typed error, not a lost task",
			pollStatus: http.StatusInternalServerError, wantHTTPStatus: http.StatusInternalServerError},
		{name: "result 404 is a typed error, not a lost task",
			pollStatus: http.StatusOK, resultStatus: http.StatusNotFound, wantHTTPStatus: http.StatusNotFound},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				switch {
				case strings.HasPrefix(r.URL.Path, "/v1/status/poll/"):
					if test.pollStatus != http.StatusOK {
						w.WriteHeader(test.pollStatus)
						return
					}
					writeStatus(t, w, TaskStatusSuccess)
				case strings.HasPrefix(r.URL.Path, "/v1/result/"):
					w.WriteHeader(test.resultStatus)
				default:
					t.Errorf("unexpected request to %s", r.URL.Path)
				}
			}))
			defer server.Close()

			client := NewClientFromURL(&ClientConfig{URL: server.URL, MaxConcurrentRequests: 1, RetryPolicy: &noRetry})
			require.True(t, client.ClientConfig.sem.TryAcquire(1), "taking the slot the submit would hold")

			_, _, err := client.GetConvertedFile(context.Background(), "task-1")
			require.Error(t, err)
			assert.Equal(t, test.wantNotFound, errors.Is(err, ErrTaskNotFound))
			if !test.wantNotFound {
				var httpErr *DoclingHTTPError
				require.ErrorAs(t, err, &httpErr)
				assert.Equal(t, test.wantHTTPStatus, httpErr.StatusCode)
			}
			requireSlotFree(t, client)
		})
	}
}

func TestTaskSubmitIsNotRetried(t *testing.T) {
	tests := []struct {
		name   string
		submit func(*Client) error
	}{
		{
			name: "convert",
			submit: func(client *Client) error {
				_, err := client.ConvertFile(context.Background(), "https://bucket.invalid/a.pdf", DoclingConfig{})
				return err
			},
		},
		{
			name: "chunk",
			submit: func(client *Client) error {
				_, err := client.ChunkDoclingDocument(
					context.Background(), "a.json", json.RawMessage(`{"name":"a"}`), validOptions())
				return err
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var posts atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method == http.MethodPost {
					posts.Add(1)
				}
				w.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()

			// A retrying policy, so a submit routed through the wrong client
			// would show up as more than one POST.
			client := NewClientFromURL(&ClientConfig{URL: server.URL, MaxConcurrentRequests: 1, RetryPolicy: &fastRetry})

			err := test.submit(client)
			var httpErr *DoclingHTTPError
			require.ErrorAs(t, err, &httpErr)
			assert.Equal(t, http.StatusServiceUnavailable, httpErr.StatusCode)
			assert.Equal(t, int32(1), posts.Load(), "a task-creating POST must not be retried")
			requireSlotFree(t, client)
		})
	}
}

func TestTaskPollIsRetried(t *testing.T) {
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if polls.Add(1) == 1 {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		writeStatus(t, w, TaskStatusStarted)
	}))
	defer server.Close()

	client := NewClientFromURL(&ClientConfig{URL: server.URL, MaxConcurrentRequests: 1, RetryPolicy: &fastRetry})

	status, _, err := client.GetConvertedFile(context.Background(), "task-1")
	require.NoError(t, err)
	assert.Equal(t, TaskStatusStarted, status)
	assert.Equal(t, int32(2), polls.Load(), "a transient poll failure should be retried")
}

func TestRedactEndpoint(t *testing.T) {
	tests := []struct {
		name     string
		endpoint string
		want     string
	}{
		{name: "credentials are masked", endpoint: "https://user:secret-token@docling.example/v1/result/x",
			want: "https://user:xxxxx@docling.example/v1/result/x"},
		{name: "a plain URL is unchanged", endpoint: "https://docling.example/v1/result/x",
			want: "https://docling.example/v1/result/x"},
		{name: "an unparseable URL is not echoed", endpoint: "http://bad host\x7f/", want: "<unparseable docling URL>"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := redactEndpoint(test.endpoint)
			assert.Equal(t, test.want, got)
			assert.NotContains(t, got, "secret-token")
		})
	}
}

func TestDoDoclingRequestAuthFallback(t *testing.T) {
	tests := []struct {
		name        string
		key         string
		accept      string // Authorization value the server accepts; empty accepts none
		wantErr     bool
		wantHeaders []string
	}{
		{name: "rejected bearer falls back to the raw key", key: "k", accept: "k",
			wantHeaders: []string{"Bearer k", "k"}},
		{name: "accepted bearer needs no fallback", key: "k", accept: "Bearer k",
			wantHeaders: []string{"Bearer k"}},
		{name: "no key means no fallback", key: "", wantErr: true,
			wantHeaders: []string{""}},
		{name: "both formats rejected is a 403", key: "k", wantErr: true,
			wantHeaders: []string{"Bearer k", "k"}},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var headers []string
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				auth := r.Header.Get("Authorization")
				headers = append(headers, auth)
				if test.accept == "" || auth != test.accept {
					w.WriteHeader(http.StatusForbidden)
					return
				}
				w.WriteHeader(http.StatusOK)
			}))
			defer server.Close()

			client := NewClientFromURL(&ClientConfig{
				URL: server.URL, Key: test.key, MaxConcurrentRequests: 1, RetryPolicy: &noRetry,
			})
			clients := map[string]*http.Client{"retrying": client.retryHTTPClient, "plain": client.plainHTTPClient}
			for name, httpClient := range clients {
				headers = nil
				body, err := client.doDoclingRequest(context.Background(), httpClient, http.MethodGet, server.URL, nil)
				if test.wantErr {
					var httpErr *DoclingHTTPError
					require.ErrorAs(t, err, &httpErr, name)
					assert.Equal(t, http.StatusForbidden, httpErr.StatusCode, name)
				} else {
					require.NoError(t, err, name)
					require.NoError(t, body.Close(), name)
				}
				assert.Equal(t, test.wantHeaders, headers, name)
			}
		})
	}
}
