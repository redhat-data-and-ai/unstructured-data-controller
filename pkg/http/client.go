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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	HTTPClientTimeout = 60 * time.Second
)

// CreateHTTPRequest creates an HTTP request with common headers and optional auth
func CreateHTTPRequest(ctx context.Context, method, endpoint string, payload []byte, authFormat,
	apiKey string) (*http.Request, error) {
	var body io.Reader
	if len(payload) > 0 {
		body = bytes.NewReader(payload)
	}

	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	if apiKey != "" && authFormat != "" {
		req.Header.Set("Authorization", fmt.Sprintf("%s %s", authFormat, apiKey))
	}

	return req, nil
}

// Do executes an HTTP request and handles error status codes
func Do(ctx context.Context, client *http.Client, req *http.Request) (int, []byte, error) {
	logger := log.FromContext(ctx)

	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("failed to send request: %w", err)
	}
	defer func() {
		if err := resp.Body.Close(); err != nil {
			logger.Error(err, "failed to close response body")
		}
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return resp.StatusCode, nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return resp.StatusCode, body, &HTTPError{
			StatusCode: resp.StatusCode,
			Body:       body,
		}
	}

	return resp.StatusCode, body, nil
}
