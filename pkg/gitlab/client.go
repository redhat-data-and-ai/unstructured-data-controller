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

package gitlab

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client provides access to the GitLab REST API for project member lookups.
type Client struct {
	baseURL    string
	token      string
	httpClient *http.Client
}

// NewClient creates a GitLab API client.
// baseURL is the GitLab instance root (e.g. "https://gitlab.com").
// token is a Personal Access Token with read_api scope.
func NewClient(baseURL, token string) *Client {
	return &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		token:   token,
		httpClient: &http.Client{
			Timeout: 30 * time.Second,
		},
	}
}

// BaseURLFromRepoURL extracts the GitLab instance base URL from a repository URL.
// "https://gitlab.com/group/project.git" returns "https://gitlab.com".
func BaseURLFromRepoURL(repoURL string) (string, error) {
	u, err := url.Parse(repoURL)
	if err != nil {
		return "", fmt.Errorf("invalid repo URL %q: %w", repoURL, err)
	}
	if u.Scheme == "" || u.Host == "" {
		return "", fmt.Errorf("repo URL %q missing scheme or host", repoURL)
	}
	return fmt.Sprintf("%s://%s", u.Scheme, u.Host), nil
}

// doRequest executes an authenticated GET against the GitLab v4 API.
func (c *Client) doRequest(ctx context.Context, endpoint string) (*http.Response, error) {
	reqURL := c.baseURL + "/api/v4" + endpoint
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("PRIVATE-TOKEN", c.token)
	return c.httpClient.Do(req)
}

// Close releases idle HTTP connections.
func (c *Client) Close() {
	c.httpClient.CloseIdleConnections()
}
