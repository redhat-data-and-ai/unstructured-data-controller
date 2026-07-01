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

package gdrive

import (
	"context"
	"io"

	"golang.org/x/sync/singleflight"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/cache"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/google"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/ldap"
)

// Client wraps Google Drive, LDAP, and cache clients to provide
// folder crawling and permission resolution functionality.
type Client struct {
	googleClient google.GoogleClient
	ldapClient   ldap.Client
	cacheClient  cache.Cache
	groupFlight  singleflight.Group
}

// NewClient creates a new gdrive Client with the provided dependencies.
func NewClient(
	googleClient google.GoogleClient,
	ldapClient ldap.Client,
	cacheClient cache.Cache,
) (*Client, error) {
	return &Client{
		googleClient: googleClient,
		ldapClient:   ldapClient,
		cacheClient:  cacheClient,
	}, nil
}

// Close releases resources held by the underlying clients.
func (c *Client) Close() {
	c.googleClient.Close()
}

// DownloadFile downloads the content of a Google Drive file.
// For Google Workspace files (Docs, Sheets, Slides), it exports
// to a portable format. The caller must close the returned
// ReadCloser.
func (c *Client) DownloadFile(
	ctx context.Context,
	fileID string,
	maxRetries int,
) (io.ReadCloser, error) {
	return c.googleClient.DownloadFile(ctx, fileID, maxRetries)
}
