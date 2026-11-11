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
	"fmt"
	"time"

	drive "google.golang.org/api/drive/v3"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// searchFields is the metadata one search result carries. No ACL: who may see
// a result is decided by the Snowflake row access policy, not by Drive.
const searchFields = "incompleteSearch,nextPageToken," +
	"files(id,name,mimeType,modifiedTime,webViewLink)"

// SearchResult is one page of a Drive search.
type SearchResult struct {
	Files []*drive.File
	// NextPageToken is empty on the last page. A caller that filters results
	// after the fact has to follow this, or it reports "nothing found" when it
	// really means "nothing on the first page".
	NextPageToken string
	// Incomplete reports Drive's incompleteSearch flag, meaning it did not
	// search everything and results may be missing whatever the caller does.
	Incomplete bool
}

// SearchFiles runs one page of a Drive query. pageToken is empty for the first
// page, and NextPageToken comes back empty on the last.
//
// query reaches Drive verbatim, so escape caller-supplied text with
// gdrive.EscapeQueryTerm first.
func (c *Client) SearchFiles(
	ctx context.Context,
	query string,
	pageSize int,
	pageToken string,
	maxRetries int,
) (*SearchResult, error) {
	logger := log.FromContext(ctx)
	start := time.Now()

	var fileList *drive.FileList
	err := withRetry(ctx, maxRetries, "SearchFiles", func() error {
		call := c.driveService.Files.List().
			Context(ctx).
			Q(query).
			Fields(searchFields).
			SupportsAllDrives(true).
			IncludeItemsFromAllDrives(true).
			PageSize(int64(pageSize))
		if pageToken != "" {
			call = call.PageToken(pageToken)
		}
		var callErr error
		fileList, callErr = call.Do()
		return callErr
	})
	if err != nil {
		return nil, fmt.Errorf("drive search failed: %w", err)
	}

	logger.V(1).Info("drive search page complete",
		"results", len(fileList.Files),
		"hasMore", fileList.NextPageToken != "",
		"incompleteSearch", fileList.IncompleteSearch,
		"timeElapsed", time.Since(start),
	)

	return &SearchResult{
		Files:         fileList.Files,
		NextPageToken: fileList.NextPageToken,
		Incomplete:    fileList.IncompleteSearch,
	}, nil
}
