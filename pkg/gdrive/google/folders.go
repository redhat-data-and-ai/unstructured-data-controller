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
	"strings"
	"time"

	drive "google.golang.org/api/drive/v3"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ListFolderContents lists all items in a folder with full
// metadata. Uses pagination and retries on transient errors.
func (c *Client) ListFolderContents(
	ctx context.Context,
	folderID string,
	maxRetries int,
) ([]*drive.File, error) {
	logger := log.FromContext(ctx).WithValues("folderID", folderID)
	start := time.Now()

	logger.V(1).Info("listing folder contents")

	var allFiles []*drive.File
	query := fmt.Sprintf(
		"'%s' in parents and trashed=false", folderID)
	pageToken := ""

	for {
		var fileList *drive.FileList
		currentPageToken := pageToken

		err := withRetry(ctx, maxRetries,
			fmt.Sprintf("ListFolderContents(%s)", folderID),
			func() error {
				call := c.driveService.Files.List().
					Context(ctx).
					Q(query).
					Fields("nextPageToken, files(id, name, " +
						"mimeType, createdTime, modifiedTime, " +
						"owners(emailAddress), size, " +
						"shortcutDetails)").
					SupportsAllDrives(true).
					IncludeItemsFromAllDrives(true).
					PageSize(1000)

				if currentPageToken != "" {
					call = call.PageToken(currentPageToken)
				}

				var callErr error
				fileList, callErr = call.Do()
				return callErr
			})

		if err != nil {
			logger.Error(err, "failed to list folder contents",
				"timeElapsed", time.Since(start),
			)
			return nil, fmt.Errorf(
				"failed to list folder %s: %w", folderID, err)
		}

		allFiles = append(allFiles, fileList.Files...)

		pageToken = fileList.NextPageToken
		if pageToken == "" {
			break
		}
	}

	logger.V(1).Info("folder contents listed",
		"itemCount", len(allFiles),
		"timeElapsed", time.Since(start),
	)

	return allFiles, nil
}

// ListSubfolders returns the folders directly inside folderID, following
// pagination. Names come back so the caller can apply a skip list.
// escapeQueryValue makes a folder ID safe inside a single-quoted query term.
//
// Duplicated from gdrive.EscapeQueryTerm rather than imported, because this
// package is below that one. An apostrophe here would close the term and
// detach the folder scope.
func escapeQueryValue(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	return strings.ReplaceAll(value, `'`, `\'`)
}

func (c *Client) ListSubfolders(
	ctx context.Context,
	folderID string,
	maxRetries int,
) ([]*drive.File, error) {
	query := fmt.Sprintf(
		"'%s' in parents and trashed=false and mimeType = 'application/vnd.google-apps.folder'",
		escapeQueryValue(folderID))

	var all []*drive.File
	pageToken := ""

	for {
		var page *drive.FileList
		currentPageToken := pageToken

		err := withRetry(ctx, maxRetries, fmt.Sprintf("ListSubfolders(%s)", folderID), func() error {
			call := c.driveService.Files.List().
				Context(ctx).
				Q(query).
				Fields("nextPageToken, files(id, name)").
				SupportsAllDrives(true).
				IncludeItemsFromAllDrives(true).
				PageSize(1000)
			if currentPageToken != "" {
				call = call.PageToken(currentPageToken)
			}
			var callErr error
			page, callErr = call.Do()
			return callErr
		})
		if err != nil {
			return nil, fmt.Errorf("listing subfolders of %s: %w", folderID, err)
		}

		all = append(all, page.Files...)
		pageToken = page.NextPageToken
		if pageToken == "" {
			return all, nil
		}
	}
}

// FolderName returns a folder's name, used to match a declared root against
// the skip list.
func (c *Client) FolderName(ctx context.Context, folderID string, maxRetries int) (string, error) {
	var file *drive.File
	err := withRetry(ctx, maxRetries, fmt.Sprintf("FolderName(%s)", folderID), func() error {
		var callErr error
		file, callErr = c.driveService.Files.Get(folderID).
			Context(ctx).
			Fields("name").
			SupportsAllDrives(true).
			Do()
		return callErr
	})
	if err != nil {
		return "", fmt.Errorf("getting name of folder %s: %w", folderID, err)
	}
	return file.Name, nil
}

// IsFolderAccessible checks if a folder can be accessed by the
// service account.
func (c *Client) IsFolderAccessible(
	ctx context.Context,
	folderID string,
) bool {
	_, err := c.driveService.Files.Get(folderID).
		Context(ctx).
		Fields("id").
		SupportsAllDrives(true).
		Do()
	return err == nil
}
