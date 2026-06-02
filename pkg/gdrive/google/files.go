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
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	drive "google.golang.org/api/drive/v3"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// googleWorkspaceExportURLs maps native Google Workspace MIME
// types to their direct export URL templates. These bypass the
// 10 MB limit of the Files.Export API.
var googleWorkspaceExportURLs = map[string]string{
	"application/vnd.google-apps.document": "" +
		"https://docs.google.com/document/d/%s/export?format=pdf",
	"application/vnd.google-apps.spreadsheet": "" +
		"https://docs.google.com/spreadsheets/d/%s/export?format=pdf",
	"application/vnd.google-apps.presentation": "" +
		"https://docs.google.com/presentation/d/%s/export?format=pdf",
	"application/vnd.google-apps.drawing": "" +
		"https://docs.google.com/drawings/d/%s/export?format=pdf",
}

// workspaceCompatibleFormats lists uploaded MIME types that can
// be exported as PDF via Google Docs editor URLs. Only Office
// and OpenDocument formats are supported — text/csv, images,
// and other importable formats return HTTP 400 from the export
// endpoint even though Drive's importFormats claims support.
var workspaceCompatibleFormats = map[string]bool{
	// Microsoft Office
	"application/msword":            true, // .doc
	"application/vnd.ms-excel":      true, // .xls
	"application/vnd.ms-powerpoint": true, // .ppt
	"application/vnd.openxmlformats-officedocument.wordprocessingml.document":   true, // .docx
	"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":         true, // .xlsx
	"application/vnd.openxmlformats-officedocument.presentationml.presentation": true, // .pptx
	// OpenDocument
	"application/vnd.oasis.opendocument.text":         true, // .odt
	"application/vnd.oasis.opendocument.spreadsheet":  true, // .ods
	"application/vnd.oasis.opendocument.presentation": true, // .odp
	// Rich text
	"application/rtf": true, // .rtf
}

// GetFilePermissions retrieves all permissions for a given file
// ID from the Drive API with retry support.
func (c *Client) GetFilePermissions(
	ctx context.Context,
	fileID string,
	maxRetries int,
) ([]*drive.Permission, error) {
	logger := log.FromContext(ctx).WithValues("fileId", fileID)
	start := time.Now()

	logger.V(1).Info("calling Drive API to fetch permissions")

	var permissionsList *drive.PermissionList
	err := withRetry(ctx, maxRetries,
		fmt.Sprintf("GetFilePermissions(%s)", fileID),
		func() error {
			var callErr error
			permissionsList, callErr = c.driveService.
				Permissions.List(fileID).
				Context(ctx).
				Fields("permissions(type,role," +
					"emailAddress,displayName,domain)").
				SupportsAllDrives(true).
				Do()
			return callErr
		})

	if err != nil {
		logger.Error(err, "Drive API call failed",
			"timeElapsed", time.Since(start),
		)
		return nil, fmt.Errorf(
			"failed to get permissions for file %s: %w",
			fileID, err)
	}

	logger.V(1).Info("Drive API call successful",
		"permissionCount", len(permissionsList.Permissions),
		"timeElapsed", time.Since(start),
	)

	return permissionsList.Permissions, nil
}

// GetFileMetadata retrieves metadata for a single file or folder.
func (c *Client) GetFileMetadata(
	ctx context.Context,
	fileID string,
	maxRetries int,
) (*drive.File, error) {
	logger := log.FromContext(ctx).WithValues("fileID", fileID)
	start := time.Now()

	logger.V(1).Info("fetching file metadata")

	var file *drive.File
	err := withRetry(ctx, maxRetries,
		fmt.Sprintf("GetFileMetadata(%s)", fileID),
		func() error {
			var callErr error
			file, callErr = c.driveService.Files.Get(fileID).
				Context(ctx).
				Fields("id, name, mimeType, createdTime, " +
					"modifiedTime, owners(emailAddress), " +
					"size, shortcutDetails").
				SupportsAllDrives(true).
				Do()
			return callErr
		})

	if err != nil {
		logger.Error(err, "failed to get file metadata",
			"timeElapsed", time.Since(start),
		)
		return nil, fmt.Errorf(
			"failed to get metadata for file %s: %w",
			fileID, err)
	}

	logger.V(1).Info("file metadata fetched",
		"fileName", file.Name,
		"timeElapsed", time.Since(start),
	)

	return file, nil
}

// DownloadFile downloads the content of a file. It handles three
// cases in order:
//
//  1. Native Google Workspace files (Docs, Sheets, Slides,
//     Drawings) — exported as PDF via direct Docs editor URLs.
//  2. Uploaded files that Google can open in a Workspace editor
//     (DOCX, XLSX, PPTX, etc.) — also exported as PDF via Docs
//     editor URLs, determined by Drive importFormats.
//  3. All other files — downloaded as raw binary via Drive API.
//
// The caller must close the returned ReadCloser.
func (c *Client) DownloadFile(
	ctx context.Context,
	fileID string,
	maxRetries int,
) (io.ReadCloser, error) {
	logger := log.FromContext(ctx).WithValues("fileID", fileID)
	start := time.Now()

	meta, err := c.GetFileMetadata(ctx, fileID, maxRetries)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to get metadata for download: %w", err)
	}

	// Case 1: Native Google Workspace file
	if urlTmpl, ok := googleWorkspaceExportURLs[meta.MimeType]; ok {
		exportURL := fmt.Sprintf(urlTmpl, fileID)
		logger.V(1).Info("exporting native Workspace file",
			"fileName", meta.Name,
			"mimeType", meta.MimeType,
		)
		body, dlErr := c.downloadFromURL(
			ctx, exportURL, maxRetries)
		if dlErr != nil {
			return nil, fmt.Errorf(
				"failed to export file %s: %w", fileID, dlErr)
		}
		logger.Info("file exported as PDF",
			"fileName", meta.Name,
			"timeElapsed", time.Since(start),
		)
		return body, nil
	}

	// Non-downloadable Google types (Forms, Sites, Maps, etc.)
	if strings.HasPrefix(
		meta.MimeType, "application/vnd.google-apps.") {
		return nil, fmt.Errorf(
			"file %s has non-downloadable MIME type: %s",
			fileID, meta.MimeType)
	}

	// Case 2: Uploaded file that Google can open in a Workspace
	// editor (DOCX, XLSX, PPTX, etc.) — export as PDF via the
	// Docs editor URL instead of downloading raw binary.
	if exportURL := c.resolveWorkspaceCompatibleExportURL(
		ctx, fileID, meta.MimeType); exportURL != "" {
		logger.V(1).Info(
			"exporting Workspace-compatible uploaded file",
			"fileName", meta.Name,
			"mimeType", meta.MimeType,
		)
		body, dlErr := c.downloadFromURL(
			ctx, exportURL, maxRetries)
		if dlErr != nil {
			return nil, fmt.Errorf(
				"failed to export compatible file %s: %w",
				fileID, dlErr)
		}
		logger.Info("Workspace-compatible file exported as PDF",
			"fileName", meta.Name,
			"originalMimeType", meta.MimeType,
			"timeElapsed", time.Since(start),
		)
		return body, nil
	}

	// Case 3: Regular file — download raw content via Drive API
	logger.V(1).Info("downloading file",
		"fileName", meta.Name,
		"mimeType", meta.MimeType,
		"size", meta.Size,
	)
	var body io.ReadCloser
	err = withRetry(ctx, maxRetries,
		fmt.Sprintf("DownloadFile(%s)", fileID),
		func() error {
			resp, callErr := c.driveService.Files.
				Get(fileID).
				Context(ctx).
				SupportsAllDrives(true).
				Download()
			if callErr != nil {
				return callErr
			}
			body = resp.Body
			return nil
		})
	if err != nil {
		return nil, fmt.Errorf(
			"failed to download file %s: %w", fileID, err)
	}

	logger.Info("file downloaded",
		"fileName", meta.Name,
		"timeElapsed", time.Since(start),
	)
	return body, nil
}

// resolveWorkspaceCompatibleExportURL checks whether an uploaded
// file's MIME type can be converted to a Google Workspace editor
// format using Drive's importFormats, and returns the
// corresponding Docs editor export URL. Returns "" if the file
// is not convertible or should be downloaded natively.
func (c *Client) resolveWorkspaceCompatibleExportURL(
	ctx context.Context,
	fileID string,
	mimeType string,
) string {
	// Only attempt export for known-good Office/ODF formats.
	// Other importable types (CSV, TXT, images) return HTTP 400
	// from the Docs editor export endpoint.
	if !workspaceCompatibleFormats[mimeType] {
		return ""
	}

	importFormats := c.fetchImportFormats(ctx)
	targets, ok := importFormats[mimeType]
	if !ok {
		return ""
	}

	for _, target := range targets {
		if urlTmpl, ok := googleWorkspaceExportURLs[target]; ok {
			return fmt.Sprintf(urlTmpl, fileID)
		}
	}
	return ""
}

// importFormats cache — fetched once per Client lifetime from
// Drive about.importFormats.
var (
	importFormatsOnce  sync.Once
	importFormatsCache map[string][]string
)

// fetchImportFormats returns the Drive importFormats mapping
// (uploaded MIME type → list of Google Workspace MIME types it
// can be converted to). Cached after the first successful call.
func (c *Client) fetchImportFormats(
	ctx context.Context,
) map[string][]string {
	importFormatsOnce.Do(func() {
		logger := log.FromContext(ctx)
		about, err := c.driveService.About.Get().
			Context(ctx).
			Fields("importFormats").
			Do()
		if err != nil {
			logger.Error(err,
				"failed to fetch Drive importFormats")
			importFormatsCache = map[string][]string{}
			return
		}
		importFormatsCache = about.ImportFormats
		logger.V(1).Info("fetched Drive importFormats",
			"formatCount", len(importFormatsCache),
		)
	})
	return importFormatsCache
}

// downloadFromURL performs an authenticated GET request to the
// given URL with retry support.
func (c *Client) downloadFromURL(
	ctx context.Context,
	url string,
	maxRetries int,
) (io.ReadCloser, error) {
	var body io.ReadCloser
	err := withRetry(ctx, maxRetries,
		fmt.Sprintf("GET %s", url),
		func() error {
			req, reqErr := http.NewRequestWithContext(
				ctx, http.MethodGet, url, nil)
			if reqErr != nil {
				return fmt.Errorf(
					"failed to create request: %w", reqErr)
			}
			resp, doErr := c.httpClient.Do(req)
			if doErr != nil {
				return doErr
			}
			if resp.StatusCode != http.StatusOK {
				_ = resp.Body.Close()
				return fmt.Errorf(
					"export returned HTTP %d",
					resp.StatusCode)
			}
			body = resp.Body
			return nil
		})
	return body, err
}
