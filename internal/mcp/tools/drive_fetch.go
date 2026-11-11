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

package tools

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
)

type driveFetchArgs struct {
	PipelineName string `json:"pipeline_name" jsonschema:"Name of the UnstructuredDataPipeline that crawled the file. If not known, call list_unstructured_data_pipelines_for_user first."`
	FileID       string `json:"file_id" jsonschema:"Google Drive file ID, as returned by drive_search."`
}

// driveFetchResult is what the caller gets for a file they may read.
type driveFetchResult struct {
	FileID       string `json:"file_id"`
	DownloadURL  string `json:"download_url"`
	ExpiresInSec int    `json:"expires_in_seconds"`
}

// errDriveFetchDenied is returned for a file the caller may not read, for one
// this pipeline never crawled, and for one whose ACL is missing. These must
// be indistinguishable: telling them apart would make the tool an existence
// oracle over documents the caller cannot see.
const errDriveFetchDenied = "Error: no file with that file_id is available to you in this pipeline."

// RegisterDriveFetch registers the drive_fetch MCP tool.
func RegisterDriveFetch(s *mcp.Server, k8sClient *k8sclient.Client) {
	store := newCrawlStore(k8sClient)

	mcp.AddTool(s, &mcp.Tool{
		Name: "drive_fetch",
		Description: `Return a temporary download URL for a Google Drive file that this pipeline has already crawled.

Use this after drive_search has given you a file_id and you need the file itself rather than its text. The URL is time-limited and needs no credentials, so give it to the user to download with; do not try to read it yourself if the file is binary.
Only files the authenticated user is allowed to read in Google Drive are returned. A file the pipeline never crawled, or one the user cannot read, reports the same message.
On error: report the exact error to the user and STOP. Do NOT retry with other pipelines or other file ids.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args driveFetchArgs) (*mcp.CallToolResult, any, error) {
		username := ""
		if tokenInfo, ok := auth.TokenInfoFromContext(ctx); ok {
			username = tokenInfo.Username
		}
		ctx = logger.NewContext(ctx, uuid.NewString(), "drive_fetch", username)
		log := logger.FromContext(ctx)

		log.Info("tool invoked", "pipeline_name", args.PipelineName, "file_id", args.FileID)

		if args.PipelineName == "" || args.FileID == "" {
			return toolError(log, "missing required parameters", nil,
				"Error: pipeline_name and file_id are required. Call drive_search first."), nil, nil
		}

		// Checked before any lookup: it is caller supplied and is joined onto
		// a bucket prefix below.
		if !driveFileID.MatchString(args.FileID) {
			return toolError(log, "malformed drive file id", nil,
				fmt.Sprintf("Error: %q is not a valid Google Drive file ID.", args.FileID)), nil, nil
		}

		if !isPipelineAllowed(ctx, args.PipelineName) {
			return toolError(log, "pipeline not in per-client allow-list", nil,
				fmt.Sprintf("Error: pipeline %q is not accessible in this session.", args.PipelineName)), nil, nil
		}

		if _, ok := auth.AccessTokenFromContext(ctx); !ok {
			return toolError(log, "oauth token not found in context", nil, errOAuthTokenNotFound), nil, nil
		}

		// A caller with no resolved identity can be matched against no ACL,
		// so there is nothing they may read.
		if username == "" {
			return toolError(log, "token carries no username", nil,
				"Error: your token carries no username, so Drive permissions cannot be checked."), nil, nil
		}

		if k8sClient == nil {
			return toolError(log, "kubernetes client is nil", nil,
				"Error: kubernetes client is not initialized"), nil, nil
		}

		stagePath, err := store.stagePath(ctx, args.PipelineName)
		if err != nil {
			return toolError(log, "failed to resolve the drive crawl stage", err,
				fmt.Sprintf("Error: pipeline %q does not crawl Google Drive.", args.PipelineName)), nil, nil
		}

		// The authorization check, against the ACL the crawler recorded for
		// this file. Everything below it is reachable only once it passes.
		allowed, err := store.permits(ctx, stagePath, args.FileID, username)
		if err != nil {
			if errors.Is(err, errNoACL) {
				log.Info("no stored permissions for file", "file_id", args.FileID, "error", err)
				return toolError(log, "refusing a file with no stored ACL", nil, errDriveFetchDenied), nil, nil
			}
			// A failed read is not a denial. Reporting it as one would have
			// the model tell the user the file does not exist, when the file
			// store was briefly unreachable.
			return toolError(log, "failed to read stored permissions", err,
				fmt.Sprintf("Error: the permission check for file %q failed. This is not the same as being denied, so it is worth retrying.",
					args.FileID)), nil, nil
		}
		if !allowed {
			log.Info("caller is not in the file's ACL", "file_id", args.FileID)
			return toolError(log, "caller not permitted to read file", nil, errDriveFetchDenied), nil, nil
		}

		url, err := store.presignedURL(ctx, stagePath, args.FileID)
		if err != nil {
			if errors.Is(err, errNoCrawledFile) {
				log.Info("file is permitted but not in the file store", "file_id", args.FileID, "error", err)
				return toolError(log, "no crawled file for this id", nil, errDriveFetchDenied), nil, nil
			}
			return toolError(log, "failed to presign the crawled file", err,
				fmt.Sprintf("Error: unable to produce a download URL for file %q.", args.FileID)), nil, nil
		}

		result := driveFetchResult{
			FileID:       args.FileID,
			DownloadURL:  url,
			ExpiresInSec: int(presignedURLTTL.Seconds()),
		}
		jsonBytes, err := json.Marshal(result)
		if err != nil {
			return toolError(log, "failed to marshal result", err,
				"Error: unable to produce the download URL result"), nil, nil
		}

		// The URL itself is never logged: it is a bearer capability, and the
		// logs outlive it.
		log.Info("completed successfully", "pipeline", args.PipelineName, "file_id", args.FileID)
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
			Text: fmt.Sprintf("Download URL for Drive file %q, valid for %d minutes:\n%s",
				args.FileID, int(presignedURLTTL.Minutes()), string(jsonBytes)),
		}}}, nil, nil
	})
}
