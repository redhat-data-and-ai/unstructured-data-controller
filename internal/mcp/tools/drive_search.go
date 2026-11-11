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
	"strings"
	"sync"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	"golang.org/x/sync/errgroup"
	drive "google.golang.org/api/drive/v3"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/google"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
)

// DriveSearcher is the slice of the Drive API that drive_search needs.
type DriveSearcher interface {
	SearchFiles(ctx context.Context, query string, pageSize int64, pageToken string, maxRetries int,
	) (*google.SearchResult, error)
}

const (
	// driveSearchPageSize is how many hits each Drive call fetches. The
	// maximum on purpose: pages are walked until the candidate budget is
	// full, and a larger page is one fewer round trip towards it.
	driveSearchPageSize = google.MaxSearchPageSize

	// maxDriveSearchPages bounds how far one request will page. A term
	// matching thousands of files this pipeline never crawled would
	// otherwise walk the whole corpus while the caller waits.
	maxDriveSearchPages = 5

	// maxDriveCandidates caps how many hits one search considers, whatever
	// the requested result count. Each candidate costs one object read for
	// its stored permissions, so this is what bounds the cost of a search.
	maxDriveCandidates = 200

	// driveACLConcurrency is how many permission checks run at once. The
	// checks are independent object reads, and doing them in series makes a
	// full candidate set take hundreds of round trips.
	driveACLConcurrency = 10

	// defaultDriveResults is how many hits are returned when the caller asks
	// for no particular number.
	defaultDriveResults = 10

	// maxDriveResults bounds one response. A model reading more file IDs
	// than this is guessing rather than searching.
	maxDriveResults = 25

	// driveSearchRetries bounds retries on a single Drive call. Low because
	// a user is waiting on it, unlike the crawler.
	driveSearchRetries = 2
)

type driveSearchArgs struct {
	PipelineName string `json:"pipeline_name" jsonschema:"Name of the UnstructuredDataPipeline whose crawled corpus the results are limited to. If not known, call list_unstructured_data_pipelines_for_user first."`
	Query        string `json:"query" jsonschema:"Words to find in the file name or contents. Matching is by whole word and is not case sensitive, so 'report' also matches 'Reports', but 'repo' does not match 'report'. Wrap the value in double quotes to match an exact phrase."`
	MaxResults   int    `json:"max_results,omitempty" jsonschema:"Maximum number of files to return. Defaults to 10, capped at 25."`
}

// driveSearchHit is one file the caller may read.
type driveSearchHit struct {
	FileID   string `json:"file_id"`
	Name     string `json:"name"`
	MimeType string `json:"mime_type"`
	// WebViewLink opens the file in Drive, for a caller who has a browser and
	// a Google session. drive_fetch is the other way: bytes with no Google
	// credential at all.
	WebViewLink  string `json:"web_view_link,omitempty"`
	ModifiedTime string `json:"modified_time,omitempty"`
	Owner        string `json:"owner,omitempty"`
	SizeBytes    int64  `json:"size_bytes,omitempty"`
}

// RegisterDriveSearch registers the drive_search MCP tool. driveClient may be
// nil, in which case the tool is not registered at all: a search tool that
// reports a configuration fault on every call is worse than one the model
// never sees.
func RegisterDriveSearch(s *mcp.Server, k8sClient *k8sclient.Client, driveClient DriveSearcher) {
	if driveClient == nil {
		return
	}
	store := newCrawlStore(k8sClient)

	mcp.AddTool(s, &mcp.Tool{
		Name: "drive_search",
		Description: `Find Google Drive files by keyword and return their file IDs.

Use this to locate a document the user named or described ("the Q3 revenue forecast"), or to find files containing an exact term such as an error code or a ticket ID. To answer a question from the text, use get_chunks_for_embeddings instead, which searches by meaning where this one matches words; this tool returns identifiers, not content. Pass the returned file_id to drive_fetch to download the file.
Matching is literal, so a word spelled differently in the document will not be found. If a search returns nothing, try a synonym or a different spelling before concluding the corpus lacks it.
Results are limited to files this pipeline has crawled and the authenticated user is allowed to read in Google Drive, so an empty result can mean the user has no access rather than that nothing matched.
On error: report the exact error to the user and STOP. Do NOT retry with other pipelines.`,
	}, func(ctx context.Context, _ *mcp.CallToolRequest, args driveSearchArgs) (*mcp.CallToolResult, any, error) {
		username := ""
		if tokenInfo, ok := auth.TokenInfoFromContext(ctx); ok {
			username = tokenInfo.Username
		}
		ctx = logger.NewContext(ctx, uuid.NewString(), "drive_search", username)
		log := logger.FromContext(ctx)

		log.Info("tool invoked", "pipeline_name", args.PipelineName,
			"query", args.Query, "max_results", args.MaxResults)

		query := strings.TrimSpace(args.Query)
		if args.PipelineName == "" || query == "" {
			return toolError(log, "missing required parameters", nil,
				"Error: pipeline_name and query are required."), nil, nil
		}

		if !isPipelineAllowed(ctx, args.PipelineName) {
			return toolError(log, "pipeline not in per-client allow-list", nil,
				fmt.Sprintf("Error: pipeline %q is not accessible in this session.", args.PipelineName)), nil, nil
		}

		if _, ok := auth.AccessTokenFromContext(ctx); !ok {
			return toolError(log, "oauth token not found in context", nil, errOAuthTokenNotFound), nil, nil
		}

		// A caller with no resolved identity can be matched against no ACL,
		// so every hit would be dropped anyway. Said plainly instead.
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

		limit := resolveDriveResultLimit(args.MaxResults)

		found, err := collectDrivePages(ctx, driveClient, google.KeywordQuery(query), limit)
		if err != nil {
			return toolError(log, "drive search failed", err,
				"Error: the Google Drive search failed. This is worth retrying."), nil, nil
		}
		log.Info("drive search returned candidates",
			"count", len(found.files), "incomplete", found.incomplete, "exhausted", found.exhausted)

		hits, err := store.permittedHits(ctx, stagePath, username, found.files)
		if err != nil {
			return toolError(log, "failed to check stored permissions", err,
				"Error: the permission check failed. This is not the same as finding nothing, so it is worth retrying."), nil, nil
		}

		truncated := false
		if len(hits) > limit {
			hits = hits[:limit]
			truncated = true
		}

		// Short of the limit with pages still unread: the budget stopped the
		// walk, not the corpus running out.
		partial := !found.exhausted && !truncated && len(hits) < limit
		coverage := driveCoverageNote(found.incomplete, partial)

		log.Info("completed successfully", "pipeline", args.PipelineName,
			"candidates", len(found.files), "returned", len(hits))

		if len(hits) == 0 {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
				Text: fmt.Sprintf("No files matching %q are available to you in pipeline %q. Either nothing matches, or the matches are outside what this pipeline crawls or what you may read in Drive.%s",
					query, args.PipelineName, coverage),
			}}}, nil, nil
		}

		jsonBytes, err := json.Marshal(hits)
		if err != nil {
			return toolError(log, "failed to marshal result", err,
				"Error: unable to produce the search result"), nil, nil
		}

		summary := fmt.Sprintf("Found %d file(s) matching %q in pipeline %q",
			len(hits), query, args.PipelineName)
		if truncated {
			summary += fmt.Sprintf(", showing the %d Drive ranked highest", limit)
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{
			Text: fmt.Sprintf("%s:\n%s%s", summary, string(jsonBytes), coverage),
		}}}, nil, nil
	})
}

// permittedHits keeps the Drive results this pipeline crawled that username
// may read, in the order Drive returned them.
func (c *crawlStore) permittedHits(
	ctx context.Context, stagePath, username string, files []*drive.File,
) ([]driveSearchHit, error) {
	return permittedHits(ctx, files, func(ctx context.Context, fileID string) (bool, error) {
		return c.permits(ctx, stagePath, fileID, username)
	})
}

// permittedHits filters Drive results through permits, preserving the order
// Drive returned them in.
//
// A file with no crawled copy or no stored ACL is dropped rather than
// failing the search: an unscoped Drive query reaches the whole corpus the
// service account can see, so most hits are legitimately neither. Only a
// fault reading the store is an error, since dropping those silently would
// report "nothing found" for an outage.
func permittedHits(
	ctx context.Context, files []*drive.File,
	permits func(ctx context.Context, fileID string) (bool, error),
) ([]driveSearchHit, error) {
	allowed := make([]bool, len(files))

	group, groupCtx := errgroup.WithContext(ctx)
	group.SetLimit(driveACLConcurrency)

	var mu sync.Mutex
	var readErr error

	for i, file := range files {
		group.Go(func() error {
			permitted, err := permits(groupCtx, file.Id)
			if err != nil {
				// Not in this pipeline's corpus, or crawled without an ACL.
				// Both are ordinary for an unscoped query.
				if isDriveMissing(err) {
					return nil
				}
				mu.Lock()
				readErr = err
				mu.Unlock()
				return err
			}
			allowed[i] = permitted
			return nil
		})
	}
	if err := group.Wait(); err != nil {
		return nil, readErr
	}

	hits := make([]driveSearchHit, 0, len(files))
	for i, file := range files {
		if !allowed[i] {
			continue
		}
		owner := ""
		if len(file.Owners) > 0 {
			owner = file.Owners[0].EmailAddress
		}
		hits = append(hits, driveSearchHit{
			FileID:       file.Id,
			Name:         file.Name,
			MimeType:     file.MimeType,
			WebViewLink:  file.WebViewLink,
			ModifiedTime: file.ModifiedTime,
			Owner:        owner,
			SizeBytes:    file.Size,
		})
	}
	return hits, nil
}

// isDriveMissing reports an outcome that means "not available here" rather
// than a fault: no crawled copy, no stored ACL, or an ID Drive returned that
// is not one this server will look up.
func isDriveMissing(err error) bool {
	return errors.Is(err, errNoCrawledFile) ||
		errors.Is(err, errNoACL) ||
		errors.Is(err, errNotADriveFileID)
}

// pagedDriveSearch is the outcome of walking one query's pages.
type pagedDriveSearch struct {
	files []*drive.File
	// incomplete is Drive's own admission that it did not search everything.
	incomplete bool
	// exhausted is true when the walk reached the last page, so a short
	// result means the corpus holds no more rather than that we stopped.
	exhausted bool
}

// collectDrivePages walks a query's pages until it has driveCandidateBudget
// files, runs out of pages, or hits the page budget.
//
// Paging matters here because access is decided after Drive has ranked:
// taking one page and filtering it reports "nothing found" when it means
// "nothing on the first page", which for a corpus the caller mostly cannot
// read is the common case rather than the edge case.
func collectDrivePages(
	ctx context.Context, driveClient DriveSearcher, query string, limit int,
) (*pagedDriveSearch, error) {
	budget := driveCandidateBudget(limit)
	found := &pagedDriveSearch{files: make([]*drive.File, 0, budget)}
	pageToken := ""

	for range maxDriveSearchPages {
		result, err := driveClient.SearchFiles(
			ctx, query, driveSearchPageSize, pageToken, driveSearchRetries)
		if err != nil {
			return nil, err
		}
		if result.Incomplete {
			found.incomplete = true
		}

		found.files = append(found.files, result.Files...)
		if len(found.files) >= budget {
			return found, nil
		}

		pageToken = result.NextPageToken
		if pageToken == "" {
			found.exhausted = true
			return found, nil
		}
	}
	return found, nil
}

// driveCandidateBudget is how many hits to gather before filtering, given
// that the caller wants limit of them.
//
// Overshooting costs a few more object reads, which are concurrent and
// cheap. Undershooting returns fewer files than the caller could have seen,
// which reads as a corpus that does not contain them.
func driveCandidateBudget(limit int) int {
	budget := limit * 4
	if budget > maxDriveCandidates {
		return maxDriveCandidates
	}
	return budget
}

// driveCoverageNote warns when a short result has a cause other than the
// corpus, so the model does not report that nothing exists. It never says
// how many files were withheld, which would leak their existence.
func driveCoverageNote(incomplete, partial bool) string {
	switch {
	case incomplete:
		return "\n\nNOTE: Google Drive reported that it did not search everything, so this list may be incomplete. Try a narrower term."
	case partial:
		return "\n\nNOTE: the search stopped before reading every match. There may be more. Try a narrower term."
	default:
		return ""
	}
}

func resolveDriveResultLimit(requested int) int {
	if requested <= 0 {
		return defaultDriveResults
	}
	if requested > maxDriveResults {
		return maxDriveResults
	}
	return requested
}
