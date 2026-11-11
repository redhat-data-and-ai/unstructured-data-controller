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
	"net/http"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/modelcontextprotocol/go-sdk/mcp"
	drive "google.golang.org/api/drive/v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/google"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

const (
	// defaultDriveSearchLimit is used when the caller does not specify a limit.
	defaultDriveSearchLimit = 10

	// maxDriveSearchLimit caps the caller-supplied limit. Drive pages at 100.
	maxDriveSearchLimit = 100

	// driveSearchRetries bounds retries on a single Drive call. Low because a
	// user is waiting, unlike the crawler.
	driveSearchRetries = 2

	// driveSearchPageSize is how many candidates each Drive call fetches.
	// Larger than the usual limit on purpose: results are filtered after Drive
	// has ranked them, so a page of candidates yields fewer visible files.
	driveSearchPageSize = 100

	// maxDriveSearchPages bounds how far a single request will page. A term
	// that matches thousands of files the caller cannot read would otherwise
	// walk the whole corpus while they wait.
	maxDriveSearchPages = 5

	// maxReadableCandidates caps how many file IDs one access check asks about,
	// which bounds the IN list sent to Snowflake.
	maxReadableCandidates = 400
)

type searchGoogleDriveArgs struct {
	PipelineName string `json:"pipeline_name" jsonschema:"Name of the UnstructuredDataPipeline. If not known, call list_unstructured_data_pipelines_for_user first and pick the matching pipeline based on its description."`
	Query        string `json:"query" jsonschema:"Words to find in the file name or contents. Matching is by whole word and is not case sensitive, so 'report' also matches 'Reports'. Wrap the value in double quotes to match an exact phrase. There is no partial word matching: 'repo' does not match 'report'."`
	Limit        int    `json:"limit,omitempty" jsonschema:"Number of files to return. Defaults to 10 and is capped at 100: a larger value returns 100 files, not an error."`
}

// DriveSearchResult is one file the caller is allowed to read. FileID is what
// get_processed_document and get_citation_image take, but only for files already
// crawled: Drive also matches newer ones, which those tools cannot open.
type DriveSearchResult struct {
	FileID       string `json:"file_id"`
	Name         string `json:"name"`
	MIMEType     string `json:"mime_type"`
	ModifiedTime string `json:"modified_time"`
	WebViewLink  string `json:"web_view_link"`
	// DownloadURL is a short-lived link to the crawler's stored copy, which the
	// caller can fetch with no Google credential. Empty when the file has not
	// been stored yet, or when the link could not be signed.
	DownloadURL string `json:"download_url,omitempty"`

	// storedPath is where the crawler put the file, used to sign DownloadURL
	// and not reported to the caller.
	storedPath string
}

// DriveSearchOptions configures the search_google_drive tool.
type DriveSearchOptions struct {
	// Identifier re-validates the bearer token on the call being handled, so a
	// request can be matched to the session by identity rather than by token
	// string. Required: the Snowflake query runs as whoever the session says
	// the caller is, so a request from someone else must not be served on it.
	Identifier auth.TokenIdentifier
}

// folderTreeTTL bounds how stale a cached folder tree may be. A folder added to
// a pipeline is invisible to search until it expires, which is the cost of not
// walking the tree on every request.
const folderTreeTTL = 10 * time.Minute

// folderTreeCache remembers each pipeline's expanded folder tree.
//
// Drive's "in parents" is not recursive, so a search has to name every folder
// in the tree, and discovering them costs one call per folder. Doing that per
// request would dominate the latency.
type folderTreeCache struct {
	mu      sync.Mutex
	entries map[string]folderTreeEntry
}

type folderTreeEntry struct {
	folderIDs []string
	expiresAt time.Time
}

func newFolderTreeCache() *folderTreeCache {
	return &folderTreeCache{entries: make(map[string]folderTreeEntry)}
}

// get returns the cached tree for a pipeline, or expands and stores one.
//
// Keyed by pipeline rather than by caller: a folder tree is the same for
// everyone, and who may read what inside it is decided later.
func (c *folderTreeCache) get(
	ctx context.Context,
	pipelineName string,
	lister gdrive.SubfolderLister,
	rootIDs []string,
	skipNames []string,
	now time.Time,
) ([]string, error) {
	c.mu.Lock()
	entry, ok := c.entries[pipelineName]
	c.mu.Unlock()
	if ok && now.Before(entry.expiresAt) {
		return entry.folderIDs, nil
	}

	folderIDs, err := gdrive.ExpandFolderTree(ctx, lister, rootIDs, skipNames, driveSearchRetries)
	if err != nil {
		return nil, err
	}

	c.mu.Lock()
	c.entries[pipelineName] = folderTreeEntry{folderIDs: folderIDs, expiresAt: now.Add(folderTreeTTL)}
	c.mu.Unlock()
	return folderIDs, nil
}

// requestHeader returns the HTTP headers of the call being handled, or nil
// when the transport did not supply them. Nil is treated as a failure by the
// session check rather than waved through.
func requestHeader(req *mcp.CallToolRequest) http.Header {
	if req == nil || req.Extra == nil {
		return nil
	}
	return req.Extra.Header
}

// driveSearcher is the slice of the Google client this tool needs.
type driveSearcher interface {
	SearchFiles(ctx context.Context, query string, pageSize int, pageToken string, maxRetries int) (*google.SearchResult, error)
}

// RegisterSearchGoogleDrive registers the search_google_drive MCP tool.
//
// The search runs as the pipeline's service account, which can read the whole
// corpus, so results are filtered against the caller and the caller never
// authenticates to Google. That filter is the only access control here, which is
// why the server refuses to register the tool without token introspection.
func RegisterSearchGoogleDrive(s *mcp.Server, k8sClient *k8sclient.Client, opts DriveSearchOptions) {
	folderTrees := newFolderTreeCache()
	destinations := newDestinationClients(k8sClient)

	mcp.AddTool(s, &mcp.Tool{
		Name: "search_google_drive",
		Description: `Find files by name or exact wording in a pipeline's Google Drive folders, using Google Drive's own search. Returns file metadata and a link, not file contents.
Use this to locate a document the user named or described ("the Q3 revenue forecast"), or to find files containing an exact term such as an error code or a ticket ID.
To answer a question from the content of the documents, prefer get_chunks_for_embeddings, which searches by meaning where this one matches words. Each result here carries a short-lived download_url for the file itself.
Matching is literal, so a word spelled differently in the document will not be found. If a search returns nothing, try a synonym or a different spelling before concluding the corpus lacks it.
Only pipelines whose source is Google Drive support this tool. Results are limited to files the authenticated user is allowed to read, so an empty result can mean the user has no access rather than that nothing matched.
On error: report the exact error to the user and STOP. Do NOT retry with other pipelines.`,
	}, func(ctx context.Context, req *mcp.CallToolRequest, args searchGoogleDriveArgs) (*mcp.CallToolResult, any, error) {
		username := ""
		if tokenInfo, ok := auth.TokenInfoFromContext(ctx); ok {
			username = tokenInfo.Username
		}
		ctx = logger.NewContext(ctx, uuid.NewString(), "search_google_drive", username)
		log := logger.FromContext(ctx)

		log.Info("tool invoked", "pipeline_name", args.PipelineName)

		// ctx belongs to the session rather than this call, so username above is
		// whoever opened it. The check confirms this request came from the same
		// person and hands back the request's own token, which is what the
		// access query must run as: after a refresh the session's is stale.
		oauthToken, err := auth.CheckRequestMatchesSession(ctx, requestHeader(req), opts.Identifier)
		if err != nil {
			return toolError(log, "request does not match the session it arrived on", err,
				"Error: this request could not be matched to your session. Reconnect and try again."), nil, nil
		}

		if args.PipelineName == "" || args.Query == "" {
			return toolError(log, "missing required parameters", nil,
				"Error: pipeline_name and query are required. Call list_unstructured_data_pipelines_for_user first to get the pipeline name."), nil, nil
		}

		if !isPipelineAllowed(ctx, args.PipelineName) {
			return toolError(log, "pipeline not in per-client allow-list", nil,
				fmt.Sprintf("Error: pipeline %q is not accessible in this session.", args.PipelineName)), nil, nil
		}

		if k8sClient == nil {
			return toolError(log, "kubernetes client is nil", nil,
				"Error: unable to connect to the cluster. Please contact your administrator."), nil, nil
		}

		driveConfig, err := k8sClient.GetPipelineDriveSearchConfig(ctx, args.PipelineName)
		if err != nil {
			// "Does not expose Google Drive search" and "no such pipeline" lead
			// somewhere different, so do not report both as a bad name.
			message := fmt.Sprintf("Error: %s.", err)
			if apierrors.IsNotFound(err) {
				message = fmt.Sprintf("Error: there is no pipeline named %q. Please verify the pipeline name is correct.",
					args.PipelineName)
			}
			return toolError(log, "failed to resolve pipeline Drive config", err, message), nil, nil
		}

		driveClient, err := google.NewClientFromJSON(ctx, driveConfig.CredentialsJSON)
		if err != nil {
			return toolError(log, "failed to create Drive client", err,
				fmt.Sprintf("Error: could not reach Google Drive for pipeline %q.", args.PipelineName)), nil, nil
		}
		defer driveClient.Close()

		// Drive's "in parents" is not recursive, so the query has to name every
		// folder in the tree or it silently misses everything in a subfolder.
		folderIDs, err := folderTrees.get(ctx, args.PipelineName, driveClient,
			driveConfig.FolderIDs, driveConfig.SkipFolderNames, time.Now())
		if err != nil {
			if errors.Is(err, gdrive.ErrTooManyFolders) {
				return toolError(log, "folder tree too large to scope a search", err,
					fmt.Sprintf("Error: pipeline %q has more than %d folders, which is too many to search in one query. Narrow the pipeline's folders.",
						args.PipelineName, gdrive.MaxScopedFolders)), nil, nil
			}
			// Worth its own message: this is a misconfiguration an administrator
			// can fix, and it otherwise looks like the corpus is simply empty.
			if errors.Is(err, gdrive.ErrFolderUnreachable) {
				return toolError(log, "a configured folder is not shared with the service account", err,
					fmt.Sprintf("Error: pipeline %q names a Google Drive folder its service account cannot open. Ask an administrator to share the folder with it.",
						args.PipelineName)), nil, nil
			}
			return toolError(log, "failed to expand the pipeline's folder tree", err,
				fmt.Sprintf("Error: could not read the folders for pipeline %q.", args.PipelineName)), nil, nil
		}
		log.Info("folder tree resolved", "roots", len(driveConfig.FolderIDs), "folders", len(folderIDs))

		allowed := func(ctx context.Context, fileIDs []string) ([]snowflake.ReadableFile, error) {
			return snowflake.FilterReadableFiles(ctx, oauthToken,
				driveConfig.Access.Database, driveConfig.Access.Schema, driveConfig.Access.Table, fileIDs)
		}

		results, outcome, err := searchDriveForUser(ctx, driveClient, args, folderIDs, allowed)
		if err != nil {
			// Not temporary, unlike the generic failure below, and the caller
			// can act on it by shortening the search term.
			if errors.Is(err, gdrive.ErrQueryTooLong) {
				return toolError(log, "built query exceeds what Drive accepts", err,
					fmt.Sprintf("Error: this search is too long to run against pipeline %q, because its folder list and your search term together exceed what Google Drive accepts. Try a shorter search term.",
						args.PipelineName)), nil, nil
			}
			return toolError(log, "drive search failed", err,
				"Error: the Google Drive search could not be completed. This is usually temporary, so it is worth retrying."), nil, nil
		}

		// Signed after filtering, so a link is only ever minted for a file the
		// caller is already allowed to have. A failure here leaves the result
		// without a URL rather than dropping it: the file is still a real
		// answer, it just cannot be downloaded this way.
		signDownloadURLs(ctx, destinations, args.PipelineName, driveConfig.StageName, results)

		jsonBytes, err := json.Marshal(results)
		if err != nil {
			return toolError(log, "failed to marshal result", err,
				"Error: the search results could not be rendered."), nil, nil
		}

		log.Info("completed successfully",
			"returned", len(results),
			"incompleteSearch", outcome.Incomplete,
			"partial", outcome.Partial)

		// Coverage is reported because it describes the search. How many matches
		// the caller was not allowed to see is not, since that would confirm
		// files exist that they cannot read.
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{
				Text: fmt.Sprintf("Found %d file(s) in pipeline %q you have access to:\n%s%s%s",
					len(results), args.PipelineName, string(jsonBytes),
					coverageNote(outcome), clampNote(args.Limit)),
			}},
		}, nil, nil
	})
}

// searchOutcome is what one search produced, beyond the files themselves.
type searchOutcome struct {
	// Incomplete is Drive's own admission that it did not search everything.
	Incomplete bool
	// Partial is true when the result is short of the limit for a reason other
	// than the corpus: the page budget stopped the walk before the pages ran out.
	Partial bool
}

// accessFilter reports which of the given Drive file IDs the caller may read,
// and where each one was stored.
//
// Implemented against Snowflake's row access policy, which is the same gate
// every other tool in this server answers to.
type accessFilter func(ctx context.Context, fileIDs []string) ([]snowflake.ReadableFile, error)

// searchDriveForUser runs the search and returns only the files the caller may
// read, as decided by allowed.
//
// Drive is asked what matches, Snowflake is asked who may see it. The search
// runs as the pipeline's service account, so Drive's own answer covers the
// whole corpus and says nothing about this caller.
func searchDriveForUser(
	ctx context.Context,
	driveClient driveSearcher,
	args searchGoogleDriveArgs,
	folderIDs []string,
	allowed accessFilter,
) ([]DriveSearchResult, searchOutcome, error) {
	limit := resolveDriveLimit(args.Limit)

	query := gdrive.BuildSearchQuery(args.Query, folderIDs)
	// The folder count is capped already, but the search term shares the same
	// byte budget, so a long term can overflow a legal tree.
	if err := gdrive.CheckQueryLength(query); err != nil {
		return nil, searchOutcome{}, err
	}

	// Pages are collected unfiltered and the whole batch is then checked in one
	// Snowflake round trip. Checking per page would cost a query per page to
	// save, at most, one.
	found, err := collectPages(ctx, driveClient, query, limit)
	if err != nil {
		return nil, searchOutcome{}, err
	}

	candidates := make([]string, 0, len(found.files))
	// Keyed on the id actually reported, not the id Drive matched: two
	// shortcuts to one target are one file to everyone downstream.
	byID := make(map[string]*drive.File, len(found.files))
	for _, file := range found.files {
		result := toDriveSearchResult(file)
		if _, seen := byID[result.FileID]; seen {
			continue
		}
		byID[result.FileID] = file
		candidates = append(candidates, result.FileID)
	}

	readable, err := allowed(ctx, candidates)
	if err != nil {
		return nil, searchOutcome{}, err
	}
	permitted := make(map[string]snowflake.ReadableFile, len(readable))
	for _, file := range readable {
		permitted[file.FileID] = file
	}

	// Ranked order comes from Drive, so walk candidates rather than readable.
	results := make([]DriveSearchResult, 0, limit)
	for _, fileID := range candidates {
		if len(results) >= limit {
			break
		}
		stored, ok := permitted[fileID]
		if !ok {
			continue
		}
		result := toDriveSearchResult(byID[fileID])
		result.storedPath = stored.FilePath
		results = append(results, result)
	}

	return results, searchOutcome{
		Incomplete: found.incomplete,
		Partial:    !found.exhausted && len(results) < limit,
	}, nil
}

// coverageNote warns when a short result has a cause other than the corpus, so
// the model does not report that nothing exists. It never says how many files
// were withheld.
func coverageNote(outcome searchOutcome) string {
	switch {
	case outcome.Incomplete:
		return "\n\nNOTE: Google Drive reported that it did not search everything, so this list may be incomplete. Try a narrower term."
	case outcome.Partial:
		return "\n\nNOTE: the search stopped before reading every match. There may be more. Try a narrower term."
	default:
		return ""
	}
}

// clampNote tells the model its limit was capped, so it does not read a capped
// result as the complete answer.
func clampNote(requested int) string {
	if requested <= maxDriveSearchLimit {
		return ""
	}
	return fmt.Sprintf(
		"\n\nNOTE: limit=%d exceeds the maximum of %d, so at most %d files are returned. This result may be incomplete for the request as phrased.",
		requested, maxDriveSearchLimit, maxDriveSearchLimit,
	)
}

// pagedSearch is the outcome of walking one query's pages.
type pagedSearch struct {
	files      []*drive.File
	incomplete bool
	exhausted  bool
}

// collectPages walks a query's pages until it has candidateBudget(limit) files,
// runs out, or hits the page budget.
//
// It gathers more than limit because access is decided afterwards, so some of
// these will be dropped. The page budget stops a term matching thousands of
// files from walking the whole corpus while the caller waits.
func collectPages(
	ctx context.Context,
	driveClient driveSearcher,
	query string,
	limit int,
) (*pagedSearch, error) {
	budget := candidateBudget(limit)
	out := &pagedSearch{files: make([]*drive.File, 0, budget)}
	pageToken := ""

	for range maxDriveSearchPages {
		result, err := driveClient.SearchFiles(ctx, query, driveSearchPageSize, pageToken, driveSearchRetries)
		if err != nil {
			return nil, err
		}
		if result.Incomplete {
			out.incomplete = true
		}

		out.files = append(out.files, result.Files...)
		if len(out.files) >= budget {
			return out, nil
		}

		pageToken = result.NextPageToken
		if pageToken == "" {
			out.exhausted = true
			return out, nil
		}
	}

	return out, nil
}

// candidateBudget is how many matches to gather before filtering, given that
// the caller wants limit of them.
//
// Overshooting costs one larger Snowflake IN list, which is cheap. Undershooting
// returns fewer files than the caller could have seen, which looks like a corpus
// that does not contain them.
func candidateBudget(limit int) int {
	budget := limit * 4
	if budget > maxReadableCandidates {
		return maxReadableCandidates
	}
	return budget
}

// toDriveSearchResult reports the file Drive matched and never follows a shortcut
// to its target, whose permissions are not the ones that were checked.
// BuildSearchQuery excludes shortcuts for the same reason.
func toDriveSearchResult(file *drive.File) DriveSearchResult {
	return DriveSearchResult{
		FileID:       file.Id,
		Name:         file.Name,
		MIMEType:     file.MimeType,
		ModifiedTime: file.ModifiedTime,
		WebViewLink:  file.WebViewLink,
	}
}

func resolveDriveLimit(requested int) int {
	if requested <= 0 {
		return defaultDriveSearchLimit
	}
	return min(requested, maxDriveSearchLimit)
}
