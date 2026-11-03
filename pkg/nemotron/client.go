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

// Package nemotron is a client for the Nemotron Parse sidecar.
//
// The sidecar exposes a submit-then-poll API deliberately shaped like
// docling-serve, so this client mirrors pkg/docling: the same weighted
// semaphore bounds in-flight conversions, and the same acquiredTasks map makes
// the operator drop tasks it no longer owns after a restart. Keeping the two
// clients structurally identical is what lets DocumentProcessor dispatch
// between them without a second control flow.
package nemotron

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type TaskStatus string

const (
	TaskStatusPending        TaskStatus = "pending"
	TaskStatusStarted        TaskStatus = "started"
	TaskStatusSuccess        TaskStatus = "success"
	TaskStatusFailure        TaskStatus = "failure"
	TaskStatusPartialSuccess TaskStatus = "partial_success"

	// SemaphoreAcquireError is returned when no local conversion slot is free.
	// The caller leaves the file unprocessed and picks it up next reconcile.
	SemaphoreAcquireError = "failed to acquire nemotron semaphore"

	// SidecarAtCapacityError is the remote equivalent: the sidecar's own task
	// store is full and answered 429. Same handling, different origin.
	SidecarAtCapacityError = "nemotron sidecar at capacity"
)

// ParseOptions is the wire-level options block sent with a submission.
// Field names match the sidecar's ParseOptions model.
type ParseOptions struct {
	Mode              string   `json:"mode,omitempty"`
	DPI               int      `json:"dpi,omitempty"`
	IncludePageImages bool     `json:"include_page_images"`
	CropSkipClasses   []string `json:"crop_skip_classes,omitempty"`
}

type Source struct {
	URL  string `json:"url"`
	Kind string `json:"kind"`
}

type parseRequest struct {
	Options ParseOptions `json:"options"`
	Sources []Source     `json:"sources"`
}

// SubmitResponse is returned immediately; the parse has not started yet.
type SubmitResponse struct {
	TaskID       string     `json:"task_id"`
	TaskStatus   TaskStatus `json:"task_status"`
	TaskPosition int        `json:"task_position"`
}

type StatusResponse struct {
	TaskID       string     `json:"task_id"`
	TaskStatus   TaskStatus `json:"task_status"`
	TaskPosition int        `json:"task_position"`
	ErrorMessage string     `json:"error_message,omitempty"`
}

// Block is one semantic region detected on a page.
type Block struct {
	Index int    `json:"index"`
	Class string `json:"class"`
	// Text is the raw model output. For tables this is LaTeX.
	Text string `json:"text"`
	// Content is postprocessed. For tables this is HTML.
	Content    string    `json:"content"`
	BBox       []float64 `json:"bbox,omitempty"`
	BBoxPixels []float64 `json:"bbox_pixels,omitempty"`
	// Crop is a sidecar-local asset name, valid only for this task's lifetime.
	// Empty when the class is skipped or the bounding box is degenerate.
	Crop string `json:"crop,omitempty"`
}

type Page struct {
	PageNumber int                      `json:"page_number"`
	Markdown   string                   `json:"markdown"`
	PageImage  string                   `json:"page_image,omitempty"`
	Blocks     []Block                  `json:"blocks"`
	Warnings   []map[string]interface{} `json:"warnings,omitempty"`
}

type Document struct {
	Pages    []Page `json:"pages"`
	Markdown string `json:"markdown"`
	// PageCount is pages in the source. Comparing it to len(Pages) is how the
	// caller detects a partially parsed document.
	PageCount     int    `json:"page_count"`
	Mode          string `json:"mode"`
	ModelRequests int    `json:"model_requests"`
}

type ResultResponse struct {
	TaskID         string     `json:"task_id"`
	Status         TaskStatus `json:"status"`
	ProcessingTime float64    `json:"processing_time"`
	Errors         []string   `json:"errors"`
	Document       *Document  `json:"document"`
}

type ClientConfig struct {
	URL                   string
	Key                   string
	MaxConcurrentRequests int64
	HTTPTimeout           time.Duration

	sem *semaphore.Weighted
}

type Client struct {
	ClientConfig  *ClientConfig
	httpClient    *http.Client
	acquiredTasks sync.Map
}

func NewClientFromURL(clientConfig *ClientConfig) *Client {
	clientConfig.sem = semaphore.NewWeighted(clientConfig.MaxConcurrentRequests)
	return &Client{
		ClientConfig: clientConfig,
		httpClient:   &http.Client{Timeout: clientConfig.HTTPTimeout},
	}
}

// IsAcquiredTask reports whether this process submitted the task. After an
// operator restart the map is empty, so jobs left in CR status are recognised
// as stale and resubmitted rather than polled forever.
func (c *Client) IsAcquiredTask(taskID string) bool {
	_, ok := c.acquiredTasks.Load(taskID)
	return ok
}

// releaseTask frees the semaphore slot, but only for a task this process
// acquired. Stale jobs from before a restart never held a slot.
func (c *Client) releaseTask(taskID string) {
	if _, loaded := c.acquiredTasks.LoadAndDelete(taskID); loaded {
		c.ClientConfig.sem.Release(1)
	}
}

func (c *Client) endpoint(parts ...string) (string, error) {
	return url.JoinPath(c.ClientConfig.URL, parts...)
}

func (c *Client) newRequest(ctx context.Context, method, endpoint string, payload []byte) (*http.Request, error) {
	var body io.Reader
	if len(payload) > 0 {
		body = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, endpoint, body)
	if err != nil {
		return nil, fmt.Errorf("creating request for %s: %w", endpoint, err)
	}
	req.Header.Set("Accept", "application/json")
	if len(payload) > 0 {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.ClientConfig.Key != "" {
		req.Header.Set("Authorization", "Bearer "+c.ClientConfig.Key)
	}
	return req, nil
}

// do issues a request and returns the body on 200. Non-200 responses are
// turned into errors here so callers only handle the happy path plus error.
func (c *Client) do(ctx context.Context, method, endpoint string, payload []byte) ([]byte, error) {
	req, err := c.newRequest(ctx, method, endpoint, payload)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("sending request to %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("reading response from %s: %w", endpoint, err)
	}

	if resp.StatusCode == http.StatusTooManyRequests {
		// The sidecar is saturated. Surfaced as a sentinel so the caller backs
		// off instead of recording a conversion failure against the document.
		return nil, errors.New(SidecarAtCapacityError)
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("nemotron request to %s returned status %d: %s",
			endpoint, resp.StatusCode, truncate(string(body), 300))
	}

	return body, nil
}

// ConvertFile submits a document for parsing and returns immediately with a
// task ID. fileURL is a presigned URL the sidecar fetches itself, so document
// bytes never pass through the operator.
func (c *Client) ConvertFile(ctx context.Context, fileURL string, options ParseOptions) (*SubmitResponse, error) {
	logger := log.FromContext(ctx)

	if !c.ClientConfig.sem.TryAcquire(1) {
		return nil, errors.New(SemaphoreAcquireError)
	}

	submitEndpoint, err := c.endpoint("/v1/parse/source/async")
	if err != nil {
		c.ClientConfig.sem.Release(1)
		return nil, fmt.Errorf("building submit endpoint: %w", err)
	}

	payload, err := json.Marshal(parseRequest{
		Options: options,
		Sources: []Source{{URL: fileURL, Kind: "http"}},
	})
	if err != nil {
		c.ClientConfig.sem.Release(1)
		return nil, fmt.Errorf("marshalling parse request: %w", err)
	}

	// Log the URL without its query string: a presigned URL's signature is a
	// bearer credential and must not reach the logs.
	sourceURL, err := baseURL(fileURL)
	if err != nil {
		c.ClientConfig.sem.Release(1)
		return nil, fmt.Errorf("parsing presigned url: %w", err)
	}
	logger.Info("submitting document to nemotron sidecar",
		"endpoint", submitEndpoint, "sourceURL", sourceURL, "mode", options.Mode)

	body, err := c.do(ctx, http.MethodPost, submitEndpoint, payload)
	if err != nil {
		c.ClientConfig.sem.Release(1)
		return nil, err
	}

	var submitResponse SubmitResponse
	if err := json.Unmarshal(body, &submitResponse); err != nil {
		c.ClientConfig.sem.Release(1)
		return nil, fmt.Errorf("decoding submit response: %w", err)
	}
	if submitResponse.TaskID == "" {
		c.ClientConfig.sem.Release(1)
		return nil, errors.New("nemotron sidecar returned an empty task id")
	}

	c.acquiredTasks.Store(submitResponse.TaskID, true)
	return &submitResponse, nil
}

// GetConvertedFile polls a task and, once terminal, returns its result.
//
// A nil result with a non-terminal status means the parse is still running and
// the caller should requeue. The semaphore slot is released on every terminal
// outcome, including failure.
func (c *Client) GetConvertedFile(ctx context.Context, taskID string) (TaskStatus, *ResultResponse, error) {
	logger := log.FromContext(ctx)

	pollEndpoint, err := c.endpoint("/v1/status/poll", taskID)
	if err != nil {
		c.releaseTask(taskID)
		return "", nil, fmt.Errorf("building poll endpoint for task %s: %w", taskID, err)
	}

	body, err := c.do(ctx, http.MethodGet, pollEndpoint, nil)
	if err != nil {
		c.releaseTask(taskID)
		return "", nil, fmt.Errorf("polling task %s: %w", taskID, err)
	}

	var statusResponse StatusResponse
	if err := json.Unmarshal(body, &statusResponse); err != nil {
		c.releaseTask(taskID)
		return "", nil, fmt.Errorf("decoding poll response for task %s: %w", taskID, err)
	}

	switch statusResponse.TaskStatus {
	case TaskStatusPending, TaskStatusStarted:
		return statusResponse.TaskStatus, nil, nil

	case TaskStatusFailure:
		c.releaseTask(taskID)
		logger.Info("nemotron task failed",
			"taskID", taskID, "error", statusResponse.ErrorMessage)
		return TaskStatusFailure, nil, nil

	case TaskStatusSuccess, TaskStatusPartialSuccess:
		// fall through to collect the result

	default:
		c.releaseTask(taskID)
		return "", nil, fmt.Errorf("invalid task status %q for task %s",
			statusResponse.TaskStatus, taskID)
	}

	resultEndpoint, err := c.endpoint("/v1/result", taskID)
	if err != nil {
		c.releaseTask(taskID)
		return "", nil, fmt.Errorf("building result endpoint for task %s: %w", taskID, err)
	}

	body, err = c.do(ctx, http.MethodGet, resultEndpoint, nil)
	if err != nil {
		c.releaseTask(taskID)
		return "", nil, fmt.Errorf("fetching result for task %s: %w", taskID, err)
	}

	var result ResultResponse
	if err := json.Unmarshal(body, &result); err != nil {
		c.releaseTask(taskID)
		return "", nil, fmt.Errorf("decoding result for task %s: %w", taskID, err)
	}
	if result.Document == nil {
		c.releaseTask(taskID)
		return "", nil, fmt.Errorf("task %s reported %s but returned no document",
			taskID, result.Status)
	}

	c.releaseTask(taskID)
	logger.Info("nemotron task completed",
		"taskID", taskID, "status", result.Status,
		"pagesParsed", len(result.Document.Pages),
		"pagesInSource", result.Document.PageCount,
		"modelRequests", result.Document.ModelRequests)

	return result.Status, &result, nil
}

// FetchAsset downloads one crop or page image. Assets are addressed by the
// name the sidecar put on the block, and only exist until the task is released
// or ages out.
func (c *Client) FetchAsset(ctx context.Context, taskID, name string) ([]byte, error) {
	assetEndpoint, err := c.endpoint("/v1/result", taskID, "asset", name)
	if err != nil {
		return nil, fmt.Errorf("building asset endpoint for task %s: %w", taskID, err)
	}

	asset, err := c.do(ctx, http.MethodGet, assetEndpoint, nil)
	if err != nil {
		return nil, fmt.Errorf("fetching asset %s for task %s: %w", name, taskID, err)
	}
	return asset, nil
}

// Release drops a task and its assets from the sidecar. Best effort: the
// sidecar evicts on its own TTL, so a failure here costs scratch space rather
// than correctness.
func (c *Client) Release(ctx context.Context, taskID string) error {
	releaseEndpoint, err := c.endpoint("/v1/result", taskID)
	if err != nil {
		return fmt.Errorf("building release endpoint for task %s: %w", taskID, err)
	}
	if _, err := c.do(ctx, http.MethodDelete, releaseEndpoint, nil); err != nil {
		return fmt.Errorf("releasing task %s: %w", taskID, err)
	}
	return nil
}

// baseURL strips query and fragment, so a presigned URL can be logged without
// leaking its signature.
func baseURL(fullURL string) (string, error) {
	parsed, err := url.Parse(fullURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}
	parsed.RawQuery = ""
	parsed.Fragment = ""
	return parsed.String(), nil
}

func truncate(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	return s[:limit]
}
