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

package docling

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/httpclient"
	"golang.org/x/sync/semaphore"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	TaskStatusPending TaskStatus = "pending" // this is returned by /v1/status/poll and /v1/result endpoint
	TaskStatusStarted TaskStatus = "started" // this is returned by /v1/status/poll and /v1/result endpoint
	TaskStatusSuccess TaskStatus = "success" // this is returned by /v1/status/poll and /v1/result endpoint
	TaskStatusFailure TaskStatus = "failure" // this is returned by /v1/status/poll and /v1/result endpoint

	TaskStatusPartialSuccess TaskStatus = "partial_success" // this is only returned by /v1/result endpoint
	TaskStatusSkipped        TaskStatus = "skipped"         // this is only returned by /v1/result endpoint

	SemaphoreAcquireError = "failed to acquire docling semaphore"
	SemaphorePanicError   = "semaphore: released more than held"
)

// ErrTaskNotFound reports that docling has no record of a task ID, typically
// after a docling restart or task expiry.
var ErrTaskNotFound = errors.New("docling task not found")

// DoclingHTTPError is returned for a non-200 docling response; match it with errors.As.
type DoclingHTTPError struct {
	StatusCode int
	Endpoint   string
}

// redactEndpoint strips any userinfo from endpoint, since the error can reach
// resource status.
func redactEndpoint(endpoint string) string {
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return "<unparseable docling URL>"
	}
	return parsed.Redacted()
}

func (e *DoclingHTTPError) Error() string {
	return fmt.Sprintf("docling request to %s returned status %d", e.Endpoint, e.StatusCode)
}

// ErrSemaphoreUnavailable reports that the client is already at its configured
// concurrency and refused the work rather than queueing for it. Callers should
// retry later instead of treating it as a failure.
var ErrSemaphoreUnavailable = errors.New(SemaphoreAcquireError)

type TaskStatus string

type PictureDescriptionAPIParams struct {
	Model     string `json:"model"`
	MaxTokens int    `json:"max_tokens,omitempty"`
}

type PictureDescriptionAPI struct {
	URL         string                      `json:"url"`
	Params      PictureDescriptionAPIParams `json:"params"`
	Prompt      string                      `json:"prompt,omitempty"`
	Timeout     float64                     `json:"timeout,omitempty"`
	Concurrency int                         `json:"concurrency,omitempty"`
	Headers     map[string]string           `json:"headers,omitempty"`
}

// VLMEngineTypeAPI selects docling's OpenAI-compatible HTTP engine. It has to
// be sent explicitly: omit it and no engine matches, so the request is
// rejected.
const VLMEngineTypeAPI = "api"

// VLMModelSpec is the model half of `vlm_pipeline_custom_config`: what to ask
// for, independent of where it is served. Docling requires every field; Name
// is free text it only reports back.
type VLMModelSpec struct {
	Name string `json:"name"`
	// DefaultRepoID is the model docling asks the endpoint for, and the only
	// place it may be set. Docling derives the request's `model` parameter
	// from it only while `engine_options.params` is absent, even an empty one,
	// which is why VLMEngineOptions has no field for params.
	DefaultRepoID string `json:"default_repo_id"`
	// Prompt selects which outputs the model produces, as a row of control
	// tokens. A wrong one returns a document that parses cleanly and is missing
	// whatever was not asked for.
	Prompt string `json:"prompt"`
	// ResponseFormat names the decoder docling applies to the reply. An unknown
	// value fails opaquely: the task ends in failure and the result endpoint
	// answers 404 without naming the field.
	ResponseFormat string `json:"response_format"`
}

// VLMEngineOptions is the transport half of `vlm_pipeline_custom_config`:
// where the model is reached and how to authenticate. That split is why the
// credential can live in the controller's Secret while the model settings live
// on the CR.
type VLMEngineOptions struct {
	EngineType string            `json:"engine_type"`
	URL        string            `json:"url"`
	Headers    map[string]string `json:"headers,omitempty"`
}

// VLMPipelineCustomConfig is docling-serve's `vlm_pipeline_custom_config`: the
// remote vision-language model the "vlm" pipeline calls instead of one of the
// bundled presets. The server needs DOCLING_SERVE_ENABLE_REMOTE_SERVICES and
// DOCLING_SERVE_ALLOW_CUSTOM_VLM_CONFIG for it.
//
// It replaces the deprecated `vlm_pipeline_model_api`. Docling discards an
// option name it does not recognise and still returns 200, so a wrong name
// here quietly runs the standard pipeline instead of failing.
type VLMPipelineCustomConfig struct {
	ModelSpec     VLMModelSpec     `json:"model_spec"`
	EngineOptions VLMEngineOptions `json:"engine_options"`
	// Scale is the rasterisation factor applied to each page before the model
	// sees it, where 1.0 is 72 DPI. Omitted rather than zeroed when unset, so
	// the server applies its own default instead of rendering at nothing.
	Scale *float64 `json:"scale,omitempty"`
	// MaxSize caps a rasterised page's longest edge in pixels. A page over it
	// is scaled down to fit, which silently lowers the effective Scale.
	MaxSize int `json:"max_size,omitempty"`
}

type DoclingConfig struct {
	FromFormats                     []string                 `json:"from_formats"`
	ToFormats                       []string                 `json:"to_formats"`
	ImageExportMode                 string                   `json:"image_export_mode"`
	DoOCR                           bool                     `json:"do_ocr"`
	ForceOCR                        bool                     `json:"force_ocr"`
	OCREngine                       string                   `json:"ocr_engine,omitempty"`
	OCRLang                         []string                 `json:"ocr_lang"`
	OCRPreset                       string                   `json:"ocr_preset,omitempty"`
	PDFBackend                      string                   `json:"pdf_backend"`
	Pipeline                        string                   `json:"pipeline,omitempty"`
	VLMPipelineCustomConfig         *VLMPipelineCustomConfig `json:"vlm_pipeline_custom_config,omitempty"`
	TableMode                       string                   `json:"table_mode"`
	TableCellMatching               *bool                    `json:"table_cell_matching,omitempty"`
	DoTableStructure                *bool                    `json:"do_table_structure,omitempty"`
	IncludeImages                   *bool                    `json:"include_images,omitempty"`
	IncludePageImages               *bool                    `json:"include_page_images,omitempty"`
	ImagesScale                     *float64                 `json:"images_scale,omitempty"`
	DoCodeEnrichment                bool                     `json:"do_code_enrichment,omitempty"`
	DoFormulaEnrichment             bool                     `json:"do_formula_enrichment,omitempty"`
	DoPictureClassification         bool                     `json:"do_picture_classification,omitempty"`
	DoPictureDescription            bool                     `json:"do_picture_description,omitempty"`
	DoChartExtraction               bool                     `json:"do_chart_extraction,omitempty"`
	PictureDescriptionAPI           *PictureDescriptionAPI   `json:"picture_description_api,omitempty"`
	PictureDescriptionAreaThreshold *float64                 `json:"picture_description_area_threshold,omitempty"`
	DocumentTimeout                 *float64                 `json:"document_timeout,omitempty"`
	PageRange                       []int                    `json:"page_range,omitempty"`
	MdPageBreakPlaceholder          string                   `json:"md_page_break_placeholder,omitempty"`
	AbortOnError                    bool                     `json:"abort_on_error"`
}

type ClientConfig struct {
	URL                   string
	Key                   string
	MaxConcurrentRequests int64
	HTTPTimeout           time.Duration
	// RetryPolicy, when set, governs every retried request. Nil means the defaults:
	// httpclient.DefaultPolicy, and a short budget for status polls.
	RetryPolicy *httpclient.Policy

	sem *semaphore.Weighted
}

type DoclingSource struct {
	URL     string            `json:"url"`
	Kind    string            `json:"kind"`
	Headers map[string]string `json:"headers,omitempty"`
}

type DoclingRequestPayload struct {
	Options *DoclingConfig  `json:"options"`
	Sources []DoclingSource `json:"sources"`
}

type Client struct {
	ClientConfig    *ClientConfig `json:"client_config"`
	retryHTTPClient *http.Client  // reused across requests for connection pooling
	pollHTTPClient  *http.Client  // short retry budget, for status polls
	plainHTTPClient *http.Client  // no retry, for non-idempotent requests like task creation
}

type DoclingResponse struct {
	Document       DoclingResponseDocument `json:"document"`
	Status         TaskStatus              `json:"status"`
	ProcessingTime float64                 `json:"processing_time"`
	Errors         []string                `json:"errors"`
}

type DoclingResponseDocument struct {
	FileName  string `json:"filename"`
	MDContent string `json:"md_content"`
	// JSONContent is the DoclingDocument, returned only when to_formats
	// includes "json". It is carried as raw bytes because addresses such as
	// "#/texts/59" index into its arrays, so a round trip through our own
	// types would move them. Prefer JSONDocument() over reading it directly.
	JSONContent    json.RawMessage `json:"json_content"`
	HTMLContent    string          `json:"html_content"`
	TextContent    string          `json:"text_content"`
	DocTagsContent string          `json:"doctags_content"`
}

// JSONDocument returns the DoclingDocument, or nil when docling did
// not return one.
//
// The field is absent when nothing asked for it and is the JSON literal null
// when docling ran but produced no document. json.RawMessage keeps both as
// bytes, and "null" is four of them, so len(JSONContent) > 0 is not a valid
// test for "a document came back".
func (d *DoclingResponseDocument) JSONDocument() json.RawMessage {
	trimmed := bytes.TrimSpace(d.JSONContent)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil
	}
	return d.JSONContent
}

type AsyncDoclingResponse struct {
	TaskID       string `json:"task_id"`
	TaskStatus   string `json:"task_status"`
	TaskPosition int    `json:"task_position,omitempty"`
}

type TaskStatusResponse struct {
	TaskID       string     `json:"task_id"`
	TaskStatus   TaskStatus `json:"task_status"`
	TaskPosition int        `json:"task_position"`
	ErrorMessage string     `json:"error_message,omitempty"`
}

// statusPollPolicy is short because the reconcile requeue already repeats a poll.
var statusPollPolicy = httpclient.Policy{RetryMax: 2, WaitMin: time.Second, WaitMax: 5 * time.Second}

// pollPolicy returns override if set, else statusPollPolicy.
func pollPolicy(override *httpclient.Policy) httpclient.Policy {
	if override != nil {
		return *override
	}
	return statusPollPolicy
}

func NewClientFromURL(clientConfig *ClientConfig) *Client {
	clientConfig.sem = semaphore.NewWeighted(clientConfig.MaxConcurrentRequests)
	return &Client{
		ClientConfig:    clientConfig,
		retryHTTPClient: httpclient.New(clientConfig.HTTPTimeout, httpclient.PolicyOrDefault(clientConfig.RetryPolicy)),
		pollHTTPClient:  httpclient.New(clientConfig.HTTPTimeout, pollPolicy(clientConfig.RetryPolicy)),
		plainHTTPClient: &http.Client{Timeout: clientConfig.HTTPTimeout},
	}
}

// safeRelease releases a semaphore slot without panicking if the semaphore
// has no outstanding acquisitions (e.g. after a controller restart with stale jobs).
func (c *Client) safeRelease() {
	defer func() {
		_ = recover()
	}()
	c.ClientConfig.sem.Release(1)
}

func (c *Client) convertSourceAsyncEndpoint() (string, error) {
	return url.JoinPath(c.ClientConfig.URL, "/v1/convert/source/async")
}

func (c *Client) getTaskStatusPollEndpoint(taskID string) (string, error) {
	return url.JoinPath(c.ClientConfig.URL, "/v1/status/poll", taskID)
}

func (c *Client) getTaskResultEndpoint(taskID string) (string, error) {
	return url.JoinPath(c.ClientConfig.URL, "/v1/result", taskID)
}

func (c *Client) createHTTPRequest(ctx context.Context, method, endpoint string, payload []byte, authFormat string) (
	*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, endpoint, bytes.NewReader(payload))
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if c.ClientConfig.Key != "" {
		req.Header.Set("Authorization", fmt.Sprintf(authFormat, c.ClientConfig.Key))
	}
	return req, nil
}

// sendWithAuthFallback sends an HTTP request using the given client, trying
// "Bearer <key>" first. If the server returns 403 and we have a key configured,
// it retries with the raw key format. This handles docling deployments that
// expect different auth header formats.
func (c *Client) sendWithAuthFallback(
	ctx context.Context, httpClient *http.Client, method, endpoint string, payload []byte,
) (*http.Response, error) {
	logger := log.FromContext(ctx)

	req, err := c.createHTTPRequest(ctx, method, endpoint, payload, "Bearer %s")
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	logger.Info("sending request to docling service", "url", endpoint)
	resp, err := httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	// Fall back to raw key auth format if Bearer token got 403.
	if resp.StatusCode == http.StatusForbidden && c.ClientConfig.Key != "" {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Error(closeErr, "failed to close response body before auth fallback")
		}
		req, err = c.createHTTPRequest(ctx, method, endpoint, payload, "%s")
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		resp, err = httpClient.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send request: %w", err)
		}
	}

	return resp, nil
}

// doDoclingRequest sends a request with auth fallback and returns the body, or
// a *DoclingHTTPError for a non-200. Pass c.plainHTTPClient for task-creating
// POSTs: a retried submit can start a duplicate task that no slot covers and nothing collects.
func (c *Client) doDoclingRequest(
	ctx context.Context, httpClient *http.Client, method, endpoint string, payload []byte,
) (io.ReadCloser, error) {
	logger := log.FromContext(ctx)

	resp, err := c.sendWithAuthFallback(ctx, httpClient, method, endpoint, payload)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		if closeErr := resp.Body.Close(); closeErr != nil {
			logger.Error(closeErr, "failed to close response body for non-200 response")
		}
		return nil, &DoclingHTTPError{StatusCode: resp.StatusCode, Endpoint: redactEndpoint(endpoint)}
	}

	return resp.Body, nil
}

func (c *Client) ConvertFile(
	ctx context.Context, fileURL string, doclingConfig DoclingConfig,
) (*AsyncDoclingResponse, error) {
	logger := log.FromContext(ctx)

	// we are using TryAcquire to avoid blocking the main thread
	acquired := c.ClientConfig.sem.TryAcquire(1)
	if !acquired {
		return nil, ErrSemaphoreUnavailable
	}
	// release the semaphore if the request fails before docling accepts the task
	success := false
	defer func() {
		if !success {
			c.ClientConfig.sem.Release(1)
		}
	}()

	convertSourceAsyncEndpoint, err := c.convertSourceAsyncEndpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to get convert source async endpoint: %w", err)
	}

	payload, err := json.Marshal(DoclingRequestPayload{
		Options: &doclingConfig,
		Sources: []DoclingSource{
			{
				URL:  fileURL,
				Kind: "http",
			},
		},
	})
	if err != nil {
		return nil, fmt.Errorf("failed to marshal config to docling payload: %w", err)
	}

	baseURL, err := getBaseURL(fileURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse presigned url: %w", err)
	}

	logger.Info("sending request to convert file", "urlToSendRequest",
		convertSourceAsyncEndpoint, "sourceFileURL", baseURL)
	var asyncResponse AsyncDoclingResponse
	responseBody, err := c.doDoclingRequest(ctx, c.plainHTTPClient, http.MethodPost, convertSourceAsyncEndpoint, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to send convert request: %w", err)
	}
	defer func() {
		if closeErr := responseBody.Close(); closeErr != nil {
			logger.Error(closeErr, "failed to close response body")
		}
	}()

	body, err := io.ReadAll(responseBody)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if err = json.Unmarshal(body, &asyncResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	success = true
	return &asyncResponse, nil
}

func (c *Client) getTaskStatus(ctx context.Context, taskID string) (bool, *TaskStatusResponse, error) {
	logger := log.FromContext(ctx)

	getTaskStatusPollEndpoint, err := c.getTaskStatusPollEndpoint(taskID)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get task status poll endpoint: %w", err)
	}

	logger.Info("sending request to get status of task", "url", getTaskStatusPollEndpoint)
	var taskStatusResponse TaskStatusResponse
	bodyResponse, err := c.doDoclingRequest(ctx, c.pollHTTPClient, http.MethodGet, getTaskStatusPollEndpoint, nil)
	if err != nil {
		// Only a 404 from the poll endpoint means docling lost the task; a 404
		// from the result endpoint does not, so this check is not in doDoclingRequest.
		var httpErr *DoclingHTTPError
		if errors.As(err, &httpErr) && httpErr.StatusCode == http.StatusNotFound {
			return false, nil, ErrTaskNotFound
		}
		return false, nil, fmt.Errorf("failed to get response body: %w", err)
	}
	defer func() {
		if err = bodyResponse.Close(); err != nil {
			err = fmt.Errorf("failed to close response body: %w", err)
		}
	}()

	if err := json.NewDecoder(bodyResponse).Decode(&taskStatusResponse); err != nil {
		return false, nil, fmt.Errorf("failed to decode response: %w", err)
	}

	return true, &taskStatusResponse, nil
}

func (c *Client) GetConvertedFile(ctx context.Context, taskID string) (TaskStatus, *DoclingResponse, error) {
	logger := log.FromContext(ctx)

	// get the task status
	isValidStatus, taskStatus, err := c.getTaskStatus(ctx, taskID)
	if err != nil {
		c.safeRelease()
		return "", nil, fmt.Errorf("failed to get task status: %w", err)
	}

	if !isValidStatus {
		// for some reason invalid task status received, we will return the error and release the semaphore
		c.safeRelease()
		return "", nil, fmt.Errorf("invalid task status received for task id: %s", taskID)
	}

	if taskStatus.TaskStatus == TaskStatusFailure {
		// task failed, release the semaphore
		c.safeRelease()
		return "", nil, fmt.Errorf("task failed: task id: %s", taskID)
	}

	// if it is started or pending, we will return it as it is
	if taskStatus.TaskStatus == TaskStatusStarted || taskStatus.TaskStatus == TaskStatusPending {
		return taskStatus.TaskStatus, nil, nil
	}

	// let's fetch the result from the task now
	taskResultURL, err := c.getTaskResultEndpoint(taskID)
	if err != nil {
		c.safeRelease()
		return "", nil, fmt.Errorf("failed to get task result endpoint: %w", err)
	}

	logger.Info("sending request to get converted file", "url", taskResultURL)
	var doclingResponse DoclingResponse
	bodyResponse, err := c.doDoclingRequest(ctx, c.retryHTTPClient, http.MethodGet, taskResultURL, nil)
	if err != nil {
		c.safeRelease()
		return "", nil, fmt.Errorf("failed to get response body: %w", err)
	}
	defer func() { _ = bodyResponse.Close() }()

	if err := json.NewDecoder(bodyResponse).Decode(&doclingResponse); err != nil {
		c.safeRelease()
		return "", nil, fmt.Errorf("failed to decode response: %w", err)
	}

	// now let's free up the semaphore based on task status
	switch doclingResponse.Status {
	case TaskStatusPending, TaskStatusStarted:
		// this is not likely to happen because we have already handled these above, but just in case
		logger.Info("task is still pending or started, will try again later", "task id", taskID)
	case TaskStatusFailure, TaskStatusSkipped:
		// free up the semaphore because the processing in docling is completed
		logger.Error(fmt.Errorf("task failed: task id: %s", taskID), "task failed")
		c.safeRelease()
	case TaskStatusPartialSuccess, TaskStatusSuccess:
		// free up the semaphore because the processing in docling is completed
		logger.Info("task completed successfully", "task id", taskID)
		c.safeRelease()
	default:
		c.safeRelease()
		return "", nil, fmt.Errorf("invalid task status received for task id: %s", taskID)
	}

	return doclingResponse.Status, &doclingResponse, nil
}

// getBaseURl will return url without query and fragments, only hostname and path
func getBaseURL(fullURL string) (string, error) {
	u, err := url.Parse(fullURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}

	// set query and fragment as empty
	u.RawQuery = ""
	u.Fragment = ""

	return u.String(), nil
}
