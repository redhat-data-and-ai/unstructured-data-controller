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

	"github.com/imdario/mergo"
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

type TaskStatus string

// +kubebuilder:object:generate=true
type DoclingConfig struct {
	FromFormats     []string `json:"from_formats"`
	ToFormats       []string `json:"to_formats"`
	ImageExportMode string   `json:"image_export_mode"`
	DoOCR           bool     `json:"do_ocr"`
	ForceOCR        bool     `json:"force_ocr"`
	OCREngine       string   `json:"ocr_engine"`
	OCRLang         []string `json:"ocr_lang"`
	PDFBackend      string   `json:"pdf_backend"`
	TableMode       string   `json:"table_mode"`
	AbortOnError    bool     `json:"abort_on_error"`
}

type ClientConfig struct {
	URL                   string
	Key                   string
	MaxConcurrentRequests int64

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
	ClientConfig *ClientConfig `json:"client_config"`
}

type DoclingResponse struct {
	Document       DoclingResponseDocument `json:"document"`
	Status         TaskStatus              `json:"status"`
	ProcessingTime float64                 `json:"processing_time"`
	Errors         []string                `json:"errors"`
}

type DoclingResponseDocument struct {
	FileName       string `json:"filename"`
	MDContent      string `json:"md_content"`
	HTMLContent    string `json:"html_content"`
	TextContent    string `json:"text_content"`
	DocTagsContent string `json:"doctags_content"`
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

func NewClientFromURL(clientConfig *ClientConfig) *Client {
	clientConfig.sem = semaphore.NewWeighted(clientConfig.MaxConcurrentRequests)
	return &Client{
		ClientConfig: clientConfig,
	}
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

func mergeDoclingConfigs(doclingConfig DoclingConfig) (DoclingConfig, error) {
	defaultDoclingConfig := DoclingConfig{
		FromFormats:     []string{"pdf", "md", "docx", "pptx"},
		ToFormats:       []string{"md"},
		ImageExportMode: "embedded",
		DoOCR:           true,
		ForceOCR:        false,
		OCREngine:       "easyocr",
		PDFBackend:      "dlparse_v4",
		TableMode:       "fast",
		AbortOnError:    true,
	}

	err := mergo.Merge(&doclingConfig, defaultDoclingConfig, mergo.WithOverride)
	if err != nil {
		return DoclingConfig{}, fmt.Errorf("failed to merge docling configs: %w", err)
	}
	return doclingConfig, nil
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

func (c *Client) createDoclingRequest(ctx context.Context, method, endpoint string, payload []byte) (
	io.ReadCloser, error) {
	logger := log.FromContext(ctx)
	client := &http.Client{
		Timeout: 15 * time.Second,
	}

	req, err := c.createHTTPRequest(ctx, method, endpoint, payload, "Bearer %s")
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	logger.Info("sending request to docling service", "url", endpoint)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to send request: %w", err)
	}

	if resp.StatusCode == http.StatusForbidden && c.ClientConfig.Key != "" {
		req, err = c.createHTTPRequest(ctx, method, endpoint, payload, "%s")
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}
		resp, err = client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("failed to send request: %w", err)
		}
	}

	if resp.StatusCode != http.StatusOK {
		logger.Error(errors.New("received non-200 OK response from endpoint"),
			"status code", resp.StatusCode, "url", endpoint)
		return nil, fmt.Errorf("failed to process request: status code %d", resp.StatusCode)
	}

	return resp.Body, nil
}

func (c *Client) ConvertFile(
	ctx context.Context, fileURL string, doclingConfig DoclingConfig,
) (*AsyncDoclingResponse, error) {
	logger := log.FromContext(ctx)
	var err error

	finalDoclingConfig, err := mergeDoclingConfigs(doclingConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to merge docling configs: %w", err)
	}

	// we are using TryAcquire to avoid blocking the main thread
	acquired := c.ClientConfig.sem.TryAcquire(1)
	if !acquired {
		return nil, errors.New(SemaphoreAcquireError)
	}

	convertSourceAsyncEndpoint, err := c.convertSourceAsyncEndpoint()
	if err != nil {
		return nil, fmt.Errorf("failed to get convert source async endpoint: %w", err)
	}

	payload, err := json.Marshal(DoclingRequestPayload{
		Options: &finalDoclingConfig,
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

	logger.Info("sending request to convert file", "url", convertSourceAsyncEndpoint, "http source", fileURL)
	// convert response to AsyncDoclingResponse
	var asyncResponse AsyncDoclingResponse
	responseBody, err := c.createDoclingRequest(ctx, http.MethodPost, convertSourceAsyncEndpoint, payload)
	if err != nil {
		return nil, fmt.Errorf("failed to get response body: %w", err)
	}

	body, err := io.ReadAll(responseBody)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	if err = json.Unmarshal(body, &asyncResponse); err != nil {
		return nil, fmt.Errorf("failed to decode response: %w", err)
	}

	defer func() {
		if err = responseBody.Close(); err != nil {
			err = fmt.Errorf("failed to close response body: %w", err)
		}
	}()

	return &asyncResponse, err
}

func (c *Client) getTaskStatus(ctx context.Context, taskID string) (bool, *TaskStatusResponse, error) {
	logger := log.FromContext(ctx)

	getTaskStatusPollEndpoint, err := c.getTaskStatusPollEndpoint(taskID)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get task status poll endpoint: %w", err)
	}

	logger.Info("sending request to get status of task", "url", getTaskStatusPollEndpoint)
	var taskStatusResponse TaskStatusResponse
	bodyResponse, err := c.createDoclingRequest(ctx, http.MethodGet, getTaskStatusPollEndpoint, nil)
	if err != nil {
		return false, nil, fmt.Errorf("failed to get response body: %w", err)
	}

	if err := json.NewDecoder(bodyResponse).Decode(&taskStatusResponse); err != nil {
		return false, nil, fmt.Errorf("failed to decode response: %w", err)
	}

	defer func() {
		if err = bodyResponse.Close(); err != nil {
			err = fmt.Errorf("failed to close response body: %w", err)
		}
	}()

	return true, &taskStatusResponse, nil
}

func (c *Client) GetConvertedFile(ctx context.Context, taskID string) (TaskStatus, *DoclingResponse, error) {
	logger := log.FromContext(ctx)

	// get the task status
	isValidStatus, taskStatus, err := c.getTaskStatus(ctx, taskID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get task status: %w", err)
	}

	if !isValidStatus {
		// for some reason invalid task status received, we will return the error and release the semaphore
		c.ClientConfig.sem.Release(1)
		return "", nil, fmt.Errorf("invalid task status received for task id: %s", taskID)
	}

	if taskStatus.TaskStatus == TaskStatusFailure {
		// for some reason invalid task status received, we will return the error and release the semaphore
		c.ClientConfig.sem.Release(1)
		return "", nil, fmt.Errorf("task failed: task id: %s", taskID)
	}

	// if it is started or pending, we will return it as it is
	if taskStatus.TaskStatus == TaskStatusStarted || taskStatus.TaskStatus == TaskStatusPending {
		return taskStatus.TaskStatus, nil, nil
	}

	// let's fetch the result from the task now
	taskResultURL, err := c.getTaskResultEndpoint(taskID)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get task result endpoint: %w", err)
	}

	logger.Info("sending request to get converted file", "url", taskResultURL)
	var doclingResponse DoclingResponse
	bodyResponse, err := c.createDoclingRequest(ctx, http.MethodGet, taskResultURL, nil)
	if err != nil {
		return "", nil, fmt.Errorf("failed to get response body: %w", err)
	}

	if err := json.NewDecoder(bodyResponse).Decode(&doclingResponse); err != nil {
		return "", nil, fmt.Errorf("failed to decode response: %w", err)
	}

	defer func() {
		if err = bodyResponse.Close(); err != nil {
			err = fmt.Errorf("failed to close response body: %w", err)
		}
	}()

	// now let's free up the semaphore based on task status
	switch doclingResponse.Status {
	case TaskStatusPending, TaskStatusStarted:
		// this is not likely to happen because we have already handled these above, but just in case
		logger.Info("task is still pending or started, will try again later", "task id", taskID)
	case TaskStatusFailure, TaskStatusSkipped:
		// free up the semaphore because the processing in docling is completed
		logger.Error(fmt.Errorf("task failed: task id: %s", taskID), "task failed")
		c.ClientConfig.sem.Release(1)
	case TaskStatusPartialSuccess, TaskStatusSuccess:
		// free up the semaphore because the processing in docling is completed
		logger.Info("task completed successfully", "task id", taskID)
		c.ClientConfig.sem.Release(1)
	default:
		return "", nil, fmt.Errorf("invalid task status received for task id: %s", taskID)
	}

	return doclingResponse.Status, &doclingResponse, nil
}
