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

package nemotron

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const presignedURL = "https://bucket.s3.amazonaws.com/doc.pdf?X-Amz-Signature=secret"

// newTestClient wires a Client to a stub sidecar.
func newTestClient(t *testing.T, handler http.HandlerFunc, maxConcurrent int64) (*Client, *httptest.Server) {
	t.Helper()
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	client := NewClientFromURL(&ClientConfig{
		URL:                   server.URL,
		Key:                   "test-key",
		MaxConcurrentRequests: maxConcurrent,
		HTTPTimeout:           5 * time.Second,
	})
	return client, server
}

func submitOnly(taskID string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(SubmitResponse{
			TaskID: taskID, TaskStatus: TaskStatusPending,
		})
	}
}

func TestConvertFile_ReturnsTaskID(t *testing.T) {
	client, _ := newTestClient(t, submitOnly("task-123"), 4)

	response, err := client.ConvertFile(context.Background(), presignedURL, ParseOptions{Mode: "double"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if response.TaskID != "task-123" {
		t.Errorf("expected task id %q, got %q", "task-123", response.TaskID)
	}
	if response.TaskStatus != TaskStatusPending {
		t.Errorf("expected status %q, got %q", TaskStatusPending, response.TaskStatus)
	}
}

func TestConvertFile_SendsSourceAndOptions(t *testing.T) {
	var received parseRequest
	var authHeader string

	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/parse/source/async" {
			t.Errorf("expected submit path, got %q", r.URL.Path)
		}
		authHeader = r.Header.Get("Authorization")
		body, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(body, &received)
		_ = json.NewEncoder(w).Encode(SubmitResponse{TaskID: "t", TaskStatus: TaskStatusPending})
	}, 4)

	options := ParseOptions{Mode: "double", DPI: 200, CropSkipClasses: []string{"Page-header"}}
	if _, err := client.ConvertFile(context.Background(), presignedURL, options); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(received.Sources) != 1 {
		t.Fatalf("expected exactly one source, got %d", len(received.Sources))
	}
	// The full presigned URL, signature included, must reach the sidecar.
	if received.Sources[0].URL != presignedURL {
		t.Errorf("expected source url %q, got %q", presignedURL, received.Sources[0].URL)
	}
	if received.Sources[0].Kind != "http" {
		t.Errorf("expected kind %q, got %q", "http", received.Sources[0].Kind)
	}
	if received.Options.Mode != "double" || received.Options.DPI != 200 {
		t.Errorf("options not forwarded: %+v", received.Options)
	}
	if authHeader != "Bearer test-key" {
		t.Errorf("expected bearer auth, got %q", authHeader)
	}
}

func TestConvertFile_SemaphoreBoundsInFlightSubmissions(t *testing.T) {
	client, _ := newTestClient(t, submitOnly("task-1"), 1)

	if _, err := client.ConvertFile(context.Background(), presignedURL, ParseOptions{}); err != nil {
		t.Fatalf("first submit should succeed: %v", err)
	}

	_, err := client.ConvertFile(context.Background(), presignedURL, ParseOptions{})
	if err == nil {
		t.Fatal("expected the second submit to be rejected")
	}
	if !strings.Contains(err.Error(), SemaphoreAcquireError) {
		t.Errorf("expected %q, got %q", SemaphoreAcquireError, err.Error())
	}
}

func TestConvertFile_SidecarAtCapacityIsDistinctFromFailure(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"detail":"task store at capacity"}`))
	}, 4)

	_, err := client.ConvertFile(context.Background(), presignedURL, ParseOptions{})
	if err == nil {
		t.Fatal("expected an error on 429")
	}
	if !strings.Contains(err.Error(), SidecarAtCapacityError) {
		t.Errorf("expected %q, got %q", SidecarAtCapacityError, err.Error())
	}
}

func TestConvertFile_ReleasesSemaphoreOnFailure(t *testing.T) {
	calls := 0
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		calls++
		if calls == 1 {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		_ = json.NewEncoder(w).Encode(SubmitResponse{TaskID: "t", TaskStatus: TaskStatusPending})
	}, 1)

	if _, err := client.ConvertFile(context.Background(), presignedURL, ParseOptions{}); err == nil {
		t.Fatal("expected the first submit to fail")
	}
	// A failed submit must not consume the only slot.
	if _, err := client.ConvertFile(context.Background(), presignedURL, ParseOptions{}); err != nil {
		t.Errorf("slot was not released after a failed submit: %v", err)
	}
}

func TestConvertFile_RejectsEmptyTaskID(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(SubmitResponse{TaskStatus: TaskStatusPending})
	}, 4)

	_, err := client.ConvertFile(context.Background(), presignedURL, ParseOptions{})
	if err == nil || !strings.Contains(err.Error(), "empty task id") {
		t.Errorf("expected an empty-task-id error, got %v", err)
	}
}

func TestIsAcquiredTask(t *testing.T) {
	client, _ := newTestClient(t, submitOnly("task-abc"), 4)

	if client.IsAcquiredTask("task-abc") {
		t.Error("task should not be acquired before submission")
	}
	if _, err := client.ConvertFile(context.Background(), presignedURL, ParseOptions{}); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !client.IsAcquiredTask("task-abc") {
		t.Error("task should be acquired after submission")
	}
	// A task submitted by a previous process is unknown to this one, which is
	// how restart-stranded jobs get dropped rather than polled forever.
	if client.IsAcquiredTask("task-from-before-restart") {
		t.Error("unknown task should not report as acquired")
	}
}

// pollThen serves a poll status, then a result payload on the result path.
func pollThen(status TaskStatus, result *ResultResponse, errorMessage string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		switch {
		case strings.HasPrefix(r.URL.Path, "/v1/status/poll/"):
			_ = json.NewEncoder(w).Encode(StatusResponse{
				TaskID: "t", TaskStatus: status, ErrorMessage: errorMessage,
			})
		case strings.HasPrefix(r.URL.Path, "/v1/result/"):
			_ = json.NewEncoder(w).Encode(result)
		default:
			_ = json.NewEncoder(w).Encode(SubmitResponse{TaskID: "t", TaskStatus: TaskStatusPending})
		}
	}
}

func successResult(status TaskStatus, pages int, pageCount int, errs []string) *ResultResponse {
	document := &Document{PageCount: pageCount, Mode: "double", ModelRequests: pages * 2}
	for i := 1; i <= pages; i++ {
		document.Pages = append(document.Pages, Page{
			PageNumber: i,
			Markdown:   "# Page",
			Blocks: []Block{
				{Index: 0, Class: "Table", Text: `\begin{tabular}`, Content: "<table></table>",
					BBox: []float64{0.1, 0.2, 0.9, 0.4}, Crop: "p001_b000.png"},
			},
		})
	}
	return &ResultResponse{TaskID: "t", Status: status, Errors: errs, Document: document}
}

func TestGetConvertedFile_NonTerminalStatusReturnsNoResult(t *testing.T) {
	for _, status := range []TaskStatus{TaskStatusPending, TaskStatusStarted} {
		t.Run(string(status), func(t *testing.T) {
			client, _ := newTestClient(t, pollThen(status, nil, ""), 4)

			got, result, err := client.GetConvertedFile(context.Background(), "t")
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != status {
				t.Errorf("expected status %q, got %q", status, got)
			}
			if result != nil {
				t.Error("expected no result while the parse is still running")
			}
		})
	}
}

func TestGetConvertedFile_SuccessReturnsDocument(t *testing.T) {
	client, _ := newTestClient(t,
		pollThen(TaskStatusSuccess, successResult(TaskStatusSuccess, 2, 2, nil), ""), 4)

	status, result, err := client.GetConvertedFile(context.Background(), "t")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != TaskStatusSuccess {
		t.Errorf("expected %q, got %q", TaskStatusSuccess, status)
	}
	if len(result.Document.Pages) != 2 {
		t.Fatalf("expected 2 pages, got %d", len(result.Document.Pages))
	}
	block := result.Document.Pages[0].Blocks[0]
	if block.Class != "Table" {
		t.Errorf("expected a Table block, got %q", block.Class)
	}
	if block.Content != "<table></table>" {
		t.Errorf("expected HTML content, got %q", block.Content)
	}
	if block.Crop != "p001_b000.png" {
		t.Errorf("expected a crop asset name, got %q", block.Crop)
	}
}

func TestGetConvertedFile_PartialSuccessReportsGapAndErrors(t *testing.T) {
	// Two pages parsed out of three; the caller detects the hole by comparing
	// PageCount against len(Pages).
	result := successResult(TaskStatusPartialSuccess, 2, 3, []string{"page 3: model returned HTTP 500"})
	client, _ := newTestClient(t, pollThen(TaskStatusPartialSuccess, result, ""), 4)

	status, got, err := client.GetConvertedFile(context.Background(), "t")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != TaskStatusPartialSuccess {
		t.Errorf("expected %q, got %q", TaskStatusPartialSuccess, status)
	}
	if got.Document.PageCount != 3 || len(got.Document.Pages) != 2 {
		t.Errorf("expected 2 of 3 pages, got %d of %d",
			len(got.Document.Pages), got.Document.PageCount)
	}
	if len(got.Errors) != 1 {
		t.Errorf("expected one page error, got %v", got.Errors)
	}
}

func TestGetConvertedFile_FailureReturnsNoDocument(t *testing.T) {
	client, _ := newTestClient(t, pollThen(TaskStatusFailure, nil, "source returned HTTP 403"), 4)

	status, result, err := client.GetConvertedFile(context.Background(), "t")
	if err != nil {
		t.Fatalf("a failed task is a status, not a transport error: %v", err)
	}
	if status != TaskStatusFailure {
		t.Errorf("expected %q, got %q", TaskStatusFailure, status)
	}
	if result != nil {
		t.Error("expected no result for a failed task")
	}
}

func TestGetConvertedFile_RejectsUnknownStatus(t *testing.T) {
	client, _ := newTestClient(t, pollThen(TaskStatus("confused"), nil, ""), 4)

	_, _, err := client.GetConvertedFile(context.Background(), "t")
	if err == nil || !strings.Contains(err.Error(), "invalid task status") {
		t.Errorf("expected an invalid-status error, got %v", err)
	}
}

func TestGetConvertedFile_RejectsSuccessWithoutDocument(t *testing.T) {
	empty := &ResultResponse{TaskID: "t", Status: TaskStatusSuccess}
	client, _ := newTestClient(t, pollThen(TaskStatusSuccess, empty, ""), 4)

	_, _, err := client.GetConvertedFile(context.Background(), "t")
	if err == nil || !strings.Contains(err.Error(), "no document") {
		t.Errorf("expected a missing-document error, got %v", err)
	}
}

func TestGetConvertedFile_MissingTaskIsAnError(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`{"detail":"task not found"}`))
	}, 4)

	_, _, err := client.GetConvertedFile(context.Background(), "gone")
	if err == nil || !strings.Contains(err.Error(), "404") {
		t.Errorf("expected a 404 error, got %v", err)
	}
}

func TestGetConvertedFile_ReleasesSlotOnTerminalStatus(t *testing.T) {
	result := successResult(TaskStatusSuccess, 1, 1, nil)
	client, _ := newTestClient(t, pollThen(TaskStatusSuccess, result, ""), 1)

	submitted, err := client.ConvertFile(context.Background(), presignedURL, ParseOptions{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, _, err := client.GetConvertedFile(context.Background(), submitted.TaskID); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if client.IsAcquiredTask(submitted.TaskID) {
		t.Error("task should no longer be acquired after completing")
	}
	// The single slot must be free again.
	if _, err := client.ConvertFile(context.Background(), presignedURL, ParseOptions{}); err != nil {
		t.Errorf("slot was not released after completion: %v", err)
	}
}

func TestFetchAsset_ReturnsBytes(t *testing.T) {
	png := []byte("\x89PNG\r\n\x1a\nfake")
	var requestedPath string

	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		requestedPath = r.URL.Path
		w.Header().Set("Content-Type", "image/png")
		_, _ = w.Write(png)
	}, 4)

	asset, err := client.FetchAsset(context.Background(), "task-1", "p001_b002.png")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if string(asset) != string(png) {
		t.Errorf("asset bytes did not round-trip")
	}
	if requestedPath != "/v1/result/task-1/asset/p001_b002.png" {
		t.Errorf("unexpected asset path %q", requestedPath)
	}
}

func TestFetchAsset_MissingAssetIsAnError(t *testing.T) {
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotFound)
	}, 4)

	if _, err := client.FetchAsset(context.Background(), "t", "p001_b000.png"); err == nil {
		t.Error("expected an error for a missing asset")
	}
}

func TestRelease_SendsDelete(t *testing.T) {
	var method, path string
	client, _ := newTestClient(t, func(w http.ResponseWriter, r *http.Request) {
		method, path = r.Method, r.URL.Path
		_, _ = w.Write([]byte(`{"released":true}`))
	}, 4)

	if err := client.Release(context.Background(), "task-9"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if method != http.MethodDelete {
		t.Errorf("expected DELETE, got %s", method)
	}
	if path != "/v1/result/task-9" {
		t.Errorf("unexpected release path %q", path)
	}
}

func TestBaseURL_StripsPresignedSignature(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		{
			name: "presigned url loses its query",
			in:   "https://bucket.s3.amazonaws.com/a/doc.pdf?X-Amz-Signature=secret&X-Amz-Expires=900",
			want: "https://bucket.s3.amazonaws.com/a/doc.pdf",
		},
		{
			name: "fragment is dropped",
			in:   "https://host/doc.pdf#page=2",
			want: "https://host/doc.pdf",
		},
		{
			name: "plain url is unchanged",
			in:   "https://host/doc.pdf",
			want: "https://host/doc.pdf",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := baseURL(tt.in)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tt.want {
				t.Errorf("expected %q, got %q", tt.want, got)
			}
			if strings.Contains(got, "Signature") {
				t.Error("signature leaked into the logged url")
			}
		})
	}
}
