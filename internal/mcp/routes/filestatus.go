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

package routes

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestatus"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
)

type FileStatusHandler struct {
	k8sClient *k8sclient.Client
	querier   filestatus.StatusQuerier
}

func NewFileStatusHandler(k8sClient *k8sclient.Client, querier filestatus.StatusQuerier) *FileStatusHandler {
	return &FileStatusHandler{k8sClient: k8sClient, querier: querier}
}

func (h *FileStatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pipelineName := r.PathValue("pipeline_name")
	if pipelineName == "" {
		writeJSONError(w, "pipeline_name is required", http.StatusBadRequest)
		return
	}

	if _, ok := auth.AccessTokenFromContext(r.Context()); !ok {
		writeJSONError(w, "oauth token not found", http.StatusUnauthorized)
		return
	}

	qc, err := h.k8sClient.GetFileStatusQueryConfig(r.Context(), pipelineName)
	if err != nil {
		slog.Error("failed to get pipeline query config", "pipeline", pipelineName, "error", err)
		writeJSONError(w, fmt.Sprintf("pipeline %q not found or misconfigured", pipelineName), http.StatusNotFound)
		return
	}

	q := r.URL.Query()
	page, err := parseOptionalInt(q.Get("page"))
	if err != nil {
		writeJSONError(w, "invalid page parameter", http.StatusBadRequest)
		return
	}
	pageSize, err := parseOptionalInt(q.Get("page_size"))
	if err != nil {
		writeJSONError(w, "invalid page_size parameter", http.StatusBadRequest)
		return
	}

	database := strings.ToUpper(strings.ReplaceAll(qc.Database, "-", "_"))
	schema := strings.ToUpper(qc.Schema)

	result, err := h.querier.GetFileProcessingStatus(r.Context(),
		filestatus.QueryConfig{
			Database: database,
			Schema:   schema,
			Stages:   qc.Stages,
		},
		filestatus.FileStatusParams{
			FileID:   q.Get("file_id"),
			FileName: q.Get("file_name"),
			Status:   q.Get("status"),
			Page:     page,
			PageSize: pageSize,
		},
	)
	if err != nil {
		slog.Error("failed to query file status", "pipeline", pipelineName, "error", err)
		writeJSONError(w, fmt.Sprintf("failed to query file status: %v", err), http.StatusInternalServerError)
		return
	}

	result.PipelineName = pipelineName

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(result); err != nil {
		slog.Error("failed to encode response", "error", err)
	}
}

func parseOptionalInt(s string) (int, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.Atoi(s)
}

func writeJSONError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]string{"error": msg}); err != nil {
		slog.Error("failed to encode error response", "error", err)
	}
}
