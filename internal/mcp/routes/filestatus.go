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

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

type FileStatusHandler struct {
	k8sClient *k8sclient.Client
}

func NewFileStatusHandler(k8sClient *k8sclient.Client) *FileStatusHandler {
	return &FileStatusHandler{k8sClient: k8sClient}
}

func (h *FileStatusHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	pipelineName := r.PathValue("pipeline_name")
	if pipelineName == "" {
		writeJSONError(w, "pipeline_name is required", http.StatusBadRequest)
		return
	}

	oauthToken, ok := auth.AccessTokenFromContext(r.Context())
	if !ok {
		writeJSONError(w, "oauth token not found", http.StatusUnauthorized)
		return
	}

	qc, err := h.k8sClient.GetPipelineQueryConfig(r.Context(), pipelineName, operatorv1alpha1.StageTypeSourceCrawler)
	if err != nil {
		slog.Error("failed to get pipeline query config", "pipeline", pipelineName, "error", err)
		writeJSONError(w, fmt.Sprintf("pipeline %q not found or misconfigured", pipelineName), http.StatusNotFound)
		return
	}

	q := r.URL.Query()
	page, _ := strconv.Atoi(q.Get("page"))
	pageSize, _ := strconv.Atoi(q.Get("page_size"))

	database := strings.ToUpper(strings.ReplaceAll(qc.Database, "-", "_"))
	schema := strings.ToUpper(qc.Schema)

	result, err := snowflake.GetFileProcessingStatus(r.Context(), oauthToken,
		database, schema,
		snowflake.FileStatusParams{
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

func writeJSONError(w http.ResponseWriter, msg string, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]string{"error": msg}); err != nil {
		slog.Error("failed to encode error response", "error", err)
	}
}
