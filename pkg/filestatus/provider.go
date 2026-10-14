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

package filestatus

import "context"

const (
	DefaultPageSize = 100
	MCPMaxResults   = 300
)

type StatusQuerierType string

const (
	StatusQuerierTypeSnowflake StatusQuerierType = "snowflake"
)

type StageMV struct {
	Name  string
	Table string
}

type QueryConfig struct {
	Database     string
	Schema       string
	ProviderType StatusQuerierType
	Stages       []StageMV
}

type StageStatus struct {
	Name  string `json:"name"`
	Error string `json:"error"`
}

type FileStatus struct {
	FileID   string        `json:"file_id"`
	FilePath string        `json:"file_path"`
	FileName string        `json:"file_name"`
	FileURL  string        `json:"file_url"`
	Stages   []StageStatus `json:"stages"`
}

type FileStatusResult struct {
	PipelineName string       `json:"pipeline_name"`
	TotalFiles   int          `json:"total_files"`
	FailedFiles  int          `json:"failed_files"`
	SourceType   string       `json:"source_type"`
	Files        []FileStatus `json:"files"`
}

type FileStatusParams struct {
	FileID   string
	FileName string
	Status   string
	Page     int
	PageSize int
}

type FileListResult struct {
	FileID   string `json:"file_id" db:"file_id"`
	FilePath string `json:"file_path" db:"file_path"`
	FileName string `json:"file_name" db:"file_name"`
	FileURL  string `json:"file_url" db:"file_url"`
}

type StatusQuerier interface {
	GetFileProcessingStatus(ctx context.Context, qc QueryConfig, params FileStatusParams) (*FileStatusResult, error)
	ListPipelineFiles(ctx context.Context, database, schema, table string, limit int) ([]FileListResult, error)
}
