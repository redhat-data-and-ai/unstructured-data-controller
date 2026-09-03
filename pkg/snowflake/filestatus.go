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

package snowflake

import (
	"context"
	"fmt"
	"strconv"
	"strings"
)

const (
	FileStatusViewName = "FILE_STATUS_VW"
	defaultPageSize    = 100
	MCPMaxResults      = 300
)

type fileStatusRow struct {
	FileID       string `db:"file_id"`
	FilePath     string `db:"file_path"`
	FileName     string `db:"file_name"`
	FileURL      string `db:"file_url"`
	SourceType   string `db:"source_type"`
	CrawlError   string `db:"crawl_error"`
	ConvertError string `db:"convert_error"`
	ChunkError   string `db:"chunk_error"`
	EmbedError   string `db:"embed_error"`
	TotalCount   string `db:"total_count"`
	FailedCount  string `db:"failed_count"`
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

func GetFileProcessingStatus(
	ctx context.Context,
	oauthToken, database, schema string,
	params FileStatusParams,
) (*FileStatusResult, error) {
	if params.PageSize <= 0 {
		params.PageSize = defaultPageSize
	}
	if params.Page <= 0 {
		params.Page = 1
	}
	offset := (params.Page - 1) * params.PageSize

	failedCond := "CRAWL_ERROR IS NOT NULL OR CONVERT_ERROR IS NOT NULL " +
		"OR CHUNK_ERROR IS NOT NULL OR EMBED_ERROR IS NOT NULL"

	query := fmt.Sprintf(
		"SELECT FILE_ID, FILE_PATH, FILE_NAME, FILE_URL, SOURCE_TYPE,\n"+
			"  CRAWL_ERROR, CONVERT_ERROR, CHUNK_ERROR, EMBED_ERROR,\n"+
			"  COUNT(*) OVER() AS TOTAL_COUNT,\n"+
			"  SUM(CASE WHEN %s THEN 1 ELSE 0 END) OVER() AS FAILED_COUNT\n"+
			"FROM %s.%s.%s", failedCond, database, schema, FileStatusViewName)

	var conditions []string
	var args []any

	if params.FileID != "" {
		conditions = append(conditions, "FILE_ID = ?")
		args = append(args, params.FileID)
	}
	if params.FileName != "" {
		conditions = append(conditions, "FILE_NAME LIKE ?")
		args = append(args, "%"+params.FileName+"%")
	}
	if params.Status == "failed" {
		conditions = append(conditions, "("+failedCond+")")
	}

	if len(conditions) > 0 {
		query += fmt.Sprintf("\nWHERE %s", strings.Join(conditions, " AND "))
	}

	query += fmt.Sprintf(
		"\nORDER BY FILE_ID\nLIMIT %d OFFSET %d", params.PageSize, offset)

	rows, err := queryRows[fileStatusRow](ctx, oauthToken, query, args...)
	if err != nil {
		return nil, err
	}

	result := &FileStatusResult{
		Files: make([]FileStatus, 0, len(rows)),
	}

	for _, row := range rows {
		if result.SourceType == "" && row.SourceType != "" {
			result.SourceType = row.SourceType
		}
		if result.TotalFiles == 0 && row.TotalCount != "" {
			if count, err := strconv.Atoi(row.TotalCount); err == nil {
				result.TotalFiles = count
			}
		}
		if result.FailedFiles == 0 && row.FailedCount != "" {
			if count, err := strconv.Atoi(row.FailedCount); err == nil {
				result.FailedFiles = count
			}
		}

		result.Files = append(result.Files, FileStatus{
			FileID:   row.FileID,
			FilePath: row.FilePath,
			FileName: row.FileName,
			FileURL:  row.FileURL,
			Stages: []StageStatus{
				{Name: "crawl", Error: row.CrawlError},
				{Name: "convert", Error: row.ConvertError},
				{Name: "chunk", Error: row.ChunkError},
				{Name: "embed", Error: row.EmbedError},
			},
		})
	}

	return result, nil
}

type FileListResult struct {
	FileID   string `json:"file_id" db:"file_id"`
	FilePath string `json:"file_path" db:"file_path"`
	FileName string `json:"file_name" db:"file_name"`
	FileURL  string `json:"file_url" db:"file_url"`
}

func ListPipelineFiles(
	ctx context.Context, oauthToken, database, schema, table string, limit int,
) ([]FileListResult, error) {
	if limit <= 0 {
		limit = MCPMaxResults
	}
	query := fmt.Sprintf(
		`SELECT FILE_ID, FILE_PATH, FILE_NAME, FILE_URL FROM %s.%s.%s ORDER BY FILE_ID LIMIT %d`,
		database, schema, table, limit,
	)
	return queryRows[FileListResult](ctx, oauthToken, query)
}
