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
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestatus"
)

type SnowflakeQuerier struct{}

func (*SnowflakeQuerier) GetFileProcessingStatus(
	ctx context.Context, qc filestatus.QueryConfig, params filestatus.FileStatusParams,
) (*filestatus.FileStatusResult, error) {
	oauthToken, ok := auth.AccessTokenFromContext(ctx)
	if !ok {
		return nil, errors.New("oauth token not found in context")
	}

	if params.PageSize <= 0 {
		params.PageSize = filestatus.DefaultPageSize
	}
	if params.Page <= 0 {
		params.Page = 1
	}
	offset := (params.Page - 1) * params.PageSize

	stages := qc.Stages
	if len(stages) == 0 {
		return nil, errors.New("no stages configured")
	}

	query, args := buildFileStatusQuery(qc, params, offset)

	scanner := func(rows *sql.Rows) (*filestatus.FileStatusResult, error) {
		return scanFileStatusRows(rows, stages)
	}
	return queryWithFunc(ctx, oauthToken, query, scanner, args...)
}

func buildFileStatusQuery(qc filestatus.QueryConfig, params filestatus.FileStatusParams, offset int) (string, []any) {
	stages := qc.Stages
	fq := func(name string) string {
		return fmt.Sprintf("%s.%s.%s", qc.Database, qc.Schema, name)
	}

	// SELECT: common fields from s0, error from each stage
	numStages := len(stages)
	selectCols := make([]string, 0, 5+numStages+2)
	selectCols = append(selectCols,
		"s0.FILE_ID", "s0.FILE_PATH", "s0.FILE_NAME", "s0.FILE_URL", "s0.SOURCE_TYPE",
	)
	errorExprs := make([]string, 0, numStages)
	for i := range stages {
		selectCols = append(selectCols, fmt.Sprintf("s%d.ERROR AS ERROR_%d", i, i))
		errorExprs = append(errorExprs, fmt.Sprintf("ERROR_%d IS NOT NULL", i))
	}
	failedCond := strings.Join(errorExprs, " OR ")
	selectCols = append(selectCols,
		"COUNT(*) OVER() AS TOTAL_COUNT",
		fmt.Sprintf("SUM(CASE WHEN %s THEN 1 ELSE 0 END) OVER() AS FAILED_COUNT", failedCond),
	)

	// FROM: s0 with all common fields, subsequent stages LEFT JOIN on FILE_ID for error only.
	// DISTINCT handles multi-row MVs (chunks, embeds).
	from := fmt.Sprintf(
		"(SELECT DISTINCT FILE_ID, FILE_PATH, FILE_NAME, FILE_URL, SOURCE_TYPE, ERROR FROM %s) s0",
		fq(stages[0].Table))
	for i := 1; i < len(stages); i++ {
		from += fmt.Sprintf(
			"\nLEFT JOIN (SELECT DISTINCT FILE_ID, ERROR FROM %s) s%d ON s0.FILE_ID = s%d.FILE_ID",
			fq(stages[i].Table), i, i)
	}

	query := fmt.Sprintf("SELECT %s\nFROM %s", strings.Join(selectCols, ", "), from)

	// WHERE
	var conditions []string
	var args []any
	if params.FileID != "" {
		conditions = append(conditions, "s0.FILE_ID = ?")
		args = append(args, params.FileID)
	}
	if params.FileName != "" {
		conditions = append(conditions, "s0.FILE_NAME LIKE ?")
		args = append(args, "%"+params.FileName+"%")
	}
	if params.Status == "failed" {
		conditions = append(conditions, "("+failedCond+")")
	}
	if len(conditions) > 0 {
		query += fmt.Sprintf("\nWHERE %s", strings.Join(conditions, " AND "))
	}

	query += fmt.Sprintf("\nORDER BY s0.FILE_ID\nLIMIT %d OFFSET %d", params.PageSize, offset)
	return query, args
}

// scanFileStatusRows scans dynamically-columned rows into FileStatusResult.
// Column order: FILE_ID, FILE_PATH, FILE_NAME, FILE_URL, SOURCE_TYPE,
// ERROR_0..ERROR_{N-1}, TOTAL_COUNT, FAILED_COUNT
func scanFileStatusRows(rows *sql.Rows, stages []filestatus.StageMV) (*filestatus.FileStatusResult, error) {
	numStages := len(stages)
	numCols := 5 + numStages + 2

	result := &filestatus.FileStatusResult{
		Files: make([]filestatus.FileStatus, 0),
	}

	for rows.Next() {
		values := make([]sql.NullString, numCols)
		ptrs := make([]any, numCols)
		for i := range ptrs {
			ptrs[i] = &values[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		if result.SourceType == "" {
			result.SourceType = values[4].String
		}
		if result.TotalFiles == 0 {
			if count, err := strconv.Atoi(values[5+numStages].String); err == nil {
				result.TotalFiles = count
			}
		}
		if result.FailedFiles == 0 {
			if count, err := strconv.Atoi(values[5+numStages+1].String); err == nil {
				result.FailedFiles = count
			}
		}

		stageStatuses := make([]filestatus.StageStatus, numStages)
		for i, stage := range stages {
			stageStatuses[i] = filestatus.StageStatus{
				Name:  stage.Name,
				Error: values[5+i].String,
			}
		}

		result.Files = append(result.Files, filestatus.FileStatus{
			FileID:   values[0].String,
			FilePath: values[1].String,
			FileName: values[2].String,
			FileURL:  values[3].String,
			Stages:   stageStatuses,
		})
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return result, nil
}

func (*SnowflakeQuerier) ListPipelineFiles(
	ctx context.Context, database, schema, table string, limit int,
) ([]filestatus.FileListResult, error) {
	oauthToken, ok := auth.AccessTokenFromContext(ctx)
	if !ok {
		return nil, errors.New("oauth token not found in context")
	}

	if limit <= 0 {
		limit = filestatus.MCPMaxResults
	}
	query := fmt.Sprintf(
		`SELECT FILE_ID, FILE_PATH, FILE_NAME, FILE_URL FROM %s.%s.%s ORDER BY FILE_ID LIMIT %d`,
		database, schema, table, limit,
	)
	return queryRows[filestatus.FileListResult](ctx, oauthToken, query)
}
