package snowflake

import (
	"context"
	"fmt"
)

type UploadedFileStatus struct {
	Source string `db:"source"`
	Target string `db:"target"`
	Status string `db:"status"`
}

// Put facilitates uploading files to the internal stage resource
func (c *Client) Put(ctx context.Context, roleName, filePath, databaseName,
	schemaName, stageName, subpath string, resources any) error {
	putQuery :=
		fmt.Sprintf("PUT 'file://%s' '@%s.%s.%s/%s' OVERWRITE = TRUE;", filePath,
			databaseName, schemaName, stageName, subpath)
	rows, err := c.query(ctx, putQuery, roleName)
	if err != nil {
		return err
	}
	defer func() {
		err = rows.Close()
	}()
	return scanRows(rows, resources)
}

func (c *Client) ListFilesFromStage(ctx context.Context, roleName, databaseName,
	schemaName, stageName string, resources any) error {
	query := fmt.Sprintf("SELECT $1 AS data FROM @%s.%s.%s;", databaseName, schemaName, stageName)
	rows, err := c.query(ctx, query, roleName)
	if err != nil {
		return err
	}
	defer func() {
		err = rows.Close()
	}()
	return scanRows(rows, resources)
}

func (c *Client) DeleteFilesFromStage(ctx context.Context,
	roleName, databaseName, schemaName, stageName string, files []string) error {
	queries := make([]string, 0, len(files))
	for _, file := range files {
		queries = append(queries, fmt.Sprintf("REMOVE '@%s.%s.%s/%s';", databaseName, schemaName, stageName, file))
	}
	if err := c.executeBatch(ctx, queries, roleName); err != nil {
		return fmt.Errorf("error deleting files from stage: %w", err)
	}
	return nil
}
