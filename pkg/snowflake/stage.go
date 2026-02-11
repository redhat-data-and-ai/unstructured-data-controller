package snowflake

import (
	"context"
	"fmt"
)

// Put facilitates uploading files to the internal stage resource
func (c *Client) Put(ctx context.Context, svcRoleName, filePath, schemaName, stageName, subpath string, resources any) error {
	putQuery := fmt.Sprintf("PUT 'file://%s' '@%s.%s.%s/%s' OVERWRITE = TRUE;", filePath, SnowpipeDatabaseName, schemaName, stageName, subpath)
	rows, err := c.query(ctx, putQuery, svcRoleName)
	if err != nil {
		return err
	}
	defer func() {
		err = rows.Close()
	}()
	return scanRows(rows, resources)
}

func (c *Client) GetDataFromStage(ctx context.Context, svcRoleName, schemaName, stageName string, resources any) error {
	query := fmt.Sprintf("SELECT $1 AS data FROM @%s.%s.%s;", SnowpipeDatabaseName, schemaName, stageName)
	rows, err := c.query(ctx, query, svcRoleName)
	if err != nil {
		return err
	}
	defer func() {
		err = rows.Close()
	}()
	return scanRows(rows, resources)
}

func (c *Client) DeleteFilesFromStage(ctx context.Context, svcRoleName, schemaName, stageName string, files []string) error {
	queries := []string{}
	for _, file := range files {
		queries = append(queries, fmt.Sprintf("REMOVE '@%s.%s.%s/%s';", SnowpipeDatabaseName, schemaName, stageName, file))
	}
	if err := c.executeBatch(ctx, queries, svcRoleName); err != nil {
		return fmt.Errorf("Error deleting files from stage: %w", err)
	}
	return nil
}
