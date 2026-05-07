package snowflake

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/snowflakedb/gosnowflake"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type Client struct {
	conn  *sql.DB
	close func() error
	role  string
}

type ClientConfig struct {
	gosnowflake.Config
}

var (
	sfclient    *Client
	clientMutex sync.Mutex
	sqlMutex    sync.Mutex
	SfConfig    *ClientConfig
)

const (
	successMessage               = "Query Executed"
	failureMessage               = "Query failed"
	roleSwitchingFailureMesssage = "Role switching failed"
)

func NewClient(ctx context.Context) (*Client, error) {
	dsn, err := gosnowflake.DSN(&SfConfig.Config)
	if err != nil {
		return nil, err
	}

	conn, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, err
	}

	sfclient = &Client{
		conn:  conn,
		close: conn.Close,
		role:  SfConfig.Role,
	}

	if err := sfclient.Ping(ctx); err != nil {
		return nil, err
	}
	return sfclient, nil
}

func (c *Client) GetRole() string {
	return c.role
}

func GetClient(ctx context.Context) (*Client, error) {
	clientMutex.Lock()
	defer clientMutex.Unlock()

	if sfclient == nil || sfclient.conn.Ping() != nil {
		client, err := NewClient(ctx)
		if err != nil {
			return nil, err
		}
		return client, nil
	}

	return sfclient, nil
}

func (c *Client) Ping(ctx context.Context) error {
	return c.conn.PingContext(ctx)
}

func (c *Client) query(ctx context.Context, sqlQuery string, role string) (*sql.Rows, error) {
	sqlMutex.Lock()
	defer sqlMutex.Unlock()

	logger := log.FromContext(ctx).WithValues(
		"role", role,
		"secondaryRole", "NONE",
		"query", sqlQuery,
	)

	if !strings.Contains(strings.ToUpper(sqlQuery), "USE ROLE") {
		if err := c.switchRoles(ctx, role); err != nil {
			logger.Error(err, roleSwitchingFailureMesssage)
			return nil, err
		}
	}

	start := time.Now()

	rows, err := c.conn.QueryContext(ctx, sqlQuery)
	duration := time.Since(start)

	if err != nil {
		logger.Error(err, failureMessage, "duration", duration.String())
		return nil, err
	}

	logger.Info(successMessage, "duration", duration.String())
	return rows, nil
}

func (c *Client) switchRoles(ctx context.Context, role string) error {
	if _, err := c.conn.ExecContext(ctx, fmt.Sprintf("USE ROLE %s;", role)); err != nil {
		return fmt.Errorf("failed to switch primary role: %w", err)
	}
	if _, err := c.conn.ExecContext(ctx, "USE SECONDARY ROLE NONE;"); err != nil {
		return fmt.Errorf("failed to switch secondary role: %w", err)
	}
	return nil
}

func (c *Client) execute(ctx context.Context, query, role string) error {
	sqlMutex.Lock()
	defer sqlMutex.Unlock()

	logger := log.FromContext(ctx).WithValues(
		"role", role,
		"secondaryRole", "NONE",
		"query", query,
	)
	start := time.Now()

	if err := c.switchRoles(ctx, role); err != nil {
		logger.Error(err, roleSwitchingFailureMesssage)
		return err
	}

	result, err := c.conn.ExecContext(ctx, query)
	if err != nil {
		logger.Error(err, failureMessage)
		return fmt.Errorf("query failed: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		if err.Error() != "no RowsAffected available after DDL statement" {
			return fmt.Errorf("failed to get rows affected: %w", err)
		}
		rowsAffected = 0
	}

	logger.Info(successMessage, "rowsAffected", rowsAffected, "duration", time.Since(start))
	return nil
}

func (c *Client) executeBatch(ctx context.Context, queries []string, role string) error {
	logger := log.FromContext(ctx).WithValues(
		"role", role,
		"numQueries", len(queries),
	)
	start := time.Now()

	for i, query := range queries {
		logger := logger.WithValues("queryIndex", i, "query", query)

		if err := c.execute(ctx, query, role); err != nil {
			logger.Error(err, failureMessage)
			return fmt.Errorf("query %d failed: %w", i, err)
		}
	}

	logger.Info("All queries executed successfully", "totalDuration", time.Since(start))
	return nil
}
