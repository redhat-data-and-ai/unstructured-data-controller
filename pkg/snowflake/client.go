package snowflake

import (
	"context"
	"database/sql"
	"errors"
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
	Sfclient    *Client
	clientMutex sync.Mutex
	sqlMutex    sync.Mutex
)

const (
	successMessage               = "Query Executed"
	failureMessage               = "Query failed"
	roleSwitchingFailureMesssage = "Role switching failed"
)

func NewClient(_ context.Context, config *ClientConfig) (*Client, error) {
	clientMutex.Lock()
	defer clientMutex.Unlock()
	if Sfclient == nil || Sfclient.conn.Ping() != nil {
		dsn, err := gosnowflake.DSN(&config.Config)
		if err != nil {
			return nil, err
		}

		conn, err := sql.Open("snowflake", dsn)
		if err != nil {
			return nil, err
		}

		Sfclient = &Client{
			conn:  conn,
			close: conn.Close,
			role:  config.Role,
		}
	}
	return Sfclient, nil
}

func (c *Client) GetRole() string {
	return c.role
}

func GetClient() (*Client, error) {
	clientMutex.Lock()
	defer clientMutex.Unlock()

	if Sfclient == nil {
		return nil, errors.New("no snowflake client found")
	}
	return Sfclient, nil
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

func (c *Client) execute(ctx context.Context, query, role string) (int64, error) {
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
		return 0, err
	}

	result, err := c.conn.ExecContext(ctx, query)
	if err != nil {
		logger.Error(err, failureMessage)
		return 0, fmt.Errorf("query failed: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		if err.Error() != "no RowsAffected available after DDL statement" {
			return 0, fmt.Errorf("failed to get rows affected: %w", err)
		}
		rowsAffected = 0
	}

	logger.Info(successMessage, "rowsAffected", rowsAffected, "duration", time.Since(start))
	return rowsAffected, nil
}

func (c *Client) executeBatch(ctx context.Context, queries []string, role string) error {
	logger := log.FromContext(ctx).WithValues(
		"role", role,
		"numQueries", len(queries),
	)
	start := time.Now()

	for i, query := range queries {
		logger := logger.WithValues("queryIndex", i, "query", query)

		if _, err := c.execute(ctx, query, role); err != nil {
			logger.Error(err, failureMessage)
			return fmt.Errorf("query %d failed: %w", i, err)
		}
	}

	logger.Info("All queries executed successfully", "totalDuration", time.Since(start))
	return nil
}
