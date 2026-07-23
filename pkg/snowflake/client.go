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
	"database/sql"
	"errors"
	"fmt"
	"os"

	"github.com/snowflakedb/gosnowflake"
)

func openConnection(oauthToken string) (*sql.DB, error) {
	account := os.Getenv("SNOWFLAKE_ACCOUNT")
	if account == "" {
		return nil, errors.New("SNOWFLAKE_ACCOUNT environment variable not set")
	}

	warehouse := os.Getenv("SNOWFLAKE_WAREHOUSE")
	if warehouse == "" {
		warehouse = "DEFAULT"
	}

	cfg := &gosnowflake.Config{
		Account:       account,
		Authenticator: gosnowflake.AuthTypeOAuth,
		Token:         oauthToken,
		Role:          "PUBLIC",
		Warehouse:     warehouse,
		OCSPFailOpen:  gosnowflake.OCSPFailOpenTrue,
	}

	dsn, err := gosnowflake.DSN(cfg)
	if err != nil {
		return nil, fmt.Errorf("failed to build snowflake DSN: %w", err)
	}

	db, err := sql.Open("snowflake", dsn)
	if err != nil {
		return nil, fmt.Errorf("failed to create snowflake connection: %w", err)
	}

	return db, nil
}
