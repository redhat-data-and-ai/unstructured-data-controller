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
	"fmt"
	"reflect"
	"strings"
)

func scanRows[T any](rows *sql.Rows) ([]T, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("failed to get column names: %w", err)
	}

	var results []T
	for rows.Next() {
		values := make([]any, len(columns))
		for i := range values {
			var placeholder any
			values[i] = &placeholder
		}

		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		var item T
		rv := reflect.ValueOf(&item).Elem()
		rt := rv.Type()

		fieldMap := make(map[string]int, rt.NumField())
		for i := range rt.NumField() {
			tag := rt.Field(i).Tag.Get("db")
			if tag != "" {
				fieldMap[strings.ToLower(tag)] = i
			}
		}

		for i, colName := range columns {
			idx, ok := fieldMap[strings.ToLower(colName)]
			if !ok {
				continue
			}
			val := *(values[i].(*any))
			if val == nil {
				continue
			}
			// SetString panics on a non-string field, and a panic here takes down
			// the whole server rather than one request. Skip instead: this scanner
			// renders every column as a string, so a non-string target is a tagging
			// mistake, not something to crash on.
			if rv.Field(idx).Kind() != reflect.String {
				continue
			}
			rv.Field(idx).SetString(dbValueString(val))
		}

		results = append(results, item)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating rows: %w", err)
	}

	return results, nil
}

func dbValueString(value any) string {
	if bytes, ok := value.([]byte); ok {
		return string(bytes)
	}
	return fmt.Sprint(value)
}
