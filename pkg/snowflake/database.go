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

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

type DatabaseInfo struct {
	Name    string `json:"name" db:"name"`
	Comment string `json:"comment,omitempty" db:"comment"`
}

func ShowDatabases(ctx context.Context, oauthToken string) (result []DatabaseInfo, err error) {
	ctx, span := otel.Tracer("pkg/snowflake").Start(ctx, "snowflake.show_databases",
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithAttributes(attribute.String("db.system", "snowflake")))
	defer func() {
		if err != nil {
			span.RecordError(err)
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}()
	return queryRows[DatabaseInfo](ctx, oauthToken, "SHOW DATABASES;")
}
