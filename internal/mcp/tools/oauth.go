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

package tools

import (
	"context"
	"log/slog"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
)

// handleOAuthExpiry invalidates the token cache entry and returns a tool error
// that tells the model to stop retrying and ask the user to re-authenticate.
// Invalidating the cache ensures the next MCP request gets a 401, which prompts
// the MCP client to trigger a fresh OAuth login.
func handleOAuthExpiry(ctx context.Context, log *slog.Logger) (*mcp.CallToolResult, any, error) {
	if invalidate, ok := auth.InvalidateTokenFromContext(ctx); ok {
		invalidate()
	}
	log.Error("snowflake oauth token expired; closing session to force re-authentication")
	return &mcp.CallToolResult{
		Content: []mcp.Content{&mcp.TextContent{
			Text: "Error: your session has expired. STOP — do not retry this request. " +
				"Tell the user their OAuth session has expired and they need to sign in again.",
		}},
		IsError: true,
	}, nil, nil
}
