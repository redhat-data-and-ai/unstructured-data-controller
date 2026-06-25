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

package auth

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"
)

// OAuthConfig holds OAuth configuration loaded from environment variables.
type OAuthConfig struct {
	ClientID         string
	ClientSecret     string
	AuthorizationURL string
	TokenURL         string
	IntrospectionURL string
	CallbackURL      string
}

// NewOAuthConfigFromEnv creates an OAuthConfig from SSO_* environment variables.
func NewOAuthConfigFromEnv() (*OAuthConfig, error) {
	cfg := &OAuthConfig{
		ClientID:         os.Getenv("SSO_CLIENT_ID"),
		ClientSecret:     os.Getenv("SSO_CLIENT_SECRET"),
		AuthorizationURL: os.Getenv("SSO_AUTHORIZATION_URL"),
		TokenURL:         os.Getenv("SSO_TOKEN_URL"),
		IntrospectionURL: os.Getenv("SSO_INTROSPECTION_URL"),
		CallbackURL:      os.Getenv("SSO_CALLBACK_URL"),
	}
	return cfg, nil
}

// IntrospectionResponse represents the OAuth 2.0 Token Introspection response per RFC 7662.
type IntrospectionResponse struct {
	Active    bool   `json:"active"`
	Sub       string `json:"sub,omitempty"`
	ClientID  string `json:"client_id,omitempty"`
	Username  string `json:"username,omitempty"`
	TokenType string `json:"token_type,omitempty"`
	Exp       int64  `json:"exp,omitempty"`
	Iat       int64  `json:"iat,omitempty"`
	Iss       string `json:"iss,omitempty"`
	Scope     string `json:"scope,omitempty"`
}

type contextKey int

const tokenInfoKey contextKey = iota

// TokenInfoFromContext retrieves the IntrospectionResponse stored by the auth middleware.
func TokenInfoFromContext(ctx context.Context) (*IntrospectionResponse, bool) {
	info, ok := ctx.Value(tokenInfoKey).(*IntrospectionResponse)
	return info, ok
}

// WithTokenInfo stores the IntrospectionResponse in the given context.
func WithTokenInfo(ctx context.Context, info *IntrospectionResponse) context.Context {
	return context.WithValue(ctx, tokenInfoKey, info)
}

type tokenCacheEntry struct {
	response  *IntrospectionResponse
	expiresAt time.Time
}

// Middleware validates OAuth Bearer tokens using a Provider for introspection.
type Middleware struct {
	provider Provider
	logger   *slog.Logger
	cache    sync.Map
	cacheTTL time.Duration
	done     chan struct{}
}

// NewMiddleware creates a new OAuth middleware that validates tokens
// by delegating introspection to the given Provider.
func NewMiddleware(provider Provider, logger *slog.Logger) *Middleware {
	m := &Middleware{
		provider: provider,
		logger:   logger,
		cacheTTL: 5 * time.Minute,
		done:     make(chan struct{}),
	}
	go m.cleanupLoop()
	return m
}

// Close stops the background cache cleanup goroutine.
func (m *Middleware) Close() {
	close(m.done)
}

func (m *Middleware) cleanupLoop() {
	ticker := time.NewTicker(1 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-m.done:
			return
		case <-ticker.C:
			now := time.Now()
			m.cache.Range(func(key, value any) bool {
				if entry, ok := value.(*tokenCacheEntry); ok && now.After(entry.expiresAt) {
					m.cache.Delete(key)
				}
				return true
			})
		}
	}
}

// Authenticate returns HTTP middleware that validates OAuth Bearer tokens.
// Validated token info is stored in the request context and accessible via TokenInfoFromContext.
func (m *Middleware) Authenticate(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		token, err := extractBearerToken(r)
		if err != nil {
			m.logger.Warn("unauthorized request", "error", err, "remote_addr", r.RemoteAddr)
			setWWWAuthenticate(w, r)
			writeJSONError(w, http.StatusUnauthorized, "unauthorized", err.Error())
			return
		}

		resp, err := m.validateToken(r.Context(), token)
		if err != nil {
			m.logger.Error("token introspection failed", "error", err, "remote_addr", r.RemoteAddr)
			writeJSONError(w, http.StatusInternalServerError, "server_error", "token validation failed")
			return
		}

		if !resp.Active {
			m.logger.Warn("inactive token", "remote_addr", r.RemoteAddr)
			setWWWAuthenticate(w, r)
			writeJSONError(w, http.StatusUnauthorized, "unauthorized", "token is not active")
			return
		}

		ctx := WithTokenInfo(r.Context(), resp)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// validateToken checks the cache first, then delegates to the Provider.
func (m *Middleware) validateToken(ctx context.Context, token string) (*IntrospectionResponse, error) {
	if entry, ok := m.cache.Load(token); ok {
		if cached, ok := entry.(*tokenCacheEntry); ok && time.Now().Before(cached.expiresAt) {
			return cached.response, nil
		}
		m.cache.Delete(token)
	}

	resp, err := m.provider.IntrospectToken(ctx, token)
	if err != nil {
		return nil, err
	}

	if resp.Active {
		ttl := m.cacheTTL
		if resp.Exp > 0 {
			if tokenTTL := time.Until(time.Unix(resp.Exp, 0)); tokenTTL > 0 && tokenTTL < ttl {
				ttl = tokenTTL
			}
		}
		m.cache.Store(token, &tokenCacheEntry{
			response:  resp,
			expiresAt: time.Now().Add(ttl),
		})
	}

	return resp, nil
}

// ProtectedResourceMetadataHandler serves OAuth Protected Resource Metadata (RFC 9728)
// at /.well-known/oauth-protected-resource. MCP clients use this to discover which
// authorization server to use for obtaining tokens.
func (*Middleware) ProtectedResourceMetadataHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		baseURL := requestBaseURL(r)
		metadata := map[string]any{
			"resource":                 baseURL,
			"authorization_servers":    []string{baseURL},
			"bearer_methods_supported": []string{"header"},
			"scopes_supported":         []string{},
			"registration_endpoint":    baseURL + "/auth/register",
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(metadata)
	}
}

// MetadataHandler serves OAuth Authorization Server Metadata (RFC 8414).
// All endpoints point to our server, which proxies authentication to the external SSO.
func (*Middleware) MetadataHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		baseURL := requestBaseURL(r)
		metadata := map[string]any{
			"issuer":                                baseURL,
			"authorization_endpoint":                baseURL + "/auth/authorize",
			"token_endpoint":                        baseURL + "/auth/token",
			"registration_endpoint":                 baseURL + "/auth/register",
			"response_types_supported":              []string{"code"},
			"response_modes_supported":              []string{"query"},
			"grant_types_supported":                 []string{"authorization_code", "refresh_token"},
			"token_endpoint_auth_methods_supported": []string{"client_secret_basic", "client_secret_post", "none"},
			"code_challenge_methods_supported":      []string{"S256"},
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(metadata)
	}
}

// setWWWAuthenticate adds the WWW-Authenticate header pointing to the protected resource
// metadata URL, enabling MCP clients to discover the OAuth flow automatically.
func setWWWAuthenticate(w http.ResponseWriter, r *http.Request) {
	resourceMetadataURL := requestBaseURL(r) + "/.well-known/oauth-protected-resource"
	w.Header().Set("WWW-Authenticate", fmt.Sprintf(`Bearer resource_metadata=%q`, resourceMetadataURL))
}

// requestBaseURL returns the server's public base URL. Prefers the static
// MCP_PUBLIC_URL env var to prevent host header injection; falls back to
// deriving from the request if not configured.
func requestBaseURL(r *http.Request) string {
	if publicURL := os.Getenv("MCP_PUBLIC_URL"); publicURL != "" {
		return strings.TrimSuffix(publicURL, "/")
	}
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if fwd := r.Header.Get("X-Forwarded-Proto"); fwd != "" {
		scheme = fwd
	}
	return fmt.Sprintf("%s://%s", scheme, r.Host)
}

func extractBearerToken(r *http.Request) (string, error) {
	header := r.Header.Get("Authorization")
	if header == "" {
		return "", errors.New("missing Authorization header")
	}
	parts := strings.SplitN(header, " ", 2)
	if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") {
		return "", errors.New("invalid Authorization header, expected Bearer token")
	}
	token := strings.TrimSpace(parts[1])
	if token == "" {
		return "", errors.New("empty Bearer token")
	}
	return token, nil
}

func writeJSONError(w http.ResponseWriter, status int, errCode, description string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(map[string]string{
		"error":             errCode,
		"error_description": description,
	}); err != nil {
		slog.Error("failed to write error response", "error", err)
	}
}
