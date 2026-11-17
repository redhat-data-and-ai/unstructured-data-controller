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
	"crypto/subtle"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// OAuthConfig holds OAuth configuration loaded from environment variables.
type OAuthConfig struct {
	ClientID             string
	ClientSecret         string
	AuthorizationURL     string
	TokenURL             string
	IntrospectionURL     string
	CallbackURL          string
	DisableIntrospection bool

	// TokenValidation selects how a Bearer token is checked. See the
	// ValidationMode constants.
	TokenValidation ValidationMode
	// JWKSURL publishes the issuer's signing keys. Required by ValidationJWKS.
	JWKSURL string
	// Issuer is the exact "iss" every accepted token must carry. Required by
	// ValidationJWKS, since a signature alone does not say who vouched for it.
	Issuer string
}

// URL schemes, named because the package compares and builds them in several
// places and a typo in a scheme comparison fails open.
const (
	schemeHTTP  = "http"
	schemeHTTPS = "https"
)

// ValidationMode selects how the middleware decides a Bearer token is genuine.
type ValidationMode string

const (
	// ValidationIntrospect asks the identity provider about every token. It is
	// the default, and the only mode that notices a token revoked before it
	// expires.
	ValidationIntrospect ValidationMode = "introspect"

	// ValidationJWKS verifies the token's signature against the issuer's
	// published keys. For providers that do not offer introspection. The check
	// is offline, so a revoked token stays usable until it expires.
	ValidationJWKS ValidationMode = "jwks"
)

// NewOAuthConfigFromEnv creates an OAuthConfig from SSO_* environment variables.
func NewOAuthConfigFromEnv() (*OAuthConfig, error) {
	cfg := &OAuthConfig{
		ClientID:             os.Getenv("SSO_CLIENT_ID"),
		ClientSecret:         os.Getenv("SSO_CLIENT_SECRET"),
		AuthorizationURL:     os.Getenv("SSO_AUTHORIZATION_URL"),
		TokenURL:             os.Getenv("SSO_TOKEN_URL"),
		IntrospectionURL:     os.Getenv("SSO_INTROSPECTION_URL"),
		CallbackURL:          os.Getenv("SSO_CALLBACK_URL"),
		DisableIntrospection: boolFromEnv("SSO_DISABLE_INTROSPECTION"),
		TokenValidation:      ValidationMode(strings.TrimSpace(os.Getenv("SSO_TOKEN_VALIDATION"))),
		JWKSURL:              strings.TrimSpace(os.Getenv("SSO_JWKS_URL")),
		Issuer:               strings.TrimSpace(os.Getenv("SSO_ISSUER")),
	}

	switch cfg.TokenValidation {
	case "":
		cfg.TokenValidation = ValidationIntrospect
	case ValidationIntrospect, ValidationJWKS:
	default:
		return nil, fmt.Errorf("SSO_TOKEN_VALIDATION is %q, want %q or %q",
			cfg.TokenValidation, ValidationIntrospect, ValidationJWKS)
	}

	// Fail at startup rather than on the first request. Without these two a
	// JWKS verifier cannot tell a genuine token from one signed by anybody.
	if cfg.TokenValidation == ValidationJWKS {
		if cfg.JWKSURL == "" {
			return nil, errors.New("SSO_TOKEN_VALIDATION=jwks requires SSO_JWKS_URL")
		}
		if cfg.Issuer == "" {
			return nil, errors.New("SSO_TOKEN_VALIDATION=jwks requires SSO_ISSUER")
		}
		if err := requireSecureURL("SSO_JWKS_URL", cfg.JWKSURL); err != nil {
			return nil, err
		}
		// Both flags are answers to the same problem, and the older one wins
		// silently in validateToken, which would leave signatures unchecked
		// while the startup log claims otherwise.
		if cfg.DisableIntrospection {
			return nil, errors.New(
				"SSO_TOKEN_VALIDATION=jwks cannot be combined with SSO_DISABLE_INTROSPECTION=true: " +
					"unset SSO_DISABLE_INTROSPECTION so tokens are actually verified")
		}
	}

	return cfg, nil
}

// requireSecureURL rejects a plaintext key or token endpoint, because anyone on
// the path could then answer with their own signing key and mint tokens for any
// user. Loopback is allowed so a local provider still works.
func requireSecureURL(name, raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("%s is not a valid URL: %w", name, err)
	}
	// "https:///certs" parses with a scheme and no host, which would start
	// cleanly and then fail on the first key fetch.
	if parsed.Host == "" {
		return fmt.Errorf("%s has no host: %q", name, raw)
	}
	if parsed.Scheme == schemeHTTPS {
		return nil
	}
	if parsed.Scheme == schemeHTTP {
		switch host := parsed.Hostname(); host {
		case "localhost", "127.0.0.1", "::1":
			return nil
		}
	}
	return fmt.Errorf("%s must use https, got %q", name, parsed.Scheme)
}

// boolFromEnv returns true when the environment variable parses as a boolean true value.
func boolFromEnv(key string) bool {
	val, _ := strconv.ParseBool(os.Getenv(key))
	return val
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

const (
	tokenInfoKey contextKey = iota
	accessTokenKey
)

// TokenInfoFromContext retrieves the IntrospectionResponse stored by the auth middleware.
func TokenInfoFromContext(ctx context.Context) (*IntrospectionResponse, bool) {
	info, ok := ctx.Value(tokenInfoKey).(*IntrospectionResponse)
	return info, ok
}

// WithTokenInfo stores the IntrospectionResponse in the given context.
func WithTokenInfo(ctx context.Context, info *IntrospectionResponse) context.Context {
	return context.WithValue(ctx, tokenInfoKey, info)
}

// AccessTokenFromContext retrieves the raw OAuth access token string stored by the auth middleware.
func AccessTokenFromContext(ctx context.Context) (string, bool) {
	token, ok := ctx.Value(accessTokenKey).(string)
	return token, ok
}

// WithAccessToken stores the raw OAuth access token in the given context.
func WithAccessToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, accessTokenKey, token)
}

type tokenCacheEntry struct {
	response  *IntrospectionResponse
	expiresAt time.Time
}

// Middleware validates OAuth Bearer tokens using a Provider for introspection.
type Middleware struct {
	provider             Provider
	logger               *slog.Logger
	cache                sync.Map
	cacheTTL             time.Duration
	done                 chan struct{}
	disableIntrospection bool
	jwks                 *JWKSVerifier
}

// MiddlewareOption adjusts a Middleware at construction.
type MiddlewareOption func(*Middleware)

// WithJWKSVerifier validates tokens by signature instead of by introspection,
// for an identity provider that does not offer an introspection endpoint.
func WithJWKSVerifier(verifier *JWKSVerifier) MiddlewareOption {
	return func(m *Middleware) { m.jwks = verifier }
}

// UsesJWKS reports whether tokens are validated by signature, for callers that
// must tell a real check from the unvalidated path left by
// SSO_DISABLE_INTROSPECTION.
//
// Deliberately answers no when that flag is also set, even though the verifier
// would in fact run, so a confused configuration can only ever switch a
// dependent feature off.
func (m *Middleware) UsesJWKS() bool { return m.jwks != nil && !m.disableIntrospection }

// NewMiddleware creates a new OAuth middleware that validates tokens
// by delegating introspection to the given Provider.
func NewMiddleware(
	provider Provider,
	logger *slog.Logger,
	disableIntrospection bool,
	opts ...MiddlewareOption,
) *Middleware {
	m := &Middleware{
		provider:             provider,
		logger:               logger,
		cacheTTL:             5 * time.Minute,
		done:                 make(chan struct{}),
		disableIntrospection: disableIntrospection,
	}
	for _, opt := range opts {
		opt(m)
	}
	if disableIntrospection {
		logger.Warn("OAuth token introspection is disabled; Bearer tokens are accepted without validation")
	} else {
		go m.cleanupLoop()
	}
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
		ctx = WithAccessToken(ctx, token)
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

// ErrSessionUserMismatch means a request arrived on an MCP session that was
// opened by a different bearer token.
var ErrSessionUserMismatch = errors.New("request token does not match the session it arrived on")

// TokenIdentifier validates a bearer token and reports who holds it.
// Middleware implements it.
type TokenIdentifier interface {
	Identify(ctx context.Context, token string) (*IntrospectionResponse, error)
}

// Identify validates a token and returns its introspection result, reusing the
// middleware's cache so a repeat lookup within one request is free.
func (m *Middleware) Identify(ctx context.Context, token string) (*IntrospectionResponse, error) {
	return m.validateToken(ctx, token)
}

// SubjectOf returns the most stable identifier a provider gave for a token, or
// "" when it gave none.
func SubjectOf(resp *IntrospectionResponse) string {
	if resp == nil {
		return ""
	}
	if resp.Sub != "" {
		return resp.Sub
	}
	return resp.Username
}

// CheckRequestMatchesSession reports whether this request was sent by the same
// caller who opened the session, and returns the request's own bearer token.
//
// The returned token is the one to act with. An MCP handler's context belongs
// to the session, so the token stored there is whatever opened it; after a
// refresh that is stale while the request carries a live one.
//
// A different token is accepted only when it resolves to the same subject,
// which is what makes a refresh work. Returns ErrSessionUserMismatch
// otherwise, and never returns a token alongside an error.
func CheckRequestMatchesSession(
	ctx context.Context, requestHeader http.Header, identifier TokenIdentifier,
) (string, error) {
	sessionToken, ok := AccessTokenFromContext(ctx)
	if !ok || sessionToken == "" {
		return "", ErrSessionUserMismatch
	}
	if requestHeader == nil {
		return "", ErrSessionUserMismatch
	}

	requestToken, err := bearerTokenFromHeader(requestHeader.Get("Authorization"))
	if err != nil {
		return "", ErrSessionUserMismatch
	}

	// Same token needs no second opinion. Constant time because it compares a
	// secret and is reachable by anyone who can reach the endpoint.
	if subtle.ConstantTimeCompare([]byte(sessionToken), []byte(requestToken)) == 1 {
		return requestToken, nil
	}

	// A different token is only acceptable if it belongs to the same person,
	// which covers a refresh. Without an identifier there is no way to tell,
	// so refuse.
	if identifier == nil {
		return "", ErrSessionUserMismatch
	}

	sessionInfo, ok := TokenInfoFromContext(ctx)
	if !ok {
		return "", ErrSessionUserMismatch
	}
	sessionSubject := SubjectOf(sessionInfo)
	if sessionSubject == "" {
		return "", fmt.Errorf("%w: the session's token carries no subject", ErrSessionUserMismatch)
	}

	requestInfo, err := identifier.Identify(ctx, requestToken)
	if err != nil {
		return "", fmt.Errorf("%w: validating the request token: %w", ErrSessionUserMismatch, err)
	}
	if requestInfo == nil || !requestInfo.Active {
		return "", fmt.Errorf("%w: the request's token is not active", ErrSessionUserMismatch)
	}
	if SubjectOf(requestInfo) != sessionSubject {
		return "", ErrSessionUserMismatch
	}
	return requestToken, nil
}

// validateToken checks the cache first, then delegates to the Provider.
func (m *Middleware) validateToken(ctx context.Context, token string) (*IntrospectionResponse, error) {
	// A configured verifier outranks the unvalidated path, so the two flags
	// together can never be weaker than the verifier alone. The config layer
	// rejects the combination too; this is the backstop.
	if m.disableIntrospection && m.jwks == nil {
		return claimsFromJWT(token), nil
	}

	if entry, ok := m.cache.Load(token); ok {
		if cached, ok := entry.(*tokenCacheEntry); ok && time.Now().Before(cached.expiresAt) {
			return cached.response, nil
		}
		m.cache.Delete(token)
	}

	resp, err := m.validateUncached(ctx, token)
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

// validateUncached checks one token with whichever mechanism is configured.
//
// Every verification failure is an inactive token, so an attacker-supplied
// token produces a refusal and never a 500. That deliberately also covers an
// unreachable key endpoint, which fails closed: the alternative leaks that the
// server is misconfigured, and the error is logged either way so an outage is
// still visible.
func (m *Middleware) validateUncached(ctx context.Context, token string) (*IntrospectionResponse, error) {
	if m.jwks == nil {
		return m.provider.IntrospectToken(ctx, token)
	}

	resp, err := m.jwks.Verify(ctx, token)
	if err != nil {
		m.logger.Warn("token failed verification", "error", err)
		return &IntrospectionResponse{Active: false}, nil
	}
	return resp, nil
}

func claimsFromJWT(token string) *IntrospectionResponse {
	resp := &IntrospectionResponse{Active: true}
	claims := jwt.MapClaims{}
	if _, _, err := jwt.NewParser().ParseUnverified(token, claims); err != nil {
		return resp // not a JWT (opaque token) — fall back to bare Active:true
	}
	if sub, ok := claims["sub"].(string); ok {
		resp.Sub = sub
	}
	if username, ok := claims["preferred_username"].(string); ok {
		resp.Username = username
	} else if email, ok := claims["email"].(string); ok {
		resp.Username = email
	}
	if exp, ok := claims["exp"].(float64); ok {
		resp.Exp = int64(exp)
		if time.Now().Unix() >= int64(exp)-30 {
			resp.Active = false
		}
	}
	return resp
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
	return bearerTokenFromHeader(r.Header.Get("Authorization"))
}

// bearerTokenFromHeader parses one Authorization header value.
func bearerTokenFromHeader(header string) (string, error) {
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
