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
	"crypto/subtle"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"time"
)

// dangerousSchemes are URL schemes that must never be accepted as redirect URIs.
var dangerousSchemes = []string{"javascript", "data", "blob", "vbscript"}

// OAuthServer implements the OAuth 2.1 Authorization Server endpoints that proxy
// authentication to an external provider via the Provider interface.
// MCP clients interact with these endpoints; actual user authentication is
// delegated to the Provider.
type OAuthServer struct {
	provider    Provider
	callbackURL string
	store       *OAuthStore
	logger      *slog.Logger
}

// NewOAuthServer creates a new OAuth authorization server.
// callbackURL is our server's /auth/callback/oidc endpoint URL that the
// external SSO will redirect to after user authentication.
func NewOAuthServer(provider Provider, callbackURL string, store *OAuthStore, logger *slog.Logger) *OAuthServer {
	return &OAuthServer{
		provider:    provider,
		callbackURL: callbackURL,
		store:       store,
		logger:      logger,
	}
}

// HandleRegister implements Dynamic Client Registration (RFC 7591).
// POST /auth/register
func (s *OAuthServer) HandleRegister(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // 1MB limit
	var req struct {
		RedirectURIs            []string `json:"redirect_uris"`
		GrantTypes              []string `json:"grant_types"`
		ResponseTypes           []string `json:"response_types"`
		TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid request body")
		return
	}

	if len(req.RedirectURIs) == 0 {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "redirect_uris is required")
		return
	}
	for _, uriStr := range req.RedirectURIs {
		u, err := url.Parse(uriStr)
		if err != nil || !u.IsAbs() || slices.Contains(dangerousSchemes, u.Scheme) {
			writeJSONError(w, http.StatusBadRequest, "invalid_request",
				"invalid redirect_uri: must be an absolute URL with a safe scheme")
			return
		}
		isLocalhost := u.Hostname() == "localhost" || u.Hostname() == "127.0.0.1" || u.Hostname() == "[::1]"
		if u.Scheme == "http" && !isLocalhost {
			writeJSONError(w, http.StatusBadRequest, "invalid_request",
				"invalid redirect_uri: http is only allowed for localhost")
			return
		}
	}

	if len(req.GrantTypes) == 0 {
		req.GrantTypes = []string{"authorization_code", "refresh_token"}
	}
	if len(req.ResponseTypes) == 0 {
		req.ResponseTypes = []string{"code"}
	}
	if req.TokenEndpointAuthMethod == "" {
		req.TokenEndpointAuthMethod = "client_secret_post"
	}

	client := s.store.CreateClient(req.RedirectURIs, req.GrantTypes, req.ResponseTypes, req.TokenEndpointAuthMethod)

	s.logger.Info("client registered", "client_id", safePrefix(client.ClientID))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(client); err != nil {
		s.logger.Error("failed to write response", "error", err)
	}
}

// HandleAuthorize implements the Authorization Endpoint (RFC 6749 Section 3.1).
// Validates the request, stores a pending authorization with a CSRF token, then
// redirects to the external provider for user authentication. The authorization
// code is only generated after successful authentication in HandleCallback.
// GET /auth/authorize
func (s *OAuthServer) HandleAuthorize(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	if q.Get("response_type") != "code" {
		writeJSONError(w, http.StatusBadRequest, "unsupported_response_type", "only 'code' is supported")
		return
	}

	clientID := q.Get("client_id")
	client, ok := s.store.GetClient(clientID)
	if !ok {
		writeJSONError(w, http.StatusBadRequest, "invalid_client", "unknown client_id")
		return
	}

	redirectURI := q.Get("redirect_uri")
	if !isValidRedirectURI(client, redirectURI) {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "redirect_uri not registered for this client")
		return
	}

	codeChallenge := q.Get("code_challenge")
	if codeChallenge == "" || q.Get("code_challenge_method") != "S256" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "code_challenge with S256 method is required")
		return
	}

	// Store the authorization context keyed by an opaque CSRF token.
	// The auth code is NOT generated here — it is created post-authentication in HandleCallback.
	csrfToken := generateRandomString(32)
	s.store.StorePending(&PendingAuthorization{
		CSRFToken:       csrfToken,
		ClientID:        clientID,
		RedirectURI:     redirectURI,
		CodeChallenge:   codeChallenge,
		ChallengeMethod: "S256",
		OrigState:       q.Get("state"),
		ExpiresAt:       time.Now().Add(10 * time.Minute),
	})

	authURL, err := s.provider.BuildAuthURL(s.callbackURL, csrfToken)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "server_error", "failed to build authorization URL")
		return
	}

	s.logger.Info("redirecting to provider for authentication")
	writeRedirectPage(w, authURL, "Redirecting to your identity provider...")
}

// HandleCallback handles the redirect from the external provider after user authentication.
// It verifies the CSRF token, exchanges the provider's code for a token, generates an
// authorization code, and redirects back to the MCP client.
// GET /auth/callback/oidc
func (s *OAuthServer) HandleCallback(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	if errCode := q.Get("error"); errCode != "" {
		s.logger.Error("provider returned error", "error", errCode, "description", q.Get("error_description"))
		writeJSONError(w, http.StatusBadRequest, errCode, q.Get("error_description"))
		return
	}

	providerCode := q.Get("code")
	if providerCode == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "missing code from provider")
		return
	}

	// Verify CSRF token and retrieve the pending authorization context.
	csrfToken := q.Get("state")
	pending, ok := s.store.ConsumePending(csrfToken)
	if !ok {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid or expired state parameter")
		return
	}

	token, err := s.provider.ExchangeCode(r.Context(), providerCode, s.callbackURL)
	if err != nil {
		s.logger.Error("provider token exchange failed", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "server_error", "failed to exchange authorization code")
		return
	}

	// Auth code is generated only AFTER successful authentication — never exposed to SSO.
	authCode := generateRandomString(32)
	s.store.StoreCode(&AuthorizationCode{
		Code:            authCode,
		ClientID:        pending.ClientID,
		RedirectURI:     pending.RedirectURI,
		CodeChallenge:   pending.CodeChallenge,
		ChallengeMethod: pending.ChallengeMethod,
		ExpiresAt:       time.Now().Add(10 * time.Minute),
		ExternalToken:   token,
	})

	redirectURL, err := url.Parse(pending.RedirectURI)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "server_error", "invalid redirect_uri")
		return
	}
	params := redirectURL.Query()
	params.Set("code", authCode)
	if pending.OrigState != "" {
		params.Set("state", pending.OrigState)
	}
	redirectURL.RawQuery = params.Encode()

	// Store the client redirect URL behind an opaque token so the browser
	// navigates to a clean /auth/complete URL with no sensitive parameters.
	completionToken := s.store.StoreCompletion(redirectURL.String())

	s.logger.Info("authentication successful, redirecting to client", "client_id", safePrefix(pending.ClientID))
	http.Redirect(w, r, "/auth/complete/"+completionToken, http.StatusFound)
}

// HandleComplete serves the post-authentication success page at a clean URL
// with no OAuth parameters exposed. It renders a meta-refresh redirect to the
// MCP client's redirect_uri.
// GET /auth/complete/{token}
func (s *OAuthServer) HandleComplete(w http.ResponseWriter, r *http.Request) {
	token := r.PathValue("token")
	clientRedirectURL, ok := s.store.ConsumeCompletion(token)
	if !ok {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid or expired completion token")
		return
	}
	writeCallbackPage(w, clientRedirectURL)
}

// HandleToken implements the Token Endpoint (RFC 6749 Section 3.2).
// Supports authorization_code (with PKCE) and refresh_token grant types.
// POST /auth/token
func (s *OAuthServer) HandleToken(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if err := r.ParseForm(); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid form data")
		return
	}

	switch r.FormValue("grant_type") {
	case "authorization_code":
		s.handleAuthorizationCodeGrant(w, r)
	case "refresh_token":
		s.handleRefreshTokenGrant(w, r)
	default:
		writeJSONError(w, http.StatusBadRequest, "unsupported_grant_type",
			fmt.Sprintf("unsupported grant_type: %s", r.FormValue("grant_type")))
	}
}

func (s *OAuthServer) handleAuthorizationCodeGrant(w http.ResponseWriter, r *http.Request) {
	code := r.FormValue("code")
	codeVerifier := r.FormValue("code_verifier")
	redirectURI := r.FormValue("redirect_uri")

	s.logger.Debug("token exchange attempt",
		"code_len", len(code),
		"has_verifier", codeVerifier != "",
		"redirect_uri", redirectURI,
	)

	authCode, ok := s.store.ConsumeCode(code)
	if !ok {
		s.logger.Warn("token exchange failed: invalid code", "code_prefix", safePrefix(code))
		writeJSONError(w, http.StatusBadRequest, "invalid_grant",
			"invalid, expired, or already used authorization code")
		return
	}

	if !s.authenticateClient(w, r, authCode.ClientID) {
		return
	}

	if authCode.RedirectURI != redirectURI {
		s.logger.Warn("token exchange failed: redirect_uri mismatch",
			"expected", authCode.RedirectURI, "got", redirectURI)
		writeJSONError(w, http.StatusBadRequest, "invalid_grant", "redirect_uri mismatch")
		return
	}

	if !ValidatePKCE(codeVerifier, authCode.CodeChallenge, authCode.ChallengeMethod) {
		s.logger.Warn("token exchange failed: PKCE validation failed")
		writeJSONError(w, http.StatusBadRequest, "invalid_grant",
			"invalid code_verifier (PKCE validation failed)")
		return
	}

	if authCode.ExternalToken == nil {
		s.logger.Warn("token exchange failed: external token not yet available")
		writeJSONError(w, http.StatusBadRequest, "invalid_grant",
			"authorization not completed — callback may not have finished")
		return
	}

	s.logger.Info("token exchange successful", "client_id", safePrefix(authCode.ClientID))
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(authCode.ExternalToken); err != nil {
		s.logger.Error("failed to write response", "error", err)
	}
}

func (s *OAuthServer) handleRefreshTokenGrant(w http.ResponseWriter, r *http.Request) {
	refreshToken := r.FormValue("refresh_token")
	if refreshToken == "" {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "refresh_token is required")
		return
	}

	if !s.authenticateClient(w, r, clientIDFromRequest(r)) {
		return
	}

	token, err := s.provider.RefreshToken(r.Context(), refreshToken)
	if err != nil {
		s.logger.Error("token refresh failed", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "server_error", "failed to refresh token")
		return
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(token); err != nil {
		s.logger.Error("failed to write response", "error", err)
	}
}

// authenticateClient validates client credentials from the request against the
// registered client. Returns true if authentication passes; writes an error
// response and returns false otherwise.
func (s *OAuthServer) authenticateClient(w http.ResponseWriter, r *http.Request, expectedClientID string) bool {
	client, ok := s.store.GetClient(expectedClientID)
	if !ok {
		s.logger.Warn("client authentication failed: client not found", "client_id", safePrefix(expectedClientID))
		writeJSONError(w, http.StatusUnauthorized, "invalid_client", "client not found")
		return false
	}

	if client.TokenEndpointAuthMethod == "none" {
		return true
	}

	clientID, clientSecret, hasBasic := r.BasicAuth()
	if !hasBasic {
		clientID = r.FormValue("client_id")
		clientSecret = r.FormValue("client_secret")
	}
	if clientID != client.ClientID ||
		subtle.ConstantTimeCompare([]byte(clientSecret), []byte(client.ClientSecret)) != 1 {
		s.logger.Warn("client authentication failed: invalid credentials", "client_id", safePrefix(clientID))
		writeJSONError(w, http.StatusUnauthorized, "invalid_client", "client authentication failed")
		return false
	}
	return true
}

func clientIDFromRequest(r *http.Request) string {
	if id, _, ok := r.BasicAuth(); ok {
		return id
	}
	return r.FormValue("client_id")
}

func isValidRedirectURI(client *OAuthClient, uri string) bool {
	return slices.Contains(client.RedirectURIs, uri)
}

const safePrefixLen = 4

func safePrefix(s string) string {
	if len(s) <= safePrefixLen {
		return s
	}
	return s[:safePrefixLen] + "..."
}
