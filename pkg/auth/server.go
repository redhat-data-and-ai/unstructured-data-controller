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
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"slices"
	"time"
)

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

// callbackState is encoded into the SSO state parameter to carry our context
// through the external authentication redirect.
type callbackState struct {
	AuthCode    string `json:"c"`
	OrigState   string `json:"s,omitempty"`
	RedirectURI string `json:"r"`
	ClientID    string `json:"i"`
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

	s.logger.Info("client registered", "client_id", client.ClientID)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	if err := json.NewEncoder(w).Encode(client); err != nil {
		s.logger.Error("failed to write response", "error", err)
	}
}

// HandleAuthorize implements the Authorization Endpoint (RFC 6749 Section 3.1).
// Validates the request, generates an authorization code, then redirects to the
// external provider for user authentication.
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

	authCode := generateRandomString(32)
	s.store.StoreCode(&AuthorizationCode{
		Code:            authCode,
		ClientID:        clientID,
		RedirectURI:     redirectURI,
		CodeChallenge:   codeChallenge,
		ChallengeMethod: "S256",
		State:           q.Get("state"),
		ExpiresAt:       time.Now().Add(10 * time.Minute),
	})

	// Encode our context into the state parameter passed through the external provider
	cbState := callbackState{
		AuthCode:    authCode,
		OrigState:   q.Get("state"),
		RedirectURI: redirectURI,
		ClientID:    clientID,
	}
	stateJSON, err := json.Marshal(cbState)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "server_error", "failed to encode state")
		return
	}
	encodedState := base64.RawURLEncoding.EncodeToString(stateJSON)

	authURL, err := s.provider.BuildAuthURL(s.callbackURL, encodedState)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "server_error", "failed to build authorization URL")
		return
	}

	s.logger.Info("redirecting to provider for authentication", "client_id", clientID)
	http.Redirect(w, r, authURL, http.StatusFound)
}

// HandleCallback handles the redirect from the external provider after user authentication.
// It exchanges the provider's authorization code for a token and redirects back to the MCP client.
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

	encodedState := q.Get("state")
	stateJSON, err := base64.RawURLEncoding.DecodeString(encodedState)
	if err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "invalid state parameter")
		return
	}
	var cbState callbackState
	if err := json.Unmarshal(stateJSON, &cbState); err != nil {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "malformed state parameter")
		return
	}

	token, err := s.provider.ExchangeCode(r.Context(), providerCode, s.callbackURL)
	if err != nil {
		s.logger.Error("provider token exchange failed", "error", err)
		writeJSONError(w, http.StatusInternalServerError, "server_error", "failed to exchange authorization code")
		return
	}

	code, ok := s.store.GetCode(cbState.AuthCode)
	if !ok {
		writeJSONError(w, http.StatusBadRequest, "invalid_request", "authorization code expired or not found")
		return
	}
	code.ExternalToken = token

	redirectURL, err := url.Parse(cbState.RedirectURI)
	if err != nil {
		writeJSONError(w, http.StatusInternalServerError, "server_error", "invalid redirect_uri")
		return
	}
	params := redirectURL.Query()
	params.Set("code", cbState.AuthCode)
	if cbState.OrigState != "" {
		params.Set("state", cbState.OrigState)
	}
	redirectURL.RawQuery = params.Encode()

	s.logger.Info("authentication successful, redirecting to client", "client_id", cbState.ClientID)
	http.Redirect(w, r, redirectURL.String(), http.StatusFound)
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
		s.logger.Warn("token exchange failed: invalid code", "code_prefix", safePrefix(code, 8))
		writeJSONError(w, http.StatusBadRequest, "invalid_grant", "invalid, expired, or already used authorization code")
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
		writeJSONError(w, http.StatusBadRequest, "invalid_grant", "invalid code_verifier (PKCE validation failed)")
		return
	}

	if authCode.ExternalToken == nil {
		s.logger.Warn("token exchange failed: external token not yet available")
		writeJSONError(w, http.StatusBadRequest, "invalid_grant",
			"authorization not completed — callback may not have finished")
		return
	}

	s.logger.Info("token exchange successful", "client_id", authCode.ClientID)
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

func isValidRedirectURI(client *OAuthClient, uri string) bool {
	return slices.Contains(client.RedirectURIs, uri)
}

func safePrefix(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
