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
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// GenericProvider implements the Provider interface for standard OAuth 2.0 / OIDC
// identity providers (Keycloak, Red Hat SSO, Okta, Azure AD, etc.).
// It uses the Authorization Code flow with token introspection (RFC 7662).
type GenericProvider struct {
	clientID         string
	clientSecret     string
	authorizationURL string
	tokenURL         string
	introspectionURL string
	httpClient       *http.Client
}

// NewGenericProvider creates a provider from an OAuthConfig.
// Requires: ClientID, ClientSecret, AuthorizationURL, TokenURL, IntrospectionURL.
func NewGenericProvider(cfg *OAuthConfig) (*GenericProvider, error) {
	var missing []string
	for _, check := range []struct{ name, val string }{
		{"SSO_CLIENT_ID", cfg.ClientID},
		{"SSO_CLIENT_SECRET", cfg.ClientSecret},
		{"SSO_AUTHORIZATION_URL", cfg.AuthorizationURL},
		{"SSO_TOKEN_URL", cfg.TokenURL},
		{"SSO_INTROSPECTION_URL", cfg.IntrospectionURL},
	} {
		if check.val == "" {
			missing = append(missing, check.name)
		}
	}
	if len(missing) > 0 {
		return nil, fmt.Errorf("generic provider: missing required config: %s", strings.Join(missing, ", "))
	}

	return &GenericProvider{
		clientID:         cfg.ClientID,
		clientSecret:     cfg.ClientSecret,
		authorizationURL: cfg.AuthorizationURL,
		tokenURL:         cfg.TokenURL,
		introspectionURL: cfg.IntrospectionURL,
		httpClient:       &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (p *GenericProvider) BuildAuthURL(callbackURL, state string) (string, error) {
	u, err := url.Parse(p.authorizationURL)
	if err != nil {
		return "", fmt.Errorf("parsing authorization URL: %w", err)
	}
	q := u.Query()
	q.Set("response_type", "code")
	q.Set("client_id", p.clientID)
	q.Set("redirect_uri", callbackURL)
	q.Set("state", state)
	q.Set("scope", "openid")
	u.RawQuery = q.Encode()
	return u.String(), nil
}

func (p *GenericProvider) ExchangeCode(ctx context.Context, code, callbackURL string) (*ExternalToken, error) {
	data := url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {code},
		"redirect_uri":  {callbackURL},
		"client_id":     {p.clientID},
		"client_secret": {p.clientSecret},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("building token request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling token endpoint: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token endpoint returned HTTP %d", resp.StatusCode)
	}

	var token ExternalToken
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return nil, fmt.Errorf("decoding token response: %w", err)
	}
	return &token, nil
}

func (p *GenericProvider) RefreshToken(ctx context.Context, refreshToken string) (*ExternalToken, error) {
	data := url.Values{
		"grant_type":    {"refresh_token"},
		"refresh_token": {refreshToken},
		"client_id":     {p.clientID},
		"client_secret": {p.clientSecret},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.tokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("building refresh request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling token endpoint for refresh: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("token refresh returned HTTP %d", resp.StatusCode)
	}

	var token ExternalToken
	if err := json.NewDecoder(resp.Body).Decode(&token); err != nil {
		return nil, fmt.Errorf("decoding refresh response: %w", err)
	}
	return &token, nil
}

func (p *GenericProvider) IntrospectToken(ctx context.Context, token string) (*IntrospectionResponse, error) {
	data := url.Values{
		"token":           {token},
		"token_type_hint": {"access_token"},
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, p.introspectionURL, strings.NewReader(data.Encode()))
	if err != nil {
		return nil, fmt.Errorf("building introspection request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	req.Header.Set("Accept", "application/json")
	req.SetBasicAuth(p.clientID, p.clientSecret)

	resp, err := p.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("calling introspection endpoint: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("introspection endpoint returned HTTP %d", resp.StatusCode)
	}

	var result IntrospectionResponse
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("decoding introspection response: %w", err)
	}
	return &result, nil
}
