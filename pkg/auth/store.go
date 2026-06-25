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
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// OAuthClient represents a dynamically registered OAuth client (RFC 7591).
type OAuthClient struct {
	ClientID                string   `json:"client_id"`
	ClientSecret            string   `json:"client_secret"`
	RedirectURIs            []string `json:"redirect_uris"`
	GrantTypes              []string `json:"grant_types"`
	ResponseTypes           []string `json:"response_types"`
	TokenEndpointAuthMethod string   `json:"token_endpoint_auth_method"`
}

// PendingAuthorization holds context for an in-flight OAuth authorize request.
// Keyed by CSRF token in the store; consumed when the SSO callback arrives.
type PendingAuthorization struct {
	CSRFToken       string
	ClientID        string
	RedirectURI     string
	CodeChallenge   string
	ChallengeMethod string
	OrigState       string
	ExpiresAt       time.Time
}

// AuthorizationCode is created after successful authentication and holds the
// external token for exchange via the token endpoint.
type AuthorizationCode struct {
	Code            string
	ClientID        string
	RedirectURI     string
	CodeChallenge   string
	ChallengeMethod string
	ExpiresAt       time.Time
	ExternalToken   *ExternalToken
}

// ExternalToken holds the token received from the upstream SSO provider.
type ExternalToken struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token,omitempty"`
	TokenType    string `json:"token_type"`
	ExpiresIn    int    `json:"expires_in,omitempty"`
	Scope        string `json:"scope,omitempty"`
}

type completionEntry struct {
	redirectURL string
	expiresAt   time.Time
}

// OAuthStore provides in-memory storage for OAuth clients, pending authorizations,
// authorization codes, and completion redirects.
type OAuthStore struct {
	clients     sync.Map
	pending     sync.Map
	codes       sync.Map
	completions sync.Map
	done        chan struct{}
}

// NewOAuthStore creates a new in-memory OAuth store with a background cleanup
// goroutine that evicts expired pending authorizations and codes.
func NewOAuthStore() *OAuthStore {
	s := &OAuthStore{done: make(chan struct{})}
	go s.cleanupLoop()
	return s
}

// Close stops the background cleanup goroutine.
func (s *OAuthStore) Close() {
	close(s.done)
}

func (s *OAuthStore) cleanupLoop() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			now := time.Now()
			s.pending.Range(func(key, value any) bool {
				if pa, ok := value.(*PendingAuthorization); ok && now.After(pa.ExpiresAt) {
					s.pending.Delete(key)
				}
				return true
			})
			s.codes.Range(func(key, value any) bool {
				if ac, ok := value.(*AuthorizationCode); ok && now.After(ac.ExpiresAt) {
					s.codes.Delete(key)
				}
				return true
			})
			s.completions.Range(func(key, value any) bool {
				if ce, ok := value.(*completionEntry); ok && now.After(ce.expiresAt) {
					s.completions.Delete(key)
				}
				return true
			})
		}
	}
}

// CreateClient registers a new OAuth client with generated credentials.
func (s *OAuthStore) CreateClient(redirectURIs, grantTypes, responseTypes []string, authMethod string) *OAuthClient {
	client := &OAuthClient{
		ClientID:                generateRandomString(32),
		ClientSecret:            generateRandomString(64),
		RedirectURIs:            redirectURIs,
		GrantTypes:              grantTypes,
		ResponseTypes:           responseTypes,
		TokenEndpointAuthMethod: authMethod,
	}
	s.clients.Store(client.ClientID, client)
	return client
}

// GetClient retrieves a registered client by ID.
func (s *OAuthStore) GetClient(clientID string) (*OAuthClient, bool) {
	val, ok := s.clients.Load(clientID)
	if !ok {
		return nil, false
	}
	client, ok := val.(*OAuthClient)
	return client, ok
}

// StorePending saves a pending authorization keyed by its CSRF token.
func (s *OAuthStore) StorePending(pa *PendingAuthorization) {
	s.pending.Store(pa.CSRFToken, pa)
}

// ConsumePending atomically retrieves and removes a pending authorization.
func (s *OAuthStore) ConsumePending(csrfToken string) (*PendingAuthorization, bool) {
	val, ok := s.pending.LoadAndDelete(csrfToken)
	if !ok {
		return nil, false
	}
	pa, ok := val.(*PendingAuthorization)
	if !ok {
		return nil, false
	}
	if time.Now().After(pa.ExpiresAt) {
		return nil, false
	}
	return pa, true
}

// StoreCode saves an authorization code for later exchange.
func (s *OAuthStore) StoreCode(code *AuthorizationCode) {
	s.codes.Store(code.Code, code)
}

// ConsumeCode atomically retrieves and removes an authorization code (single-use).
// Returns nil if the code is invalid, expired, or already consumed.
func (s *OAuthStore) ConsumeCode(code string) (*AuthorizationCode, bool) {
	val, ok := s.codes.LoadAndDelete(code)
	if !ok {
		return nil, false
	}
	ac, ok := val.(*AuthorizationCode)
	if !ok {
		return nil, false
	}
	if time.Now().After(ac.ExpiresAt) {
		return nil, false
	}
	return ac, true
}

// StoreCompletion saves a redirect URL keyed by a short-lived token.
// Used to redirect from the callback to a clean URL before showing the success page.
func (s *OAuthStore) StoreCompletion(redirectURL string) string {
	token := generateRandomString(16)
	s.completions.Store(token, &completionEntry{
		redirectURL: redirectURL,
		expiresAt:   time.Now().Add(2 * time.Minute),
	})
	return token
}

// ConsumeCompletion atomically retrieves and removes a completion redirect URL.
func (s *OAuthStore) ConsumeCompletion(token string) (string, bool) {
	val, ok := s.completions.LoadAndDelete(token)
	if !ok {
		return "", false
	}
	ce, ok := val.(*completionEntry)
	if !ok || time.Now().After(ce.expiresAt) {
		return "", false
	}
	return ce.redirectURL, true
}

func generateRandomString(length int) string {
	b := make([]byte, (length+1)/2)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b)[:length]
}
