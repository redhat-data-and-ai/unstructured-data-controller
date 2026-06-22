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

// AuthorizationCode represents a pending authorization code grant with PKCE.
type AuthorizationCode struct {
	Code            string
	ClientID        string
	RedirectURI     string
	CodeChallenge   string
	ChallengeMethod string
	State           string
	ExpiresAt       time.Time
	Used            bool
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

// OAuthStore provides in-memory storage for OAuth clients and authorization codes.
type OAuthStore struct {
	clients sync.Map
	codes   sync.Map
}

// NewOAuthStore creates a new in-memory OAuth store.
func NewOAuthStore() *OAuthStore {
	return &OAuthStore{}
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

// StoreCode saves an authorization code for later exchange.
func (s *OAuthStore) StoreCode(code *AuthorizationCode) {
	s.codes.Store(code.Code, code)
}

// GetCode retrieves an authorization code without consuming it.
func (s *OAuthStore) GetCode(code string) (*AuthorizationCode, bool) {
	val, ok := s.codes.Load(code)
	if !ok {
		return nil, false
	}
	ac, ok := val.(*AuthorizationCode)
	return ac, ok
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

func generateRandomString(length int) string {
	b := make([]byte, (length+1)/2)
	if _, err := rand.Read(b); err != nil {
		panic("crypto/rand failed: " + err.Error())
	}
	return hex.EncodeToString(b)[:length]
}
