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
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

const pkceMethodS256 = "S256"

// mockProvider implements Provider for testing middleware behavior in isolation.
type mockProvider struct {
	introspectFunc func(ctx context.Context, token string) (*IntrospectionResponse, error)
}

func (*mockProvider) BuildAuthURL(string, string) (string, error) { return "", nil }
func (*mockProvider) ExchangeCode(context.Context, string, string) (*ExternalToken, error) {
	return nil, nil
}
func (*mockProvider) RefreshToken(context.Context, string) (*ExternalToken, error) { return nil, nil }
func (m *mockProvider) IntrospectToken(ctx context.Context, token string) (*IntrospectionResponse, error) {
	return m.introspectFunc(ctx, token)
}

func TestNewOAuthConfigFromEnv(t *testing.T) {
	for _, key := range []string{
		"SSO_CLIENT_ID", "SSO_CLIENT_SECRET", "SSO_AUTHORIZATION_URL",
		"SSO_TOKEN_URL", "SSO_INTROSPECTION_URL", "SSO_ISSUER_HOST", "SSO_CALLBACK_URL",
	} {
		t.Setenv(key, "")
	}
	t.Setenv("SSO_CLIENT_ID", "my-client")
	t.Setenv("SSO_CLIENT_SECRET", "my-secret")

	cfg, err := NewOAuthConfigFromEnv()
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.ClientID != "my-client" {
		t.Errorf("ClientID = %q, want %q", cfg.ClientID, "my-client")
	}
	if cfg.ClientSecret != "my-secret" {
		t.Errorf("ClientSecret = %q, want %q", cfg.ClientSecret, "my-secret")
	}
}

func TestNewGenericProvider_Validation(t *testing.T) {
	tests := []struct {
		name    string
		cfg     *OAuthConfig
		wantErr string
	}{
		{
			name: "all fields present",
			cfg: &OAuthConfig{
				ClientID: "id", ClientSecret: "secret",
				AuthorizationURL: "https://sso/auth", TokenURL: "https://sso/token",
				IntrospectionURL: "https://sso/introspect",
			},
		},
		{
			name:    "missing all",
			cfg:     &OAuthConfig{},
			wantErr: "SSO_CLIENT_ID",
		},
		{
			name: "missing introspection",
			cfg: &OAuthConfig{
				ClientID: "id", ClientSecret: "secret",
				AuthorizationURL: "https://sso/auth", TokenURL: "https://sso/token",
			},
			wantErr: "SSO_INTROSPECTION_URL",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := NewGenericProvider(tt.cfg)
			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("error %q does not contain %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestExtractBearerToken(t *testing.T) {
	tests := []struct {
		name    string
		header  string
		want    string
		wantErr bool
	}{
		{name: "valid token", header: "Bearer abc123", want: "abc123"},
		{name: "valid with extra spaces", header: "Bearer   abc123  ", want: "abc123"},
		{name: "case insensitive bearer", header: "bearer abc123", want: "abc123"},
		{name: "missing header", header: "", wantErr: true},
		{name: "not bearer", header: "Basic abc123", wantErr: true},
		{name: "empty token", header: "Bearer ", wantErr: true},
		{name: "bearer only", header: "Bearer", wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := httptest.NewRequest(http.MethodGet, "/", nil)
			if tt.header != "" {
				r.Header.Set("Authorization", tt.header)
			}

			got, err := extractBearerToken(r)
			if (err != nil) != tt.wantErr {
				t.Fatalf("extractBearerToken() error = %v, wantErr %v", err, tt.wantErr)
			}
			if got != tt.want {
				t.Errorf("extractBearerToken() = %q, want %q", got, tt.want)
			}
		})
	}
}

func TestMiddleware_Authenticate(t *testing.T) {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError}))

	t.Run("missing authorization header returns 401", func(t *testing.T) {
		m := NewMiddleware(&mockProvider{}, logger)
		handler := m.Authenticate(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			t.Error("handler should not be called")
		}))

		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/mcp", nil))

		if rr.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rr.Code, http.StatusUnauthorized)
		}
		assertJSONErrorResponse(t, rr)
	})

	t.Run("inactive token returns 401", func(t *testing.T) {
		p := &mockProvider{introspectFunc: func(_ context.Context, _ string) (*IntrospectionResponse, error) {
			return &IntrospectionResponse{Active: false}, nil
		}}
		m := NewMiddleware(p, logger)
		handler := m.Authenticate(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			t.Error("handler should not be called")
		}))

		req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
		req.Header.Set("Authorization", "Bearer expired-token")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusUnauthorized {
			t.Errorf("status = %d, want %d", rr.Code, http.StatusUnauthorized)
		}
	})

	t.Run("active token passes through with context", func(t *testing.T) {
		p := &mockProvider{introspectFunc: func(_ context.Context, _ string) (*IntrospectionResponse, error) {
			return &IntrospectionResponse{Active: true, Sub: "user-123", Username: "testuser"}, nil
		}}
		m := NewMiddleware(p, logger)

		var capturedInfo *IntrospectionResponse
		handler := m.Authenticate(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
			info, ok := TokenInfoFromContext(r.Context())
			if !ok {
				t.Error("TokenInfoFromContext returned false")
				return
			}
			capturedInfo = info
		}))

		req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
		req.Header.Set("Authorization", "Bearer valid-token")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusOK {
			t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
		}
		if capturedInfo == nil {
			t.Fatal("capturedInfo is nil")
		}
		if capturedInfo.Sub != "user-123" {
			t.Errorf("Sub = %q, want %q", capturedInfo.Sub, "user-123")
		}
		if capturedInfo.Username != "testuser" {
			t.Errorf("Username = %q, want %q", capturedInfo.Username, "testuser")
		}
	})

	t.Run("cached token avoids repeated introspection", func(t *testing.T) {
		callCount := 0
		p := &mockProvider{introspectFunc: func(_ context.Context, _ string) (*IntrospectionResponse, error) {
			callCount++
			return &IntrospectionResponse{Active: true, Sub: "user-1"}, nil
		}}
		m := NewMiddleware(p, logger)
		handler := m.Authenticate(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))

		for range 3 {
			req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
			req.Header.Set("Authorization", "Bearer cached-token")
			rr := httptest.NewRecorder()
			handler.ServeHTTP(rr, req)
			if rr.Code != http.StatusOK {
				t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
			}
		}

		if callCount != 1 {
			t.Errorf("introspection called %d times, want 1 (cached)", callCount)
		}
	})

	t.Run("provider error returns 500", func(t *testing.T) {
		p := &mockProvider{introspectFunc: func(_ context.Context, _ string) (*IntrospectionResponse, error) {
			return nil, errors.New("provider unavailable")
		}}
		m := NewMiddleware(p, logger)
		handler := m.Authenticate(http.HandlerFunc(func(_ http.ResponseWriter, _ *http.Request) {
			t.Error("handler should not be called")
		}))

		req := httptest.NewRequest(http.MethodPost, "/mcp", nil)
		req.Header.Set("Authorization", "Bearer some-token")
		rr := httptest.NewRecorder()
		handler.ServeHTTP(rr, req)

		if rr.Code != http.StatusInternalServerError {
			t.Errorf("status = %d, want %d", rr.Code, http.StatusInternalServerError)
		}
	})
}

func TestTokenInfoContext(t *testing.T) {
	info := &IntrospectionResponse{Active: true, Sub: "user-42"}
	ctx := WithTokenInfo(context.Background(), info)

	got, ok := TokenInfoFromContext(ctx)
	if !ok {
		t.Fatal("TokenInfoFromContext returned false")
	}
	if got.Sub != "user-42" {
		t.Errorf("Sub = %q, want %q", got.Sub, "user-42")
	}

	_, ok = TokenInfoFromContext(context.Background())
	if ok {
		t.Error("TokenInfoFromContext should return false for empty context")
	}
}

func TestMetadataHandler(t *testing.T) {
	m := NewMiddleware(&mockProvider{}, slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	handler := m.MetadataHandler()

	rr := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/.well-known/oauth-authorization-server", nil)
	req.Host = "localhost:8080"
	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if ct := rr.Header().Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var metadata map[string]any
	if err := json.NewDecoder(rr.Body).Decode(&metadata); err != nil {
		t.Fatalf("failed to decode metadata: %v", err)
	}

	base := "http://localhost:8080"
	if metadata["issuer"] != base {
		t.Errorf("issuer = %v, want %q", metadata["issuer"], base)
	}
	if metadata["authorization_endpoint"] != base+"/auth/authorize" {
		t.Errorf("authorization_endpoint = %v", metadata["authorization_endpoint"])
	}
	if metadata["token_endpoint"] != base+"/auth/token" {
		t.Errorf("token_endpoint = %v", metadata["token_endpoint"])
	}
	if metadata["registration_endpoint"] != base+"/auth/register" {
		t.Errorf("registration_endpoint = %v", metadata["registration_endpoint"])
	}

	ccms, ok := metadata["code_challenge_methods_supported"].([]any)
	if !ok || len(ccms) == 0 {
		t.Error("code_challenge_methods_supported missing or empty")
	} else if ccms[0] != pkceMethodS256 {
		t.Errorf("code_challenge_methods_supported[0] = %v, want %s", ccms[0], pkceMethodS256)
	}
}

func TestValidatePKCE(t *testing.T) {
	verifier := "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk"
	challenge := "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM"

	if !ValidatePKCE(verifier, challenge, pkceMethodS256) {
		t.Error("ValidatePKCE should return true for known test vector")
	}
	if ValidatePKCE("wrong-verifier", challenge, pkceMethodS256) {
		t.Error("ValidatePKCE should return false for wrong verifier")
	}
	if ValidatePKCE("anything", "anything", "plain") {
		t.Error("ValidatePKCE should reject non-S256 methods")
	}
	if ValidatePKCE("", "challenge", pkceMethodS256) {
		t.Error("ValidatePKCE should reject empty verifier")
	}
}

func assertJSONErrorResponse(t *testing.T, rr *httptest.ResponseRecorder) {
	t.Helper()
	ct := rr.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
	var body map[string]string
	if err := json.NewDecoder(rr.Body).Decode(&body); err != nil {
		t.Fatalf("failed to decode error response: %v", err)
	}
	if body["error"] == "" {
		t.Error("error field is empty")
	}
	if body["error_description"] == "" {
		t.Error("error_description field is empty")
	}
}
