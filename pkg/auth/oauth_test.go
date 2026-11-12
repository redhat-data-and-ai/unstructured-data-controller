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
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	pkceMethodS256 = "S256"
	testUsername   = "testuser"
)

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
		"SSO_TOKEN_URL", "SSO_INTROSPECTION_URL", "SSO_CALLBACK_URL",
		"SSO_DISABLE_INTROSPECTION",
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
		{
			name: "missing introspection allowed when disabled",
			cfg: &OAuthConfig{
				ClientID: "id", ClientSecret: "secret",
				AuthorizationURL: "https://sso/auth", TokenURL: "https://sso/token",
				DisableIntrospection: true,
			},
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
		m := NewMiddleware(&mockProvider{}, logger, false)
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
		m := NewMiddleware(p, logger, false)
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
			return &IntrospectionResponse{Active: true, Sub: "user-123", Username: testUsername}, nil
		}}
		m := NewMiddleware(p, logger, false)

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
		if capturedInfo.Username != testUsername {
			t.Errorf("Username = %q, want %q", capturedInfo.Username, testUsername)
		}
	})

	t.Run("cached token avoids repeated introspection", func(t *testing.T) {
		callCount := 0
		p := &mockProvider{introspectFunc: func(_ context.Context, _ string) (*IntrospectionResponse, error) {
			callCount++
			return &IntrospectionResponse{Active: true, Sub: "user-1"}, nil
		}}
		m := NewMiddleware(p, logger, false)
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
		m := NewMiddleware(p, logger, false)
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
	m := NewMiddleware(&mockProvider{}, slog.New(slog.NewJSONHandler(os.Stdout, nil)), false)
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

func TestClaimsFromJWT_ExpiredToken(t *testing.T) {
	expiredClaims := jwt.MapClaims{
		"sub":                testUsername,
		"preferred_username": testUsername,
		"exp":                float64(time.Now().Add(-10 * time.Minute).Unix()),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, expiredClaims)
	tokenString, _ := token.SignedString([]byte("test-secret"))

	resp := claimsFromJWT(tokenString)

	if resp.Active {
		t.Fatal("expired token should have Active=false")
	}
	if resp.Username != testUsername {
		t.Errorf("Username = %q, want %q", resp.Username, testUsername)
	}
}

func TestClaimsFromJWT_ValidToken(t *testing.T) {
	validClaims := jwt.MapClaims{
		"sub":                testUsername,
		"preferred_username": testUsername,
		"exp":                float64(time.Now().Add(30 * time.Minute).Unix()),
	}
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, validClaims)
	tokenString, _ := token.SignedString([]byte("test-secret"))

	resp := claimsFromJWT(tokenString)

	if !resp.Active {
		t.Fatal("valid token should have Active=true")
	}
	if resp.Username != testUsername {
		t.Errorf("Username = %q, want %q", resp.Username, testUsername)
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

// A tool handler's context belongs to the MCP session, fixed when the session
// was opened, so it names whoever ran initialize rather than whoever sent the
// current call. These cover the guard that catches that mismatch.
func TestCheckRequestMatchesSession(t *testing.T) {
	sessionCtx := func(token string) context.Context {
		return WithAccessToken(context.Background(), token)
	}
	headerWith := func(value string) http.Header {
		header := http.Header{}
		if value != "" {
			header.Set("Authorization", value)
		}
		return header
	}

	tests := []struct {
		name    string
		ctx     context.Context
		header  http.Header
		wantErr bool
	}{
		{
			name:   "same token passes",
			ctx:    sessionCtx("alice-token"),
			header: headerWith("Bearer alice-token"),
		},
		{
			name:   "bearer keyword is case insensitive",
			ctx:    sessionCtx("alice-token"),
			header: headerWith("bearer alice-token"),
		},
		{
			// The whole point: a second caller on someone else's session.
			name:    "a different token is refused",
			ctx:     sessionCtx("alice-token"),
			header:  headerWith("Bearer mallory-token"),
			wantErr: true,
		},
		{
			name:    "a token that is a prefix of the session's is refused",
			ctx:     sessionCtx("alice-token-long"),
			header:  headerWith("Bearer alice-token"),
			wantErr: true,
		},
		{
			name:    "no header is refused rather than waved through",
			ctx:     sessionCtx("alice-token"),
			header:  headerWith(""),
			wantErr: true,
		},
		{
			name:    "nil header is refused, since the caller cannot vouch for the request",
			ctx:     sessionCtx("alice-token"),
			header:  nil,
			wantErr: true,
		},
		{
			name:    "a malformed header is refused",
			ctx:     sessionCtx("alice-token"),
			header:  headerWith("alice-token"),
			wantErr: true,
		},
		{
			name:    "no session token is refused",
			ctx:     context.Background(),
			header:  headerWith("Bearer alice-token"),
			wantErr: true,
		},
		{
			name:    "an empty session token is refused",
			ctx:     sessionCtx(""),
			header:  headerWith("Bearer alice-token"),
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// nil identifier: these cases either match the session token
			// exactly or must be refused outright, so none needs one.
			_, err := CheckRequestMatchesSession(tt.ctx, tt.header, nil)

			if tt.wantErr {
				if !errors.Is(err, ErrSessionUserMismatch) {
					t.Errorf("error = %v, want one wrapping ErrSessionUserMismatch", err)
				}
				return
			}
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

// fakeIdentifier answers for known tokens and refuses anything else.
type fakeIdentifier struct {
	subjects map[string]*IntrospectionResponse
	err      error
	calls    int
}

func (f *fakeIdentifier) Identify(_ context.Context, token string) (*IntrospectionResponse, error) {
	f.calls++
	if f.err != nil {
		return nil, f.err
	}
	resp, ok := f.subjects[token]
	if !ok {
		return &IntrospectionResponse{Active: false}, nil
	}
	return resp, nil
}

// A client that refreshes its access token mid-session is still the same
// person, so the tool has to keep working. Comparing token strings alone would
// lock them out until they reconnected.
func TestCheckRequestMatchesSessionAcceptsARefreshedToken(t *testing.T) {
	sessionInfo := &IntrospectionResponse{Active: true, Sub: "user-123", Username: "alice"}
	ctx := WithTokenInfo(WithAccessToken(context.Background(), "old-token"), sessionInfo)

	header := http.Header{}
	header.Set("Authorization", "Bearer refreshed-token")

	identifier := &fakeIdentifier{subjects: map[string]*IntrospectionResponse{
		"refreshed-token": {Active: true, Sub: "user-123", Username: "alice"},
	}}

	token, err := CheckRequestMatchesSession(ctx, header, identifier)
	if err != nil {
		t.Errorf("a refreshed token for the same subject was refused: %v", err)
	}
	// The caller acts with what comes back. Handing over the session's token
	// here would mean acting with the stale one the refresh just replaced.
	if token != "refreshed-token" {
		t.Errorf("returned %q, want the request's own token", token)
	}
	if identifier.calls != 1 {
		t.Errorf("validated %d times, want 1", identifier.calls)
	}
}

// A rejected request must not hand back a token, since a caller that ignores
// the error would then act with one that was never approved.
func TestCheckRequestMatchesSessionReturnsNoTokenOnRejection(t *testing.T) {
	sessionInfo := &IntrospectionResponse{Active: true, Sub: "user-123"}
	ctx := WithTokenInfo(WithAccessToken(context.Background(), "session-token"), sessionInfo)

	header := http.Header{}
	header.Set("Authorization", "Bearer someone-elses-token")

	identifier := &fakeIdentifier{subjects: map[string]*IntrospectionResponse{
		"someone-elses-token": {Active: true, Sub: "user-999"},
	}}

	token, err := CheckRequestMatchesSession(ctx, header, identifier)

	if err == nil {
		t.Fatal("a token for a different subject was accepted")
	}
	if token != "" {
		t.Errorf("returned token %q alongside an error, want empty", token)
	}
}

func TestCheckRequestMatchesSessionRejectsADifferentSubject(t *testing.T) {
	tests := []struct {
		name        string
		sessionInfo *IntrospectionResponse
		identifier  *fakeIdentifier
	}{
		{
			name:        "another live user's token",
			sessionInfo: &IntrospectionResponse{Active: true, Sub: "user-123"},
			identifier: &fakeIdentifier{subjects: map[string]*IntrospectionResponse{
				"other-token": {Active: true, Sub: "user-999"},
			}},
		},
		{
			name:        "a token that is no longer active",
			sessionInfo: &IntrospectionResponse{Active: true, Sub: "user-123"},
			identifier: &fakeIdentifier{subjects: map[string]*IntrospectionResponse{
				"other-token": {Active: false, Sub: "user-123"},
			}},
		},
		{
			name:        "a token nobody recognises",
			sessionInfo: &IntrospectionResponse{Active: true, Sub: "user-123"},
			identifier:  &fakeIdentifier{subjects: map[string]*IntrospectionResponse{}},
		},
		{
			name:        "validation itself failing",
			sessionInfo: &IntrospectionResponse{Active: true, Sub: "user-123"},
			identifier:  &fakeIdentifier{err: errors.New("introspection endpoint down")},
		},
		{
			// With no subject there is nothing to compare, so a changed token
			// cannot be shown to belong to the same person.
			name:        "a session whose token carried no subject",
			sessionInfo: &IntrospectionResponse{Active: true},
			identifier: &fakeIdentifier{subjects: map[string]*IntrospectionResponse{
				"other-token": {Active: true, Sub: "user-123"},
			}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := WithTokenInfo(WithAccessToken(context.Background(), "session-token"), tt.sessionInfo)
			header := http.Header{}
			header.Set("Authorization", "Bearer other-token")

			_, err := CheckRequestMatchesSession(ctx, header, tt.identifier)

			if !errors.Is(err, ErrSessionUserMismatch) {
				t.Errorf("error = %v, want one wrapping ErrSessionUserMismatch", err)
			}
		})
	}
}

// The common case, an unchanged token, must not pay for an introspection call.
func TestCheckRequestMatchesSessionSkipsValidationForTheSameToken(t *testing.T) {
	ctx := WithTokenInfo(
		WithAccessToken(context.Background(), "same-token"),
		&IntrospectionResponse{Active: true, Sub: "user-123"},
	)
	header := http.Header{}
	header.Set("Authorization", "Bearer same-token")

	identifier := &fakeIdentifier{}

	if _, err := CheckRequestMatchesSession(ctx, header, identifier); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if identifier.calls != 0 {
		t.Errorf("validated %d times for an unchanged token, want 0", identifier.calls)
	}
}

func TestSubjectOf(t *testing.T) {
	tests := []struct {
		name string
		resp *IntrospectionResponse
		want string
	}{
		{"sub wins over username", &IntrospectionResponse{Sub: "s", Username: "u"}, "s"},
		{"username is the fallback", &IntrospectionResponse{Username: "u"}, "u"},
		{"neither gives nothing", &IntrospectionResponse{}, ""},
		{"nil gives nothing", nil, ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SubjectOf(tt.resp); got != tt.want {
				t.Errorf("SubjectOf = %q, want %q", got, tt.want)
			}
		})
	}
}
