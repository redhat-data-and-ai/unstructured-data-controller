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
	"crypto/rand"
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	testIssuer = "https://sso.example.com/realms/test"
	// testKid is the only key the test JWKS publishes. A token naming anything
	// else must be refused.
	testKid = "key-1"
)

// quietLogger keeps expected rejection warnings out of the test output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// jwksServer serves the public half of key as a JWKS, counting fetches so a
// test can assert the keys are cached rather than refetched per token.
//
// The counter is atomic because the handler runs on the server's goroutine
// while the test reads it on its own.
type jwksServer struct {
	*httptest.Server
	fetches atomic.Int64
}

func newJWKSServer(t *testing.T, key *rsa.PrivateKey) *jwksServer {
	t.Helper()
	server := &jwksServer{}
	server.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		server.fetches.Add(1)
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kid": testKid,
			"kty": "RSA",
			"alg": "RS256",
			"use": "sig",
			"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
		// Serving half a key set would fail verification for a reason that has
		// nothing to do with what the test is checking.
		if err != nil {
			t.Errorf("serving the JWKS fixture: %v", err)
		}
	}))
	t.Cleanup(server.Close)
	return server
}

func signToken(t *testing.T, key *rsa.PrivateKey, kid string, claims jwt.MapClaims) string {
	t.Helper()
	token := jwt.NewWithClaims(jwt.SigningMethodRS256, claims)
	if kid != "" {
		token.Header["kid"] = kid
	}
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatalf("signing: %v", err)
	}
	return signed
}

func validClaims() jwt.MapClaims {
	return jwt.MapClaims{
		"iss":                testIssuer,
		"sub":                "user-123",
		"azp":                "unstructured-mcp-server",
		"preferred_username": "someone",
		"scope":              "openid email",
		"exp":                time.Now().Add(5 * time.Minute).Unix(),
		"iat":                time.Now().Unix(),
	}
}

func TestJWKSVerifierAcceptsAGenuineToken(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := newJWKSServer(t, key)
	verifier := NewJWKSVerifier(server.URL, testIssuer, server.Client())

	resp, err := verifier.Verify(context.Background(), signToken(t, key, testKid, validClaims()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if !resp.Active {
		t.Error("Active = false, want true")
	}
	if resp.Sub != "user-123" {
		t.Errorf("Sub = %q, want user-123", resp.Sub)
	}
	// A caller reads ClientID whichever mechanism produced the response, so
	// azp has to land there.
	if resp.ClientID != "unstructured-mcp-server" {
		t.Errorf("ClientID = %q, want unstructured-mcp-server", resp.ClientID)
	}
	if resp.Username != "someone" {
		t.Errorf("Username = %q, want someone", resp.Username)
	}
	if resp.Iss != testIssuer {
		t.Errorf("Iss = %q, want %q", resp.Iss, testIssuer)
	}
}

// Falls back to email when the token carries no preferred_username, since that
// is the only other thing identifying the caller.
func TestJWKSVerifierFallsBackToEmail(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := newJWKSServer(t, key)
	verifier := NewJWKSVerifier(server.URL, testIssuer, server.Client())

	claims := validClaims()
	delete(claims, "preferred_username")
	claims["email"] = "someone@example.com"

	resp, err := verifier.Verify(context.Background(), signToken(t, key, testKid, claims))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if resp.Username != "someone@example.com" {
		t.Errorf("Username = %q, want the email", resp.Username)
	}
}

func TestJWKSVerifierRejections(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	attacker, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := newJWKSServer(t, key)
	verifier := NewJWKSVerifier(server.URL, testIssuer, server.Client())

	expired := validClaims()
	expired["exp"] = time.Now().Add(-time.Minute).Unix()

	wrongIssuer := validClaims()
	wrongIssuer["iss"] = "https://evil.example.com/realms/test"

	tests := []struct {
		name  string
		token string
	}{
		{"signed by somebody else", signToken(t, attacker, testKid, validClaims())},
		{"expired", signToken(t, key, testKid, expired)},
		{"issued by another issuer", signToken(t, key, testKid, wrongIssuer)},
		{"names an unknown key", signToken(t, key, "key-99", validClaims())},
		{"not a JWT", "not-a-token"},
		{"empty", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := verifier.Verify(context.Background(), tt.token); err == nil {
				t.Fatal("Verify accepted a token it should have refused")
			}
		})
	}
}

// The alg header must not choose the algorithm. "none" would make every token
// self-signing, and HS256 would let the public key be used as a shared secret.
func TestJWKSVerifierRefusesAlgorithmSubstitution(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := newJWKSServer(t, key)
	verifier := NewJWKSVerifier(server.URL, testIssuer, server.Client())

	unsigned := jwt.NewWithClaims(jwt.SigningMethodNone, validClaims())
	unsigned.Header["kid"] = testKid
	noneToken, err := unsigned.SignedString(jwt.UnsafeAllowNoneSignatureType)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(context.Background(), noneToken); err == nil {
		t.Error("an alg=none token was accepted")
	}

	// Sign with the public modulus as an HMAC secret, the classic confusion.
	hmacToken := jwt.NewWithClaims(jwt.SigningMethodHS256, validClaims())
	hmacToken.Header["kid"] = testKid
	forged, err := hmacToken.SignedString(key.N.Bytes())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.Verify(context.Background(), forged); err == nil {
		t.Error("an HS256 token signed with the public key was accepted")
	}
}

// Fetching the key set per token would put a request to the identity provider
// on the path of every single call.
func TestJWKSVerifierCachesKeys(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := newJWKSServer(t, key)
	verifier := NewJWKSVerifier(server.URL, testIssuer, server.Client())

	const verifications = 5
	for range verifications {
		if _, err := verifier.Verify(context.Background(), signToken(t, key, testKid, validClaims())); err != nil {
			t.Fatal(err)
		}
	}

	if got := server.fetches.Load(); got >= verifications {
		t.Errorf("fetched the JWKS %d times for %d tokens, want the keys cached",
			got, verifications)
	}
}

// An https endpoint that redirects to plaintext would let anyone on that hop
// substitute the signing key, which forges every identity this server trusts.
func TestJWKSVerifierRefusesARedirectToPlaintext(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	// Redirects off-loopback, so the loopback exemption does not apply.
	redirector := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, "http://keys.example.com/certs", http.StatusFound)
	}))
	defer redirector.Close()

	verifier := NewJWKSVerifier(redirector.URL, testIssuer, redirector.Client())

	if _, err := verifier.Verify(context.Background(),
		signToken(t, key, testKid, validClaims())); err == nil {
		t.Fatal("a redirect to a plaintext key endpoint was followed")
	}
}

// An unreachable key endpoint must fail closed, never accept the token.
func TestJWKSVerifierFailsClosedWhenKeysAreUnreachable(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer down.Close()

	verifier := NewJWKSVerifier(down.URL, testIssuer, down.Client())

	resp, err := verifier.Verify(context.Background(), signToken(t, key, testKid, validClaims()))
	if err == nil {
		t.Fatal("Verify succeeded while the key endpoint was down")
	}
	if resp != nil {
		t.Error("Verify returned a response alongside an error")
	}
}

// A token that does not verify must read as "not authorised" rather than as a
// server fault, so an attacker cannot turn a crafted token into a 500.
func TestMiddlewareJWKSFailureModes(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	attacker, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := newJWKSServer(t, key)

	middleware := NewMiddleware(&mockProvider{}, quietLogger(), false,
		WithJWKSVerifier(NewJWKSVerifier(server.URL, testIssuer, server.Client())))
	defer middleware.Close()

	if !middleware.UsesJWKS() {
		t.Error("UsesJWKS = false after WithJWKSVerifier")
	}

	resp, err := middleware.validateUncached(context.Background(),
		signToken(t, attacker, testKid, validClaims()))
	if err != nil {
		t.Fatalf("a forged token should be inactive, not an error: %v", err)
	}
	if resp.Active {
		t.Error("a forged token was reported active")
	}

	// Config refuses this combination, so this is the backstop. The verifier
	// must still run, and UsesJWKS must not claim a check a caller can rely on.
	confused := NewMiddleware(&mockProvider{}, quietLogger(), true,
		WithJWKSVerifier(NewJWKSVerifier(server.URL, testIssuer, server.Client())))
	defer confused.Close()

	if confused.UsesJWKS() {
		t.Error("UsesJWKS = true while SSO_DISABLE_INTROSPECTION is also set")
	}
	forged, err := confused.validateToken(context.Background(),
		signToken(t, attacker, testKid, validClaims()))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if forged.Active {
		t.Error("a forged token was accepted because SSO_DISABLE_INTROSPECTION skipped the verifier")
	}
}

func TestNewOAuthConfigFromEnvTokenValidation(t *testing.T) {
	tests := []struct {
		name    string
		env     map[string]string
		wantErr string
		wantMod ValidationMode
	}{
		{"unset defaults to introspection", nil, "", ValidationIntrospect},
		{"introspect is accepted",
			map[string]string{"SSO_TOKEN_VALIDATION": "introspect"}, "", ValidationIntrospect},
		{"jwks needs a key URL",
			map[string]string{"SSO_TOKEN_VALIDATION": "jwks", "SSO_ISSUER": testIssuer},
			"SSO_JWKS_URL", ""},
		{"jwks needs an issuer",
			map[string]string{"SSO_TOKEN_VALIDATION": "jwks", "SSO_JWKS_URL": "https://x/certs"},
			"SSO_ISSUER", ""},
		{"jwks with both is accepted",
			map[string]string{"SSO_TOKEN_VALIDATION": "jwks", "SSO_JWKS_URL": "https://x/certs",
				"SSO_ISSUER": testIssuer}, "", ValidationJWKS},
		{"an unknown mode is refused rather than ignored",
			map[string]string{"SSO_TOKEN_VALIDATION": "trustme"}, "want", ""},
		// A plaintext key endpoint lets anyone on the path serve their own
		// signing key, which forges every identity the server trusts.
		{"a plaintext key URL is refused",
			map[string]string{"SSO_TOKEN_VALIDATION": "jwks", "SSO_ISSUER": testIssuer,
				"SSO_JWKS_URL": "http://sso.example.com/certs"}, "must use https", ""},
		{"a plaintext key URL on loopback is allowed for local dev",
			map[string]string{"SSO_TOKEN_VALIDATION": "jwks", "SSO_ISSUER": testIssuer,
				"SSO_JWKS_URL": "http://localhost:8443/certs"}, "", ValidationJWKS},
		// Parses with a scheme and no endpoint, so it would start cleanly and
		// then fail on the first key fetch.
		{"an https key URL with no host is refused",
			map[string]string{"SSO_TOKEN_VALIDATION": "jwks", "SSO_ISSUER": testIssuer,
				"SSO_JWKS_URL": "https:///certs"}, "has no host", ""},
		// These two flags answer the same problem, and silently letting the
		// older one win would leave signatures unchecked while startup claims
		// they are verified.
		{"jwks combined with the unvalidated path is refused",
			map[string]string{"SSO_TOKEN_VALIDATION": "jwks", "SSO_ISSUER": testIssuer,
				"SSO_JWKS_URL":              "https://sso.example.com/certs",
				"SSO_DISABLE_INTROSPECTION": "true"}, "cannot be combined", ""},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Cleared first, so a developer running with any of these exported
			// does not change what a case means. The "unset defaults to
			// introspection" case would otherwise fail with no code change.
			for _, key := range []string{
				"SSO_TOKEN_VALIDATION", "SSO_JWKS_URL", "SSO_ISSUER",
				"SSO_DISABLE_INTROSPECTION",
			} {
				t.Setenv(key, "")
			}
			for key, value := range tt.env {
				t.Setenv(key, value)
			}

			cfg, err := NewOAuthConfigFromEnv()

			if tt.wantErr != "" {
				if err == nil {
					t.Fatalf("expected an error mentioning %q", tt.wantErr)
				}
				if !strings.Contains(err.Error(), tt.wantErr) {
					t.Errorf("error %q does not mention %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if cfg.TokenValidation != tt.wantMod {
				t.Errorf("TokenValidation = %q, want %q", cfg.TokenValidation, tt.wantMod)
			}
		})
	}
}

// A provider with no introspection endpoint is the reason jwks mode exists, so
// requiring a URL it does not have would make that mode unstartable.
func TestNewGenericProviderDoesNotRequireIntrospectionInJWKSMode(t *testing.T) {
	cfg := &OAuthConfig{
		ClientID:         "client",
		ClientSecret:     "secret",
		AuthorizationURL: "https://sso.example.com/auth",
		TokenURL:         "https://sso.example.com/token",
		TokenValidation:  ValidationJWKS,
	}

	if _, err := NewGenericProvider(cfg); err != nil {
		t.Fatalf("jwks mode should start without SSO_INTROSPECTION_URL: %v", err)
	}

	cfg.TokenValidation = ValidationIntrospect
	if _, err := NewGenericProvider(cfg); err == nil {
		t.Error("introspect mode started without SSO_INTROSPECTION_URL")
	}
}
