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
	"errors"
	"io"
	"log/slog"
	"math/big"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

const (
	testIssuer = "https://sso.example.com/realms/test"
	// testKid is the only key the test JWKS publishes. A token naming anything
	// else must be refused, which is what the unknown-key case checks.
	testKid = "key-1"
)

// quietLogger keeps expected rejection warnings out of the test output.
func quietLogger() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

// jwksServer serves the public half of key as a JWKS, counting fetches so a
// test can assert the keys are cached rather than refetched per token.
type jwksServer struct {
	*httptest.Server
	fetches int
}

func newJWKSServer(t *testing.T, key *rsa.PrivateKey) *jwksServer {
	t.Helper()
	server := &jwksServer{}
	server.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		server.fetches++
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"keys": []map[string]string{{
			"kid": testKid,
			"kty": "RSA",
			"alg": "RS256",
			"use": "sig",
			"n":   base64.RawURLEncoding.EncodeToString(key.N.Bytes()),
			"e":   base64.RawURLEncoding.EncodeToString(big.NewInt(int64(key.E)).Bytes()),
		}}})
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
	// The allow-list reads ClientID, so azp has to land there or the check
	// silently passes everything.
	if resp.ClientID != "unstructured-mcp-server" {
		t.Errorf("ClientID = %q, want unstructured-mcp-server", resp.ClientID)
	}
	if resp.Username != "someone" {
		t.Errorf("Username = %q, want someone", resp.Username)
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

	noExpiry := validClaims()
	delete(noExpiry, "exp")

	tests := []struct {
		name  string
		token string
	}{
		{"signed by somebody else", signToken(t, attacker, testKid, validClaims())},
		{"expired", signToken(t, key, testKid, expired)},
		{"issued by another issuer", signToken(t, key, testKid, wrongIssuer)},
		{"no expiry at all", signToken(t, key, testKid, noExpiry)},
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

// A kid of the wrong type is a malformed header, not an absent one. Treating
// it as absent would fall through to the single-key path and verify against a
// key the token never named.
func TestJWKSVerifierRefusesANonStringKid(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := newJWKSServer(t, key)
	verifier := NewJWKSVerifier(server.URL, testIssuer, server.Client())

	token := jwt.NewWithClaims(jwt.SigningMethodRS256, validClaims())
	token.Header["kid"] = 12345
	signed, err := token.SignedString(key)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := verifier.Verify(context.Background(), signed); !errors.Is(err, ErrNoSuchKey) {
		t.Fatalf("error = %v, want one wrapping ErrNoSuchKey", err)
	}
}

func TestJWKSVerifierCachesKeys(t *testing.T) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	server := newJWKSServer(t, key)
	verifier := NewJWKSVerifier(server.URL, testIssuer, server.Client())

	for range 5 {
		if _, err := verifier.Verify(context.Background(), signToken(t, key, testKid, validClaims())); err != nil {
			t.Fatal(err)
		}
	}

	if server.fetches != 1 {
		t.Errorf("fetched the JWKS %d times, want 1", server.fetches)
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

func TestRSAPublicKeyFromJWK(t *testing.T) {
	tests := []struct {
		name     string
		modulus  string
		exponent string
	}{
		{"modulus is not base64url", "!!!not base64!!!", "AQAB"},
		{"exponent is not base64url", base64.RawURLEncoding.EncodeToString([]byte{1, 2, 3}), "!!!"},
		{"exponent is empty", base64.RawURLEncoding.EncodeToString([]byte{1, 2, 3}), ""},
		{"exponent is zero", base64.RawURLEncoding.EncodeToString([]byte{1, 2, 3}),
			base64.RawURLEncoding.EncodeToString([]byte{0})},
		{"exponent is absurdly long", base64.RawURLEncoding.EncodeToString([]byte{1, 2, 3}),
			base64.RawURLEncoding.EncodeToString([]byte{1, 2, 3, 4, 5, 6, 7, 8, 9})},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := rsaPublicKeyFromJWK(tt.modulus, tt.exponent); err == nil {
				t.Error("accepted a malformed JWK")
			}
		})
	}

	key, err := rsaPublicKeyFromJWK(
		base64.RawURLEncoding.EncodeToString(big.NewInt(0).SetBytes([]byte{1, 2, 3}).Bytes()),
		base64.RawURLEncoding.EncodeToString(big.NewInt(65537).Bytes()))
	if err != nil {
		t.Fatalf("a well formed JWK was refused: %v", err)
	}
	if key.E != 65537 {
		t.Errorf("E = %d, want 65537", key.E)
	}
}

// A bad signature must read as "not authorised", while an unreachable key
// endpoint must read as a server fault. Collapsing the two either hides an
// outage or turns one into a 401 storm.
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

	resp, err := middleware.validateUncached(context.Background(),
		signToken(t, attacker, testKid, validClaims()))
	if err != nil {
		t.Fatalf("a forged token should be inactive, not an error: %v", err)
	}
	if resp.Active {
		t.Error("a forged token was reported active")
	}

	down := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer down.Close()

	outage := NewMiddleware(&mockProvider{}, quietLogger(), false,
		WithJWKSVerifier(NewJWKSVerifier(down.URL, testIssuer, down.Client())))
	defer outage.Close()

	if _, err := outage.validateUncached(context.Background(),
		signToken(t, key, testKid, validClaims())); err == nil {
		t.Error("a key endpoint outage was reported as a valid answer")
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
