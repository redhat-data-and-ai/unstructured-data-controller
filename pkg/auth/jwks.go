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
	"crypto/rsa"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"net/http"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

// ErrNoSuchKey means the token names a signing key the issuer does not publish.
var ErrNoSuchKey = errors.New("no signing key matches the token's kid")

// jwksDocument is the JSON Web Key Set published by an issuer, per RFC 7517.
type jwksDocument struct {
	Keys []jsonWebKey `json:"keys"`
}

// jsonWebKey is one published key. Only the RSA fields this package verifies
// with are declared; anything else in the set is ignored.
type jsonWebKey struct {
	Kid string `json:"kid"`
	Kty string `json:"kty"`
	Alg string `json:"alg"`
	Use string `json:"use"`
	N   string `json:"n"`
	E   string `json:"e"`
}

// jwksMinRefreshInterval bounds how often an unknown kid may trigger a fetch,
// so a stream of tokens with bogus kids cannot be turned into a request flood
// against the identity provider.
const jwksMinRefreshInterval = time.Minute

// JWKSVerifier validates JWT access tokens against the issuer's published
// signing keys, for deployments whose identity provider does not offer token
// introspection.
//
// Signature, issuer and expiry are all checked. Unlike introspection this is an
// offline check, so a token revoked before its expiry still verifies.
type JWKSVerifier struct {
	jwksURL    string
	issuer     string
	httpClient *http.Client

	mu          sync.RWMutex
	keys        map[string]*rsa.PublicKey
	lastFetched time.Time
}

// NewJWKSVerifier returns a verifier for tokens issued by issuer and signed
// with the keys published at jwksURL. Keys are fetched lazily and refreshed
// when a token names an unknown key.
func NewJWKSVerifier(jwksURL, issuer string, httpClient *http.Client) *JWKSVerifier {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	return &JWKSVerifier{
		jwksURL:    jwksURL,
		issuer:     issuer,
		httpClient: httpClient,
		keys:       make(map[string]*rsa.PublicKey),
	}
}

// Verify checks the token's signature, issuer and expiry, and returns the same
// shape introspection would, so callers cannot tell the two apart.
//
// Returns an error rather than an inactive response when the token cannot be
// trusted, so a verification outage can never read as a valid anonymous caller.
func (v *JWKSVerifier) Verify(ctx context.Context, token string) (*IntrospectionResponse, error) {
	claims := jwt.MapClaims{}
	parser := jwt.NewParser(
		// RS256 only. Accepting the algorithm named in the header would let a
		// caller choose "none", or sign with the public key as an HMAC secret.
		jwt.WithValidMethods([]string{"RS256"}),
		jwt.WithExpirationRequired(),
		jwt.WithIssuer(v.issuer),
	)

	if _, err := parser.ParseWithClaims(token, claims, v.keyForToken(ctx)); err != nil {
		return nil, fmt.Errorf("verifying token: %w", err)
	}

	resp := &IntrospectionResponse{Active: true, Iss: v.issuer}
	if sub, ok := claims["sub"].(string); ok {
		resp.Sub = sub
	}
	// azp names the client the token was minted for. client_id is the
	// introspection spelling, so the allow-list reads one field either way.
	if azp, ok := claims["azp"].(string); ok {
		resp.ClientID = azp
	}
	if username, ok := claims["preferred_username"].(string); ok {
		resp.Username = username
	} else if email, ok := claims["email"].(string); ok {
		resp.Username = email
	}
	if scope, ok := claims["scope"].(string); ok {
		resp.Scope = scope
	}
	if exp, ok := claims["exp"].(float64); ok {
		resp.Exp = int64(exp)
	}
	if iat, ok := claims["iat"].(float64); ok {
		resp.Iat = int64(iat)
	}
	return resp, nil
}

// keyForToken resolves the signing key named by the token's kid header.
func (v *JWKSVerifier) keyForToken(ctx context.Context) jwt.Keyfunc {
	return func(token *jwt.Token) (any, error) {
		// A kid that is present but not a string is a malformed header, not a
		// missing one, so refuse rather than falling back to the single-key
		// path and verifying against a key the token never named.
		rawKid, present := token.Header["kid"]
		kid, isString := rawKid.(string)
		if present && !isString {
			return nil, fmt.Errorf("%w: kid header is %T, want string", ErrNoSuchKey, rawKid)
		}

		if key := v.cachedKey(kid); key != nil {
			return key, nil
		}

		// An unknown kid usually means the issuer rotated its keys, so refetch
		// once before giving up.
		if err := v.refresh(ctx); err != nil {
			return nil, fmt.Errorf("fetching signing keys: %w", err)
		}
		if key := v.cachedKey(kid); key != nil {
			return key, nil
		}
		return nil, fmt.Errorf("%w: %q", ErrNoSuchKey, kid)
	}
}

func (v *JWKSVerifier) cachedKey(kid string) *rsa.PublicKey {
	v.mu.RLock()
	defer v.mu.RUnlock()

	if key, ok := v.keys[kid]; ok {
		return key
	}
	// A JWKS with exactly one key need not name it, and some issuers omit kid
	// from the token header entirely.
	if kid == "" && len(v.keys) == 1 {
		for _, key := range v.keys {
			return key
		}
	}
	return nil
}

// refresh replaces the cached keys with those currently published.
func (v *JWKSVerifier) refresh(ctx context.Context) error {
	v.mu.Lock()
	if time.Since(v.lastFetched) < jwksMinRefreshInterval && len(v.keys) > 0 {
		v.mu.Unlock()
		return nil
	}
	v.mu.Unlock()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, v.jwksURL, nil)
	if err != nil {
		return fmt.Errorf("building JWKS request: %w", err)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := v.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("calling JWKS endpoint: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("JWKS endpoint returned HTTP %d", resp.StatusCode)
	}

	var document jwksDocument
	if err := json.NewDecoder(resp.Body).Decode(&document); err != nil {
		return fmt.Errorf("decoding JWKS: %w", err)
	}

	keys := make(map[string]*rsa.PublicKey, len(document.Keys))
	for _, key := range document.Keys {
		if key.Kty != "RSA" || (key.Use != "" && key.Use != "sig") {
			continue
		}
		if key.Alg != "" && key.Alg != "RS256" {
			continue
		}
		publicKey, err := rsaPublicKeyFromJWK(key.N, key.E)
		if err != nil {
			// One malformed key must not hide the rest of the set.
			continue
		}
		keys[key.Kid] = publicKey
	}
	if len(keys) == 0 {
		return errors.New("JWKS contained no usable RS256 signing key")
	}

	v.mu.Lock()
	v.keys = keys
	v.lastFetched = time.Now()
	v.mu.Unlock()
	return nil
}

// rsaPublicKeyFromJWK rebuilds an RSA public key from the base64url modulus and
// exponent of a JWK.
func rsaPublicKeyFromJWK(modulus, exponent string) (*rsa.PublicKey, error) {
	rawModulus, err := base64.RawURLEncoding.DecodeString(modulus)
	if err != nil {
		return nil, fmt.Errorf("decoding modulus: %w", err)
	}
	rawExponent, err := base64.RawURLEncoding.DecodeString(exponent)
	if err != nil {
		return nil, fmt.Errorf("decoding exponent: %w", err)
	}
	if len(rawExponent) == 0 || len(rawExponent) > 8 {
		return nil, fmt.Errorf("exponent is %d bytes, want 1 to 8", len(rawExponent))
	}

	var value uint64
	for _, b := range rawExponent {
		value = value<<8 | uint64(b)
	}
	if value == 0 || value > 1<<31-1 {
		return nil, fmt.Errorf("exponent %d is out of range", value)
	}

	return &rsa.PublicKey{N: new(big.Int).SetBytes(rawModulus), E: int(value)}, nil
}
