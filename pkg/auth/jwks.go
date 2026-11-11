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
	"fmt"
	"net/http"
	"time"

	"github.com/coreos/go-oidc/v3/oidc"
)

// JWKSVerifier validates JWT access tokens against the issuer's published
// signing keys, for deployments whose identity provider does not offer token
// introspection.
//
// Signature, issuer and expiry are all checked, and only RS256 is accepted.
// Unlike introspection this is an offline check, so a token revoked before its
// expiry still verifies until it expires.
type JWKSVerifier struct {
	verifier *oidc.IDTokenVerifier
}

// NewJWKSVerifier returns a verifier for tokens issued by issuer and signed
// with the keys published at jwksURL.
//
// Keys are fetched lazily, cached, and refetched when a token names one that
// is not held, so an issuer rotating its keys needs no restart here. Passing a
// nil httpClient uses a default with a timeout.
//
// A redirect to a plaintext endpoint is refused whatever client is given,
// because a key fetched over http can be swapped in flight by anyone on the
// path, and a swapped key forges every identity this server trusts.
func NewJWKSVerifier(jwksURL, issuer string, httpClient *http.Client) *JWKSVerifier {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 10 * time.Second}
	}
	guarded := *httpClient
	guarded.CheckRedirect = func(req *http.Request, _ []*http.Request) error {
		return requireSecureURL("JWKS redirect target", req.URL.String())
	}

	// Background rather than a request's context: the key set outlives any one
	// request, and cancelling it on the first caller to disconnect would leave
	// every later request without keys.
	keyCtx := oidc.ClientContext(context.Background(), &guarded)

	return &JWKSVerifier{
		verifier: oidc.NewVerifier(issuer, oidc.NewRemoteKeySet(keyCtx, jwksURL), &oidc.Config{
			// Named explicitly so the algorithm in the token header can never
			// select the verifier: "none" and an HS256 token signed with the
			// public key are both refused.
			SupportedSigningAlgs: []string{oidc.RS256},
			// These tokens are access tokens for other resources as well, so
			// their audience is not this server. Identity comes from the
			// subject, and what that subject may read is decided downstream.
			SkipClientIDCheck: true,
		}),
	}
}

// Verify checks the token and returns the same shape introspection would, so
// callers cannot tell the two apart.
//
// Any failure is reported as an error. The caller decides what that means: see
// Middleware.validateUncached, which treats it as an inactive token rather
// than a server fault, so a bad token reads as a refusal.
func (v *JWKSVerifier) Verify(ctx context.Context, token string) (*IntrospectionResponse, error) {
	idToken, err := v.verifier.Verify(ctx, token)
	if err != nil {
		return nil, fmt.Errorf("verifying token: %w", err)
	}

	var claims struct {
		AuthorizedParty   string `json:"azp"`
		PreferredUsername string `json:"preferred_username"`
		Email             string `json:"email"`
		Scope             string `json:"scope"`
	}
	if err := idToken.Claims(&claims); err != nil {
		return nil, fmt.Errorf("reading token claims: %w", err)
	}

	resp := &IntrospectionResponse{
		Active: true,
		Sub:    idToken.Subject,
		Iss:    idToken.Issuer,
		Scope:  claims.Scope,
		Exp:    idToken.Expiry.Unix(),
		Iat:    idToken.IssuedAt.Unix(),
		// azp names the client the token was minted for. client_id is the
		// introspection spelling, so a caller reads one field either way.
		ClientID: claims.AuthorizedParty,
	}
	resp.Username = claims.PreferredUsername
	if resp.Username == "" {
		resp.Username = claims.Email
	}
	return resp, nil
}
