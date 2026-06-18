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

import "context"

// Provider abstracts the upstream OAuth identity provider. Each implementation
// (generic OIDC, Google, etc.) handles the provider-specific details of building
// auth URLs, exchanging codes, refreshing tokens, and introspecting tokens.
//
// The OAuthServer and Middleware are provider-agnostic — they delegate all
// provider-specific work through this interface.
type Provider interface {
	// BuildAuthURL constructs the external authorization URL to redirect the user to.
	// callbackURL is our server's /auth/callback/oidc endpoint.
	// state is an opaque value passed through the redirect round-trip.
	BuildAuthURL(callbackURL, state string) (string, error)

	// ExchangeCode trades an authorization code received from the external provider
	// for an access token (and optionally a refresh token).
	ExchangeCode(ctx context.Context, code, callbackURL string) (*ExternalToken, error)

	// RefreshToken obtains a new access token using a refresh token.
	RefreshToken(ctx context.Context, refreshToken string) (*ExternalToken, error)

	// IntrospectToken validates an access token and returns its metadata.
	// Implementations may call an introspection endpoint (RFC 7662) or use
	// provider-specific validation (e.g. Google's tokeninfo).
	IntrospectToken(ctx context.Context, token string) (*IntrospectionResponse, error)
}
