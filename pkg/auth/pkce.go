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
	"crypto/sha256"
	"encoding/base64"
)

// ValidatePKCE verifies that a code_verifier matches the stored code_challenge
// using the S256 method (SHA256 + base64url without padding) per RFC 7636.
func ValidatePKCE(verifier, challenge, method string) bool {
	if method != "S256" || verifier == "" || challenge == "" {
		return false
	}
	hash := sha256.Sum256([]byte(verifier))
	computed := base64.RawURLEncoding.EncodeToString(hash[:])
	return computed == challenge
}
