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

package httpclient

import (
	"testing"
	"time"
)

func TestNewWithRetry(t *testing.T) {
	timeout := 30 * time.Second
	client := NewWithRetry(timeout)

	if client == nil {
		t.Fatal("NewWithRetry returned nil client")
	}
	if client.Transport == nil {
		t.Fatal("returned client has nil Transport, expected go-retryablehttp RoundTripper")
	}
}

func TestNewWithRetryZeroTimeout(t *testing.T) {
	client := NewWithRetry(0)

	if client == nil {
		t.Fatal("NewWithRetry with zero timeout returned nil client")
	}
}
