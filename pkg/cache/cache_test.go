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

package cache

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/cache/inmemory"
)

func TestNewInMemoryCacheInstance(t *testing.T) {
	config := Config{
		Driver: "memory",
		InMemory: &inmemory.Config{
			DefaultExpiration: 15,
			CleanupInterval:   30,
		},
	}

	mem, err := New(&config)
	assert.Nil(t, err)
	assert.NotNil(t, mem)
}

func TestNewCacheNilConfig(t *testing.T) {
	_, err := New(nil)
	assert.NotNil(t, err)
}

func TestNewCacheInvalidDriver(t *testing.T) {
	config := Config{
		Driver: "invalid",
	}
	_, err := New(&config)
	assert.Equal(t, ErrInvalidCacheDriver, err)
}
