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
	"context"
	"errors"
	"time"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/cache/inmemory"
)

var (
	// ErrInvalidCacheDriver is returned when an unsupported cache driver is provided.
	ErrInvalidCacheDriver = errors.New("invalid cache driver")
)

const (
	DriverMemory = "memory"

	NoExpiration = -1 * time.Second
)

// Cache defines a generic interface for cache operations.
type Cache interface {
	// Get returns the value for the given key.
	Get(ctx context.Context, key string) (any, error)

	// GetByPattern returns all values matching the given key pattern.
	GetByPattern(ctx context.Context, keyPattern string) (map[string]any, error)

	// Set stores a value with the given key and TTL.
	Set(ctx context.Context, key string, value any, ttl time.Duration) error

	// Delete removes the value for the given key.
	Delete(ctx context.Context, key string) error
}

// Config is the configuration for the cache client.
type Config struct {
	// Driver is the type of cache backend (e.g., "memory").
	Driver string `yaml:"driver" json:"driver"`

	// InMemory is the configuration for the in-memory cache backend.
	InMemory *inmemory.Config `yaml:"inmemory" json:"inmemory"`
}

// DefaultConfig returns a Config with sensible defaults for in-memory caching.
func DefaultConfig() Config {
	return Config{
		Driver: DriverMemory,
		InMemory: &inmemory.Config{
			DefaultExpiration: -1,
			CleanupInterval:   -1,
		},
	}
}

// New returns a new cache client based on the provided configuration.
func New(config *Config) (Cache, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}

	switch config.Driver {
	case DriverMemory:
		return inmemory.NewCache(config.InMemory)
	default:
		return nil, ErrInvalidCacheDriver
	}
}
