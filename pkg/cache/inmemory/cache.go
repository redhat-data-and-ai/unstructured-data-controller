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

package inmemory

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"time"

	gocache "github.com/patrickmn/go-cache"
)

// InMemoryCache holds the handler for the in-memory cache using go-cache.
type InMemoryCache struct {
	client *gocache.Cache
}

// Config is the configuration for the in-memory cache.
type Config struct {
	DefaultExpiration int32 `yaml:"defaultExpiration" json:"defaultExpiration"`
	CleanupInterval   int32 `yaml:"cleanupInterval" json:"cleanupInterval"`
}

// NewCache creates a new in-memory cache instance.
func NewCache(config *Config) (*InMemoryCache, error) {
	if config == nil {
		config = defaultConfig()
	}

	defaultExpiration := time.Duration(config.DefaultExpiration) * time.Second
	cleanupExpiration := time.Duration(config.CleanupInterval) * time.Second

	client := gocache.New(defaultExpiration, cleanupExpiration)

	return &InMemoryCache{
		client: client,
	}, nil
}

// Set stores a value with the given key and TTL.
func (imc *InMemoryCache) Set(_ context.Context, key string, value any, ttl time.Duration) error {
	imc.client.Set(key, value, ttl)
	return nil
}

// Get retrieves the value for the given key.
func (imc *InMemoryCache) Get(_ context.Context, key string) (any, error) {
	val, found := imc.client.Get(key)
	if !found {
		return "", errors.New("key not found")
	}
	return val, nil
}

// GetByPattern returns all values whose keys match the given glob pattern.
func (imc *InMemoryCache) GetByPattern(ctx context.Context, keyPattern string) (map[string]any, error) {
	keys, err := imc.ScanKeys(ctx, keyPattern)
	if err != nil {
		return nil, fmt.Errorf("error scanning keys: %w", err)
	}

	values := make(map[string]any)
	for _, key := range keys {
		val, found := imc.client.Get(key)
		if found {
			values[key] = val
		}
	}
	return values, nil
}

// Delete removes the value for the given key.
func (imc *InMemoryCache) Delete(_ context.Context, key string) error {
	_, found := imc.client.Get(key)
	if found {
		imc.client.Delete(key)
	}
	return nil
}

// ScanKeys returns all keys matching the given glob pattern.
// Pattern uses * to match any sequence of characters and ? to match any single character.
func (imc *InMemoryCache) ScanKeys(_ context.Context, pattern string) ([]string, error) {
	items := imc.client.Items()
	var keys []string

	regexPattern := globToRegex(pattern)
	regex, err := regexp.Compile(regexPattern)
	if err != nil {
		return nil, fmt.Errorf("invalid pattern %s: %w", pattern, err)
	}

	for key := range items {
		if regex.MatchString(key) {
			keys = append(keys, key)
		}
	}

	return keys, nil
}

// globToRegex converts a glob pattern to a regex pattern.
func globToRegex(glob string) string {
	var result string
	for i := 0; i < len(glob); i++ {
		c := glob[i]
		switch c {
		case '*':
			result += ".*"
		case '?':
			result += "."
		case '.', '+', '(', ')', '|', '[', ']', '{', '}', '^', '$', '\\':
			result += "\\" + string(c)
		default:
			result += string(c)
		}
	}
	return "^" + result + "$"
}

// Flush removes all items from the cache.
func (imc *InMemoryCache) Flush(_ context.Context) {
	imc.client.Flush()
}

// ItemCount returns the number of items in the cache.
func (imc *InMemoryCache) ItemCount() int {
	return imc.client.ItemCount()
}

func defaultConfig() *Config {
	return &Config{
		DefaultExpiration: -1,
		CleanupInterval:   -1,
	}
}
