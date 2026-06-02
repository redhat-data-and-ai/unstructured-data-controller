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
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewCacheInstance(t *testing.T) {
	config := &Config{
		DefaultExpiration: 15,
		CleanupInterval:   30,
	}

	mem, err := NewCache(config)
	assert.Nil(t, err)
	assert.NotNil(t, mem)
}

func TestNewCacheInstanceNilConfig(t *testing.T) {
	mem, err := NewCache(nil)
	assert.Nil(t, err)
	assert.NotNil(t, mem)
}

func TestInMemoryCache_Set(t *testing.T) {
	config := &Config{
		DefaultExpiration: 15,
		CleanupInterval:   30,
	}

	mem, err := NewCache(config)
	assert.Nil(t, err)
	assert.NotNil(t, mem)

	err = mem.Set(context.TODO(), "test-key", "test-set-val", time.Minute)
	assert.Nil(t, err)

	val, err := mem.Get(context.TODO(), "test-key")
	assert.Nil(t, err)
	assert.Equal(t, "test-set-val", val)
}

func TestInMemoryGetWithoutSet(t *testing.T) {
	config := &Config{
		DefaultExpiration: 15,
		CleanupInterval:   30,
	}

	mem, err := NewCache(config)
	assert.Nil(t, err)
	assert.NotNil(t, mem)

	val, err := mem.Get(context.Background(), "test-key")
	assert.NotNil(t, err)
	assert.Equal(t, "", val)
}

func TestInMemoryCache_Delete(t *testing.T) {
	config := &Config{
		DefaultExpiration: 15,
		CleanupInterval:   30,
	}

	mem, err := NewCache(config)
	assert.Nil(t, err)
	assert.NotNil(t, mem)

	err = mem.Set(context.TODO(), "test-key", "test-set-val", time.Minute)
	assert.Nil(t, err)

	val, err := mem.Get(context.TODO(), "test-key")
	assert.Nil(t, err)
	assert.Equal(t, "test-set-val", val)

	err = mem.Delete(context.Background(), "test-key")
	assert.Nil(t, err)

	val, err = mem.Get(context.Background(), "test-key")
	assert.NotNil(t, err)
	assert.Equal(t, "", val)
}

func TestInMemoryCacheGetByPattern(t *testing.T) {
	config := &Config{
		DefaultExpiration: 15,
		CleanupInterval:   30,
	}

	mem, err := NewCache(config)
	assert.Nil(t, err)
	assert.NotNil(t, mem)

	err = mem.Set(context.Background(), "user:1", "value1", time.Minute)
	assert.Nil(t, err)
	err = mem.Set(context.Background(), "user:2", "value2", time.Minute)
	assert.Nil(t, err)
	err = mem.Set(context.Background(), "user:3", "value3", time.Minute)
	assert.Nil(t, err)
	err = mem.Set(context.Background(), "other:1", "othervalue", time.Minute)
	assert.Nil(t, err)

	values, err := mem.GetByPattern(context.Background(), "user:*")
	assert.Nil(t, err)
	assert.Equal(t, 3, len(values))

	stringValues := make([]string, 0, len(values))
	for _, v := range values {
		stringValues = append(stringValues, v.(string))
	}

	assert.Contains(t, stringValues, "value1")
	assert.Contains(t, stringValues, "value2")
	assert.Contains(t, stringValues, "value3")
	assert.NotContains(t, stringValues, "othervalue")

	values, err = mem.GetByPattern(context.Background(), "nonexistent:*")
	assert.Nil(t, err)
	assert.Equal(t, 0, len(values))
}
