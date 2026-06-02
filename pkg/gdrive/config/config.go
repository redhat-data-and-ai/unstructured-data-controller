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

package config

import (
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/cache"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/ldap"
)

// Config represents the top-level configuration for the gdrive package.
type Config struct {
	LDAP  ldap.Config  `yaml:"ldap" json:"ldap"`
	Cache cache.Config `yaml:"cache" json:"cache"`
	Crawl Crawl        `yaml:"crawl" json:"crawl"`
}

// Crawl contains configuration for folder crawling.
type Crawl struct {
	// SkipFolderNames is a list of folder names to skip during crawling.
	SkipFolderNames []string `yaml:"skipFolderNames" json:"skipFolderNames"`
	// MaxRetries is the maximum number of retries for API calls.
	MaxRetries int `yaml:"maxRetries" json:"maxRetries"`
	// ConcurrentFolders is the maximum number of folders to crawl concurrently.
	ConcurrentFolders int `yaml:"concurrentFolders" json:"concurrentFolders"`
}

// DefaultCrawl returns a Crawl config with sensible defaults.
func DefaultCrawl() Crawl {
	return Crawl{
		SkipFolderNames:   []string{},
		MaxRetries:        3,
		ConcurrentFolders: 5,
	}
}

// DefaultConfig returns a Config with sensible defaults.
func DefaultConfig() Config {
	return Config{
		LDAP:  ldap.DefaultConfig(),
		Cache: cache.DefaultConfig(),
		Crawl: DefaultCrawl(),
	}
}
