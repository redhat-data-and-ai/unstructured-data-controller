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

package gdrive

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"golang.org/x/sync/errgroup"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/cache"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/cache/inmemory"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/config"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/google"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/ldap"
)

// OrchestratorConfig holds the configuration for the orchestrator.
type OrchestratorConfig struct {
	CredentialsFile   string
	FileIDsPath       string
	FolderIDsPath     string
	OutputDir         string
	FailedFileIDsName string
	Config            *config.Config
}

// Orchestrator coordinates the folder crawling and
// permission resolution pipeline.
type Orchestrator struct {
	config OrchestratorConfig
}

// NewOrchestrator creates a new Orchestrator instance.
func NewOrchestrator(cfg OrchestratorConfig) *Orchestrator {
	return &Orchestrator{config: cfg}
}

// Run executes the crawling process.
func (o *Orchestrator) Run(ctx context.Context) error {
	logger := log.FromContext(ctx)

	if _, err := os.Stat(o.config.CredentialsFile); os.IsNotExist(err) {
		return fmt.Errorf(
			"credentials file not found: %s", o.config.CredentialsFile)
	}

	if err := os.MkdirAll(o.config.OutputDir, 0755); err != nil {
		return fmt.Errorf(
			"failed to create output directory: %w", err)
	}

	// Phase 1: Folder crawling
	if o.config.FolderIDsPath != "" {
		folderIDsStr, err := os.ReadFile(o.config.FolderIDsPath)
		if err != nil {
			return fmt.Errorf("failed to read folder IDs: %w", err)
		}

		var folderIDs FolderIDs
		if err := json.Unmarshal(folderIDsStr, &folderIDs); err != nil {
			return fmt.Errorf(
				"failed to unmarshal folder IDs: %w", err)
		}

		logger.Info("loaded folder IDs to crawl",
			"folderIDs", len(folderIDs.FolderIDs),
		)
		if err := o.processFolderIDs(ctx, folderIDs.FolderIDs); err != nil {
			logger.Error(err, "failed to process folder IDs")
			return fmt.Errorf(
				"failed to process folder IDs: %w", err)
		}
	}

	// Phase 2: File ID permission processing
	if o.config.FileIDsPath != "" {
		fileIDsStr, err := os.ReadFile(o.config.FileIDsPath)
		if err != nil {
			return fmt.Errorf("failed to read file IDs: %w", err)
		}

		var fileIDs FileIDs
		if err := json.Unmarshal(fileIDsStr, &fileIDs); err != nil {
			return fmt.Errorf(
				"failed to unmarshal file IDs: %w", err)
		}

		logger.Info("loaded file IDs to process",
			"fileIDs", len(fileIDs.FileIDs),
		)
		if err := o.processFileIDs(ctx, fileIDs.FileIDs); err != nil {
			logger.Error(err, "failed to process file IDs")
			return fmt.Errorf(
				"failed to process file IDs: %w", err)
		}
	}

	logger.Info("crawling completed successfully")
	return nil
}

// processFileIDs processes a list of file IDs for permission resolution.
func (o *Orchestrator) processFileIDs(
	ctx context.Context,
	fileIDs []string,
) error {
	ldapClient, err := ldap.InitLDAP(o.config.Config.LDAP)
	if err != nil {
		return fmt.Errorf(
			"failed to initialize LDAP client: %w", err)
	}

	cacheClient, err := cache.New(&cache.Config{
		Driver: "memory",
		InMemory: &inmemory.Config{
			DefaultExpiration: -1,
			CleanupInterval:   -1,
		},
	})
	if err != nil {
		return fmt.Errorf(
			"failed to create cache client: %w", err)
	}

	googleClient, err := google.NewClient(
		ctx, o.config.CredentialsFile)
	if err != nil {
		return fmt.Errorf(
			"failed to create Google client: %w", err)
	}

	client, err := NewClient(
		googleClient, ldapClient, cacheClient)
	if err != nil {
		return fmt.Errorf(
			"failed to create Drive client: %w", err)
	}

	return o.processFileIDsWithClient(ctx, fileIDs, client)
}

// processFolderIDs crawls folders for metadata, then feeds discovered
// file IDs into permission resolution.
func (o *Orchestrator) processFolderIDs(
	ctx context.Context,
	folderIDs []string,
) error {
	logger := log.FromContext(ctx)
	totalStartTime := time.Now()

	googleClient, err := google.NewClient(
		ctx, o.config.CredentialsFile)
	if err != nil {
		return fmt.Errorf(
			"failed to create Google client: %w", err)
	}

	ldapClient, err := ldap.InitLDAP(o.config.Config.LDAP)
	if err != nil {
		return fmt.Errorf(
			"failed to initialize LDAP client: %w", err)
	}

	cacheClient, err := cache.New(&cache.Config{
		Driver: "memory",
		InMemory: &inmemory.Config{
			DefaultExpiration: -1,
			CleanupInterval:   -1,
		},
	})
	if err != nil {
		return fmt.Errorf(
			"failed to create cache client: %w", err)
	}

	client, err := NewClient(
		googleClient, ldapClient, cacheClient)
	if err != nil {
		return fmt.Errorf(
			"failed to create Drive client: %w", err)
	}

	crawlConfig := o.config.Config.Crawl
	if crawlConfig.MaxRetries == 0 {
		crawlConfig.MaxRetries = 3
	}
	if crawlConfig.ConcurrentFolders == 0 {
		crawlConfig.ConcurrentFolders = 5
	}

	// --- Phase 1: Crawl all root folders concurrently ---
	logger.Info("starting folder crawl phase",
		"folderCount", len(folderIDs),
		"concurrentFolders", crawlConfig.ConcurrentFolders,
		"skipFolderNames", crawlConfig.SkipFolderNames,
	)

	type folderResult struct {
		result *CrawlResult
		err    error
	}

	results := make([]folderResult, len(folderIDs))
	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(crawlConfig.ConcurrentFolders)

	for i, folderID := range folderIDs {
		g.Go(func() error {
			start := time.Now()
			crawlRes, crawlErr := client.CrawlFolder(
				ctx,
				folderID,
				crawlConfig.SkipFolderNames,
				crawlConfig.MaxRetries,
			)
			results[i] = folderResult{
				result: crawlRes, err: crawlErr,
			}
			logger.Info("folder crawl completed",
				"folderID", folderID,
				"timeElapsed", time.Since(start),
			)
			return nil
		})
	}
	_ = g.Wait()

	// Merge results from all folders
	allRecords := make([]CrawlRecord, 0)
	mergedInaccessible := InaccessibleItems{
		Folders:               make([]InaccessibleFolder, 0),
		Files:                 make([]InaccessibleFile, 0),
		ShortcutTargetFolders: make([]InaccessibleShortcutFolder, 0),
		ShortcutTargetFiles:   make([]InaccessibleShortcutFile, 0),
	}

	for i, r := range results {
		if r.err != nil {
			logger.Error(r.err, "folder crawl failed",
				"folderID", folderIDs[i],
			)
			mergedInaccessible.Folders = append(
				mergedInaccessible.Folders,
				InaccessibleFolder{
					FolderID:     folderIDs[i],
					FolderName:   "",
					RootFolderID: folderIDs[i],
				},
			)
			allRecords = append(allRecords, CrawlRecord{
				FileID:       folderIDs[i],
				MimeType:     folderMimeType,
				RootFolderID: folderIDs[i],
				Status:       "error",
				Reason: fmt.Sprintf(
					"root folder inaccessible: %v", r.err),
			})
			continue
		}
		allRecords = append(allRecords, r.result.Records...)
		mergedInaccessible.Folders = append(
			mergedInaccessible.Folders,
			r.result.InaccessibleItems.Folders...)
		mergedInaccessible.Files = append(
			mergedInaccessible.Files,
			r.result.InaccessibleItems.Files...)
		mergedInaccessible.ShortcutTargetFolders = append(
			mergedInaccessible.ShortcutTargetFolders,
			r.result.InaccessibleItems.ShortcutTargetFolders...)
		mergedInaccessible.ShortcutTargetFiles = append(
			mergedInaccessible.ShortcutTargetFiles,
			r.result.InaccessibleItems.ShortcutTargetFiles...)
	}

	inaccessibleCount := len(mergedInaccessible.Folders) +
		len(mergedInaccessible.Files) +
		len(mergedInaccessible.ShortcutTargetFolders) +
		len(mergedInaccessible.ShortcutTargetFiles)
	logger.Info("folder crawl phase completed",
		"totalRecords", len(allRecords),
		"inaccessible", inaccessibleCount,
		"crawlTimeElapsed", time.Since(totalStartTime),
	)

	// Write crawl_metadata.json
	crawlJSON, err := json.MarshalIndent(allRecords, "", "  ")
	if err != nil {
		return fmt.Errorf(
			"failed to marshal crawl metadata: %w", err)
	}
	crawlFile := fmt.Sprintf(
		"%s/crawl_metadata.json", o.config.OutputDir)
	if err := os.WriteFile(crawlFile, crawlJSON, 0644); err != nil {
		return fmt.Errorf(
			"failed to write crawl metadata: %w", err)
	}
	logger.Info("crawl metadata written", "file", crawlFile)

	// Write inaccessible_items.json
	inaccessibleJSON, err := json.MarshalIndent(
		mergedInaccessible, "", "  ")
	if err != nil {
		return fmt.Errorf(
			"failed to marshal inaccessible items: %w", err)
	}
	inaccessibleFile := fmt.Sprintf(
		"%s/inaccessible_items.json", o.config.OutputDir)
	if err := os.WriteFile(
		inaccessibleFile, inaccessibleJSON, 0644); err != nil {
		return fmt.Errorf(
			"failed to write inaccessible items: %w", err)
	}
	logger.Info("inaccessible items written", "file", inaccessibleFile)

	// --- Phase 2: Permission resolution for discovered files ---
	var discoveredFileIDs []string
	seen := make(map[string]bool)
	for _, record := range allRecords {
		if record.Status != "successful" {
			continue
		}
		if record.MimeType == folderMimeType {
			continue
		}
		if !seen[record.FileID] {
			seen[record.FileID] = true
			discoveredFileIDs = append(
				discoveredFileIDs, record.FileID)
		}
	}

	logger.Info("starting permission resolution for discovered files",
		"discoveredFiles", len(discoveredFileIDs),
	)

	if len(discoveredFileIDs) > 0 {
		if err := o.processFileIDsWithClient(
			ctx, discoveredFileIDs, client); err != nil {
			logger.Error(err, "failed to process permissions")
			return fmt.Errorf(
				"failed to process permissions: %w", err)
		}
	}

	logger.Info("folder crawl + permission pipeline completed",
		"totalTimeElapsed", time.Since(totalStartTime),
	)
	return nil
}

// processFileIDsWithClient processes file IDs using a pre-initialized client.
func (o *Orchestrator) processFileIDsWithClient(
	ctx context.Context,
	fileIDs []string,
	client *Client,
) error {
	logger := log.FromContext(ctx)
	totalStartTime := time.Now()

	var mu sync.Mutex
	var failedArr, warnedArr []string
	var allErrors []error

	g, _ := errgroup.WithContext(ctx)
	g.SetLimit(10)

	for _, fileID := range fileIDs {
		g.Go(func() error {
			start := time.Now()
			permissions, warnings, err := client.GetFilePermissions(
				ctx, fileID, 3)
			logger.Info("fetched file permissions",
				"fileID", fileID,
				"timeElapsed", time.Since(start),
			)

			if err != nil {
				logger.Error(err, "failed to get file permissions")
				mu.Lock()
				allErrors = append(allErrors, fmt.Errorf(
					"failed to get permissions for %s: %w",
					fileID, err))
				failedArr = append(failedArr, fileID)
				mu.Unlock()
				return nil
			}

			if len(warnings) > 0 {
				logger.Info(
					"file permissions processed with warnings",
					"fileID", fileID,
					"warnings", warnings,
				)
				mu.Lock()
				warnedArr = append(warnedArr, fileID)
				mu.Unlock()
			}

			outputJSON, err := json.MarshalIndent(
				permissions, "", "  ")
			if err != nil {
				mu.Lock()
				allErrors = append(allErrors, fmt.Errorf(
					"failed to marshal permissions: %w", err))
				mu.Unlock()
				return nil
			}

			outPath := fmt.Sprintf(
				"%s/permissions_%s.json",
				o.config.OutputDir, fileID)
			if err := os.WriteFile(
				outPath, outputJSON, 0644); err != nil {
				mu.Lock()
				allErrors = append(allErrors, fmt.Errorf(
					"failed to write permissions file: %w", err))
				mu.Unlock()
				return nil
			}

			logger.Info("permissions saved to file",
				"fileID", fileID,
				"timeElapsed", time.Since(start),
			)
			return nil
		})
	}

	_ = g.Wait()

	// Write failed file IDs
	failedPath := fmt.Sprintf(
		"%s/%s", o.config.OutputDir, o.config.FailedFileIDsName)
	if err := writeJSONFile(failedPath, failedArr); err != nil {
		return fmt.Errorf(
			"failed to write failed file IDs: %w", err)
	}

	// Write warned file IDs
	warnPath := fmt.Sprintf(
		"%s/warnings.json", o.config.OutputDir)
	if err := writeJSONFile(warnPath, warnedArr); err != nil {
		return fmt.Errorf(
			"failed to write warnings file: %w", err)
	}
	logger.Info("warnings written", "warnedFiles", len(warnedArr))

	logger.Info("permission resolution completed",
		"timeElapsed", time.Since(totalStartTime),
	)

	if len(allErrors) > 0 {
		logger.Error(nil, "some permission resolutions failed",
			"errors", len(allErrors),
		)
		return fmt.Errorf(
			"multiple errors occurred: %v", allErrors)
	}

	return nil
}

// writeJSONFile marshals data to JSON and writes it to the given path.
func writeJSONFile(path string, data any) error {
	jsonBytes, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return fmt.Errorf(
			"failed to marshal JSON for %s: %w", path, err)
	}
	return os.WriteFile(path, jsonBytes, 0644)
}
