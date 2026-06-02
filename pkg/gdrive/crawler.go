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
	"fmt"
	"sync"

	"google.golang.org/api/drive/v3"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	folderMimeType   = "application/vnd.google-apps.folder"
	shortcutMimeType = "application/vnd.google-apps.shortcut"
)

// CrawlResult holds the output of a folder crawl.
type CrawlResult struct {
	mu                sync.Mutex
	Records           []CrawlRecord
	InaccessibleItems InaccessibleItems
}

// crawlState bundles the shared parameters threaded through
// the recursive crawl so that individual methods stay under
// the argument-count limit.
type crawlState struct {
	ctx            context.Context
	rootFolderID   string
	visitedFolders *sync.Map
	skipSet        map[string]bool
	maxRetries     int
	result         *CrawlResult
}

// CrawlFolder recursively crawls a root folder and returns all
// discovered file records and any inaccessible items.
func (c *Client) CrawlFolder(
	ctx context.Context,
	rootFolderID string,
	skipFolderNames []string,
	maxRetries int,
) (*CrawlResult, error) {
	logger := log.FromContext(ctx).WithValues(
		"rootFolderID", rootFolderID,
	)

	rootFolder, err := c.googleClient.GetFileMetadata(
		ctx, rootFolderID, maxRetries)
	if err != nil {
		logger.Error(err, "failed to get root folder metadata")
		return nil, fmt.Errorf(
			"failed to get root folder metadata for %s: %w",
			rootFolderID, err)
	}

	logger.Info("starting folder crawl",
		"rootFolderName", rootFolder.Name,
	)

	skipSet := make(map[string]bool, len(skipFolderNames))
	for _, name := range skipFolderNames {
		skipSet[name] = true
	}

	s := &crawlState{
		ctx:            ctx,
		rootFolderID:   rootFolderID,
		visitedFolders: &sync.Map{},
		skipSet:        skipSet,
		maxRetries:     maxRetries,
		result:         &CrawlResult{},
	}

	c.crawlCollectEntries(s,
		rootFolderID, rootFolder.Name, nil)

	inaccessibleCount := len(s.result.InaccessibleItems.Folders) +
		len(s.result.InaccessibleItems.Files) +
		len(s.result.InaccessibleItems.ShortcutTargetFolders) +
		len(s.result.InaccessibleItems.ShortcutTargetFiles)
	logger.Info("folder crawl completed",
		"recordCount", len(s.result.Records),
		"inaccessibleCount", inaccessibleCount,
	)

	return s.result, nil
}

// crawlCollectEntries recursively traverses a folder and
// collects file records.
func (c *Client) crawlCollectEntries(
	s *crawlState,
	folderID string,
	folderName string,
	fromShortcutID *string,
) {
	logger := log.FromContext(s.ctx).WithValues(
		"folderID", folderID,
		"folderName", folderName,
		"rootFolderID", s.rootFolderID,
	)

	// Cycle detection
	if _, visited := s.visitedFolders.LoadOrStore(folderID, true); visited {
		logger.Info("skipping already visited folder")
		return
	}

	if s.skipSet[folderName] {
		logger.Info("skipping folder (in skip list)")
		s.result.appendRecord(CrawlRecord{
			FileID:       folderID,
			FileName:     folderName,
			MimeType:     folderMimeType,
			RootFolderID: s.rootFolderID,
			Status:       "skipped",
			Reason:       "folder in skip list",
		})
		return
	}

	logger.Info("crawling folder")

	items, err := c.googleClient.ListFolderContents(
		s.ctx, folderID, s.maxRetries)
	if err != nil {
		logger.Error(err, "failed to list folder contents")
		if fromShortcutID != nil {
			s.result.appendInaccessibleShortcutFolder(
				InaccessibleShortcutFolder{
					ShortcutFileID: *fromShortcutID,
					TargetFolderID: folderID,
					RootFolderID:   s.rootFolderID,
				})
		} else {
			s.result.appendInaccessibleFolder(InaccessibleFolder{
				FolderID:     folderID,
				FolderName:   folderName,
				RootFolderID: s.rootFolderID,
			})
		}
		s.result.appendRecord(CrawlRecord{
			FileID:       folderID,
			FileName:     folderName,
			MimeType:     folderMimeType,
			RootFolderID: s.rootFolderID,
			Status:       "error",
			Reason:       "folder not accessible by service account",
		})
		return
	}

	logger.Info("listed folder contents", "itemCount", len(items))

	if len(items) == 0 {
		if c.googleClient.IsFolderAccessible(s.ctx, folderID) {
			logger.Info("folder is empty but accessible")
			s.result.appendRecord(CrawlRecord{
				FileID:       folderID,
				FileName:     folderName,
				MimeType:     folderMimeType,
				RootFolderID: s.rootFolderID,
				Status:       "skipped",
				Reason:       "empty folder",
			})
		} else {
			logger.Info("folder returned 0 items and is not accessible")
			if fromShortcutID != nil {
				s.result.appendInaccessibleShortcutFolder(
					InaccessibleShortcutFolder{
						ShortcutFileID: *fromShortcutID,
						TargetFolderID: folderID,
						RootFolderID:   s.rootFolderID,
					})
			} else {
				s.result.appendInaccessibleFolder(InaccessibleFolder{
					FolderID:     folderID,
					FolderName:   folderName,
					RootFolderID: s.rootFolderID,
				})
			}
			s.result.appendRecord(CrawlRecord{
				FileID:       folderID,
				FileName:     folderName,
				MimeType:     folderMimeType,
				RootFolderID: s.rootFolderID,
				Status:       "error",
				Reason:       "inaccessible",
			})
		}
		return
	}

	for _, item := range items {
		switch item.MimeType {
		case folderMimeType:
			c.handleSubfolder(s, item,
				folderID, folderName)
		case shortcutMimeType:
			c.handleShortcut(s, item,
				folderID, folderName)
		default:
			record := buildCrawlRecord(
				item, folderID, folderName, s.rootFolderID)
			s.result.appendRecord(record)
			logger.V(1).Info("recorded file",
				"fileID", item.Id,
				"fileName", item.Name,
			)
		}
	}
}

// handleSubfolder processes a subfolder item during crawl.
func (c *Client) handleSubfolder(
	s *crawlState,
	item *drive.File,
	parentFolderID string,
	parentFolderName string,
) {
	logger := log.FromContext(s.ctx)

	if s.skipSet[item.Name] {
		logger.Info("skipping subfolder (in skip list)",
			"folderName", item.Name,
		)
		s.result.appendRecord(CrawlRecord{
			FileID:           item.Id,
			FileName:         item.Name,
			MimeType:         folderMimeType,
			RootFolderID:     s.rootFolderID,
			ParentFolderID:   parentFolderID,
			ParentFolderName: parentFolderName,
			Status:           "skipped",
			Reason:           "folder in skip list",
		})
		return
	}

	logger.Info("entering subfolder", "folderName", item.Name)
	c.crawlCollectEntries(s, item.Id, item.Name, nil)
}

// handleShortcut processes a shortcut item during crawl.
func (c *Client) handleShortcut(
	s *crawlState,
	item *drive.File,
	parentFolderID string,
	parentFolderName string,
) {
	logger := log.FromContext(s.ctx).WithValues(
		"shortcutID", item.Id,
		"shortcutName", item.Name,
	)

	if item.ShortcutDetails == nil {
		logger.Info("shortcut has no details, skipping")
		s.result.appendRecord(CrawlRecord{
			FileID:           item.Id,
			FileName:         item.Name,
			MimeType:         shortcutMimeType,
			RootFolderID:     s.rootFolderID,
			ParentFolderID:   parentFolderID,
			ParentFolderName: parentFolderName,
			Status:           "skipped",
			Reason:           "shortcut has no target",
		})
		return
	}

	targetID := item.ShortcutDetails.TargetId
	targetMimeType := item.ShortcutDetails.TargetMimeType

	logger.Info("processing shortcut",
		"targetID", targetID,
		"targetMimeType", targetMimeType,
	)

	if targetMimeType == folderMimeType {
		targetMeta, err := c.googleClient.GetFileMetadata(
			s.ctx, targetID, s.maxRetries)
		if err != nil {
			logger.Error(err, "cannot access shortcut target folder")
			s.result.appendInaccessibleShortcutFolder(
				InaccessibleShortcutFolder{
					ShortcutFileID: item.Id,
					TargetFolderID: targetID,
					RootFolderID:   s.rootFolderID,
				})
			s.result.appendRecord(CrawlRecord{
				FileID:           targetID,
				FileName:         item.Name,
				MimeType:         folderMimeType,
				RootFolderID:     s.rootFolderID,
				ParentFolderID:   parentFolderID,
				ParentFolderName: parentFolderName,
				Status:           "error",
				Reason:           "shortcut target folder inaccessible",
			})
			return
		}

		targetName := targetMeta.Name
		if s.skipSet[targetName] {
			logger.Info(
				"skipping shortcut target folder (in skip list)",
				"targetName", targetName,
			)
			s.result.appendRecord(CrawlRecord{
				FileID:           targetID,
				FileName:         targetName,
				MimeType:         folderMimeType,
				RootFolderID:     s.rootFolderID,
				ParentFolderID:   parentFolderID,
				ParentFolderName: parentFolderName,
				Status:           "skipped",
				Reason:           "shortcut target folder in skip list",
			})
			return
		}

		logger.Info("following shortcut to folder",
			"targetName", targetName,
		)
		shortcutID := item.Id
		c.crawlCollectEntries(s,
			targetID, targetName, &shortcutID)
	} else {
		targetMeta, err := c.googleClient.GetFileMetadata(
			s.ctx, targetID, s.maxRetries)
		if err != nil {
			logger.Error(err, "cannot access shortcut target file")
			s.result.appendInaccessibleShortcutFile(
				InaccessibleShortcutFile{
					ShortcutFileID: item.Id,
					TargetFileID:   targetID,
					RootFolderID:   s.rootFolderID,
				})
			s.result.appendRecord(CrawlRecord{
				FileID:           targetID,
				FileName:         item.Name,
				MimeType:         targetMimeType,
				RootFolderID:     s.rootFolderID,
				ParentFolderID:   parentFolderID,
				ParentFolderName: parentFolderName,
				Status:           "error",
				Reason:           "shortcut target file inaccessible",
			})
			return
		}

		record := buildCrawlRecord(
			targetMeta, parentFolderID,
			parentFolderName, s.rootFolderID)
		s.result.appendRecord(record)
		logger.Info("recorded shortcut target file",
			"targetName", targetMeta.Name,
		)
	}
}

// buildCrawlRecord creates a CrawlRecord from a Drive file.
func buildCrawlRecord(
	file *drive.File,
	parentFolderID, parentFolderName, rootFolderID string,
) CrawlRecord {
	owner := ""
	if len(file.Owners) > 0 {
		owner = file.Owners[0].EmailAddress
	}
	return CrawlRecord{
		FileID:           file.Id,
		FileName:         file.Name,
		MimeType:         file.MimeType,
		CreatedAt:        file.CreatedTime,
		UpdatedAt:        file.ModifiedTime,
		Owner:            owner,
		FileSize:         file.Size,
		RootFolderID:     rootFolderID,
		ParentFolderID:   parentFolderID,
		ParentFolderName: parentFolderName,
		Status:           "successful",
	}
}

// Thread-safe append methods for CrawlResult

func (r *CrawlResult) appendRecord(record CrawlRecord) {
	r.mu.Lock()
	r.Records = append(r.Records, record)
	r.mu.Unlock()
}

func (r *CrawlResult) appendInaccessibleFolder(
	item InaccessibleFolder,
) {
	r.mu.Lock()
	r.InaccessibleItems.Folders = append(
		r.InaccessibleItems.Folders, item)
	r.mu.Unlock()
}

func (r *CrawlResult) appendInaccessibleShortcutFolder(
	item InaccessibleShortcutFolder,
) {
	r.mu.Lock()
	r.InaccessibleItems.ShortcutTargetFolders = append(
		r.InaccessibleItems.ShortcutTargetFolders, item)
	r.mu.Unlock()
}

func (r *CrawlResult) appendInaccessibleShortcutFile(
	item InaccessibleShortcutFile,
) {
	r.mu.Lock()
	r.InaccessibleItems.ShortcutTargetFiles = append(
		r.InaccessibleItems.ShortcutTargetFiles, item)
	r.mu.Unlock()
}
