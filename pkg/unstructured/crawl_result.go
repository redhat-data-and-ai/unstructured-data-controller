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

package unstructured

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
)

const (
	CrawlStatusSuccessful = "successful"
	CrawlStatusSkipped    = "skipped"
	CrawlStatusError      = "error"
	CrawlMetadataDir      = "crawl_metadata"
	extensionNone         = "(none)"
)

// CrawlResult is one JSON object per discovered file under stages/<crawl>/catalog/.
// The same catalog/<fileId>.json path is upserted on each reconcile.
type CrawlResult struct {
	FileID     string `json:"file_id"`
	FileName   string `json:"file_name"`
	SourcePath string `json:"source_path,omitempty"`
	FileURL    string `json:"file_url,omitempty"`
	MediaType  string `json:"media_type,omitempty"`
	Extension  string `json:"extension"` // without leading dot, e.g. "pdf"
	Status     string `json:"status"`
	Reason     string `json:"reason,omitempty"`
	SourceType string `json:"source_type"`
	CrawledAt  string `json:"crawled_at"`
}

func CrawlCatalogPath(outputDir, fileID string) string {
	return path.Join(outputDir, CrawlMetadataDir, fileID+".json")
}

// GDriveFileURL returns the standard Google Drive web link for a file ID.
func GDriveFileURL(fileID string) string {
	return fmt.Sprintf("https://drive.google.com/file/d/%s/view", fileID)
}

// catalogExtension returns an extension without a leading dot (e.g. "pdf").
func catalogExtension(ext string) string {
	ext = strings.ToLower(strings.TrimSpace(ext))
	if ext == "" || ext == extensionNone {
		return extensionNone
	}
	return strings.TrimPrefix(ext, ".")
}

func storeCrawlResult(ctx context.Context, fs *filestore.FileStore, outputDir string, r CrawlResult) error {
	if r.CrawledAt == "" {
		r.CrawledAt = time.Now().UTC().Format(time.RFC3339)
	}
	if r.Extension == "" {
		r.Extension = catalogExtension(FileExtension(r.FileName))
	} else {
		r.Extension = catalogExtension(r.Extension)
	}
	data, err := json.Marshal(r)
	if err != nil {
		return err
	}
	return fs.Store(ctx, CrawlCatalogPath(outputDir, r.FileID), data)
}
