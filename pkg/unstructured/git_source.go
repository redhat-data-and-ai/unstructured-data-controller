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
	"errors"
	"fmt"
	"path"
	"strings"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gitclient"
)

type GitSource struct {
	GitClient     *gitclient.Client
	FilePatterns  []string
	Paths         []string
	IgnoreFolders []string
	OutputDir     string
	HeadHash      string
}

func (g *GitSource) SyncFilesToFilestore(ctx context.Context, fs *filestore.FileStore) ([]RawFileMetadata, error) {
	logger := log.FromContext(ctx)
	logger.Info("cloning git repo and walking files",
		"patterns", g.FilePatterns, "paths", g.Paths)

	entries, err := g.GitClient.CloneAndWalk(ctx, g.FilePatterns, g.Paths, g.IgnoreFolders)
	if err != nil {
		return nil, fmt.Errorf("git clone and walk failed: %w", err)
	}

	logger.Info("discovered files from git repo", "count", len(entries))

	var storedFiles []RawFileMetadata
	errorList := map[string]error{}
	sourceFileMap := map[string]bool{}

	for _, entry := range entries {
		if int64(len(entry.Content)) > maxFileSize {
			logger.Info("WARNING: skipping file exceeding max file size limit",
				"path", entry.Path, "sizeMB", len(entry.Content)/(1<<20))
			continue
		}

		filePath := path.Join(g.OutputDir, entry.Path)
		sourceFileMap[filePath] = true

		file := RawFileMetadata{
			FilePath: filePath,
			UID:      entry.Hash,
		}

		stored, err := g.storeFile(ctx, fs, &file, entry.Content)
		if err != nil {
			logger.Error(err, "failed to store git file", "path", entry.Path)
			errorList[entry.Path] = err
			continue
		}
		if stored {
			logger.Info("stored git file", "path", entry.Path)
			storedFiles = append(storedFiles, file)
		}
	}

	localFiles, err := fs.ListFilesInPath(ctx, g.OutputDir)
	if err != nil {
		logger.Error(err, "failed to list files in filestore for gc", "outputDir", g.OutputDir)
	} else {
		for _, localFilePath := range localFiles {
			rawFilePath := localFilePath
			if trimmed, ok := strings.CutSuffix(localFilePath, ".json"); ok {
				rawFilePath = trimmed
			}
			if _, exists := sourceFileMap[rawFilePath]; !exists {
				logger.Info("file no longer in git repo, deleting from filestore", "file", localFilePath)
				if err := fs.Delete(ctx, localFilePath); err != nil {
					logger.Error(err, "failed to delete file from filestore", "file", localFilePath)
					errorList[localFilePath] = err
				}
			}
		}
	}

	errorMessage := ""
	for filePath, err := range errorList {
		errorMessage += fmt.Sprintf("file: %s, error: %v\n", filePath, err)
	}
	if len(errorMessage) > 0 {
		return storedFiles, errors.New(errorMessage)
	}

	return storedFiles, nil
}

func (*GitSource) storeFile(
	ctx context.Context, fs *filestore.FileStore, file *RawFileMetadata, content []byte,
) (bool, error) {
	logger := log.FromContext(ctx)
	filePath := file.FilePath
	metadataPath := MetadataPath(filePath)

	fileExists, err := fs.Exists(ctx, filePath)
	if err != nil {
		return false, err
	}

	metadataExists, err := fs.Exists(ctx, metadataPath)
	if err != nil {
		return false, err
	}

	if fileExists && metadataExists {
		metadata, err := fs.Retrieve(ctx, metadataPath)
		if err != nil {
			return false, err
		}
		var existingFile RawFileMetadata
		if err := json.Unmarshal(metadata, &existingFile); err != nil {
			return false, err
		}
		if existingFile.UID == file.UID {
			logger.V(1).Info("git file unchanged, skipping", "file", filePath)
			return false, nil
		}
	}

	if err := fs.Store(ctx, filePath, content); err != nil {
		return false, fmt.Errorf("failed to store file %s: %w", filePath, err)
	}

	metadataData, err := json.Marshal(file)
	if err != nil {
		return false, fmt.Errorf("failed to marshal metadata for %s: %w", filePath, err)
	}
	if err := fs.Store(ctx, metadataPath, metadataData); err != nil {
		return false, fmt.Errorf("failed to store metadata for %s: %w", filePath, err)
	}

	return true, nil
}
