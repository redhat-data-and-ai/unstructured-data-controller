/*
Copyright 2025.

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
	"io"
	"path"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	"golang.org/x/sync/errgroup"
	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive"
)

type DataSource interface {
	// SyncFilesToFilestore will store all files from the source to the filestore and return the list of file paths
	SyncFilesToFilestore(ctx context.Context, fs *filestore.FileStore) ([]RawFileMetadata, error)
}

type S3BucketSource struct {
	S3Client  *s3.Client
	Bucket    string
	Prefix    string
	OutputDir string
}

func (s *S3BucketSource) SyncFilesToFilestore(ctx context.Context, fs *filestore.FileStore) ([]RawFileMetadata, error) {
	logger := log.FromContext(ctx)
	logger.Info("listing objects in prefix", "bucket", s.Bucket, "prefix", s.Prefix)
	objects, err := awsclienthandler.ListObjectsInPrefix(ctx, s.S3Client, s.Bucket, s.Prefix)
	if err != nil {
		return nil, err
	}

	storedFiles := []RawFileMetadata{}
	errorList := map[string]error{}
	sourceFileMap := map[string]bool{}

	for _, object := range objects {
		// skip S3 folder marker objects (keys ending with "/") — storing these
		// as regular files would block directory creation for real files underneath
		if strings.HasSuffix(*object.Key, "/") {
			continue
		}
		file := RawFileMetadata{
			FilePath: s.filestorePath(*object.Key),
			UID:      *object.ETag,
		}
		logger.Info("storing file", "file", file.FilePath)
		sourceFileMap[file.FilePath] = true

		stored, err := s.storeFile(ctx, fs, &file)
		if err != nil {
			logger.Error(err, "failed to store file", "file", file.FilePath)
			errorList[file.FilePath] = err
			continue
		}
		if stored {
			logger.Info("successfully stored file", "file", file.FilePath)
			storedFiles = append(storedFiles, file)
		}
	}
	// Listing all the file in the local s3 filestore
	localFiles, err := fs.ListFilesInPath(ctx, s.outputPrefix())
	if err != nil {
		logger.Error(err, "failed to list files in filestore", "prefix", s.Prefix)
		return nil, err
	}

	// logic to delete files and its respective files if the file is removed from upstream bucket
	for _, localFilePath := range localFiles {
		rawFilePath := localFilePath
		if trimmed, ok := strings.CutSuffix(localFilePath, ".json"); ok {
			rawFilePath = trimmed
		}

		if _, exists := sourceFileMap[rawFilePath]; !exists {
			logger.Info("file or its parent does not exist in the source, deleting from the filestore", "file", localFilePath)
			if err := fs.Delete(ctx, localFilePath); err != nil {
				logger.Error(err, "failed to delete file from filestore", "file", localFilePath)
				errorList[localFilePath] = err
			} else {
				logger.Info("successfully deleted file from the filestore", "file", localFilePath)
			}
		}
	}

	errorMessage := ""
	for filePath, err := range errorList {
		errorMessage += fmt.Sprintf("file: %s, error: %v\n", filePath, err)
	}
	if len(errorMessage) > 0 {
		return nil, errors.New(errorMessage)
	}

	return storedFiles, nil
}

// storeFile will store the given file to the filestore
// it will make sure that the file is unique by comparing the object's ETag with the file's metadata
func (s *S3BucketSource) storeFile(ctx context.Context, fs *filestore.FileStore, file *RawFileMetadata) (bool, error) {
	logger := log.FromContext(ctx)
	logger.Info("storing file", "file", file.FilePath)

	filePath := file.FilePath
	metadataPath := MetadataPath(filePath)

	// check if the file exists in the filestore

	// for a file to exist in the filestore, both, the file and the metadata file must exist
	fileExists, err := fs.Exists(ctx, filePath)
	if err != nil {
		logger.Error(err, "failed to check if file exists in filestore", "file", filePath)
		return false, err
	}

	metadataExists, err := fs.Exists(ctx, metadataPath)
	if err != nil {
		logger.Error(err, "failed to check if metadata file exists in filestore", "file", metadataPath)
		return false, err
	}

	if fileExists && metadataExists {
		logger.Info("file and metadata file exist in filestore, checking if they are the same", "file",
			filePath, "metadataFile", metadataPath)

		// then compare the metadata file's ETag with the object's ETag
		metadata, err := fs.Retrieve(ctx, metadataPath)
		if err != nil {
			logger.Error(err, "failed to retrieve metadata file from filestore", "file", metadataPath)
			return false, err
		}

		// unmarshal the metadata file into a FileMetadata struct
		var existingFile RawFileMetadata
		err = json.Unmarshal(metadata, &existingFile)
		if err != nil {
			logger.Error(err, "failed to unmarshal metadata file", "file", metadataPath)
			return false, err
		}

		if existingFile.UID == file.UID {
			// the file and the metadata file are the same, so we can skip storing it
			logger.Info("file and metadata file are the same, skipping ...", "file", filePath)
			return false, nil
		}
	}

	// we are here because the file or the metadata file does not exist
	// so we can safely store the file and the corresponding metadata file

	// store the file first — fetch from S3 using the original key
	s3Key := s.s3Key(filePath)
	objectOutput, err := awsclienthandler.GetObject(ctx, s.S3Client, s.Bucket, s3Key)
	if err != nil {
		logger.Error(err, "failed to get object from S3", "file", filePath)
		return false, err
	}

	data, err := io.ReadAll(objectOutput.Body)
	if err != nil {
		logger.Error(err, "failed to read object from S3", "file", filePath)
		return false, err
	}
	if err = fs.Store(ctx, filePath, data); err != nil {
		logger.Error(err, "failed to store file in filestore", "file", filePath)
		return false, err
	}

	metadataData, err := json.Marshal(file)
	if err != nil {
		logger.Error(err, "failed to marshal metadata file", "file", metadataPath)
		return false, err
	}
	if err = fs.Store(ctx, metadataPath, metadataData); err != nil {
		logger.Error(err, "failed to store metadata file in filestore", "file", metadataPath)
		return false, err
	}

	logger.Info("successfully stored file and metadata file in filestore", "file", filePath, "metadataFile", metadataPath)
	return true, nil
}

func (s *S3BucketSource) outputPrefix() string {
	if s.OutputDir != "" {
		return s.OutputDir
	}
	return s.Prefix
}

// filestorePath remaps an S3 key to the filestore output directory.
func (s *S3BucketSource) filestorePath(s3Key string) string {
	if s.OutputDir == "" {
		return s3Key
	}
	baseName := strings.TrimPrefix(s3Key, s.Prefix)
	return path.Join(s.OutputDir, baseName)
}

// s3Key derives the original S3 key from a filestore path.
func (s *S3BucketSource) s3Key(filestorePath string) string {
	if s.OutputDir == "" {
		return filestorePath
	}
	baseName := strings.TrimPrefix(filestorePath, s.OutputDir)
	return path.Join(s.Prefix, baseName)
}

// GDriveSource implements DataSource for Google Drive folders.
type GDriveSource struct {
	GDriveClient        *gdrive.Client
	FolderIDs           []string
	SkipFolderNames     []string
	MaxRetries          int
	ConcurrentFolders   int
	ConcurrentDownloads int
	OutputDir           string
}

func (g *GDriveSource) SyncFilesToFilestore(ctx context.Context, fs *filestore.FileStore) ([]RawFileMetadata, error) {
	logger := log.FromContext(ctx)

	// Phase 1: Crawl all root folders concurrently
	logger.Info("starting gdrive folder crawl",
		"folderCount", len(g.FolderIDs),
		"concurrentFolders", g.ConcurrentFolders,
	)

	type folderResult struct {
		result *gdrive.CrawlResult
		err    error
	}
	results := make([]folderResult, len(g.FolderIDs))
	crawlGroup, _ := errgroup.WithContext(ctx)
	crawlGroup.SetLimit(g.ConcurrentFolders)

	for i, folderID := range g.FolderIDs {
		crawlGroup.Go(func() error {
			crawlRes, crawlErr := g.GDriveClient.CrawlFolder(
				ctx, folderID, g.SkipFolderNames, g.MaxRetries)
			results[i] = folderResult{result: crawlRes, err: crawlErr}
			return nil
		})
	}
	_ = crawlGroup.Wait()

	// Merge and filter crawl records to only successful non-folder files
	var fileRecords []gdrive.CrawlRecord
	seen := make(map[string]bool)
	for i, r := range results {
		if r.err != nil {
			logger.Error(r.err, "folder crawl failed", "folderID", g.FolderIDs[i])
			continue
		}
		for _, record := range r.result.Records {
			if record.Status != "successful" {
				continue
			}
			if record.MimeType == "application/vnd.google-apps.folder" {
				continue
			}
			if !seen[record.FileID] {
				seen[record.FileID] = true
				fileRecords = append(fileRecords, record)
			}
		}
	}

	logger.Info("gdrive crawl complete, starting file download",
		"discoveredFiles", len(fileRecords),
		"concurrentDownloads", g.ConcurrentDownloads,
	)

	// Phase 2: Download files, fetch permissions, store to filestore
	var mu sync.Mutex
	var storedFiles []RawFileMetadata
	errorList := map[string]error{}
	currentFileIDs := make(map[string]bool, len(fileRecords))

	dlGroup, _ := errgroup.WithContext(ctx)
	dlGroup.SetLimit(g.ConcurrentDownloads)

	for _, record := range fileRecords {
		currentFileIDs[record.FileID] = true
		dlGroup.Go(func() error {
			filestorePath := path.Join(g.OutputDir, record.FileID, record.FileName)
			uid := record.FileID + ":" + record.UpdatedAt
			file := RawFileMetadata{
				FilePath: filestorePath,
				UID:      uid,
			}

			stored, err := g.storeFile(ctx, fs, &file, record.FileID)
			if err != nil {
				logger.Error(err, "failed to store gdrive file",
					"fileID", record.FileID, "fileName", record.FileName)
				mu.Lock()
				errorList[record.FileID] = err
				mu.Unlock()
				return nil
			}
			if stored {
				logger.Info("stored gdrive file",
					"fileID", record.FileID, "fileName", record.FileName)
				mu.Lock()
				storedFiles = append(storedFiles, file)
				mu.Unlock()
			}
			return nil
		})
	}
	_ = dlGroup.Wait()

	// Phase 3: Garbage collection — delete files and permissions no longer in source
	localFiles, err := fs.ListFilesInPath(ctx, g.OutputDir)
	if err != nil {
		logger.Error(err, "failed to list files in filestore for gc", "outputDir", g.OutputDir)
	} else {
		permissionsPrefix := path.Join(g.OutputDir, "permissions") + "/"
		for _, localFilePath := range localFiles {
			// Handle permissions directory: only keep permissions_<fileID>.json files
			if baseName, ok := strings.CutPrefix(localFilePath, permissionsPrefix); ok {
				if !strings.HasPrefix(baseName, "permissions_") {
					logger.Info("deleting legacy permissions file without prefix", "file", localFilePath)
					if err := fs.Delete(ctx, localFilePath); err != nil {
						logger.Error(err, "failed to delete permissions file", "file", localFilePath)
					}
					continue
				}
				permFileID := strings.TrimSuffix(strings.TrimPrefix(baseName, "permissions_"), ".json")
				if !currentFileIDs[permFileID] {
					logger.Info("permissions file no longer in source, deleting", "file", localFilePath)
					if err := fs.Delete(ctx, localFilePath); err != nil {
						logger.Error(err, "failed to delete permissions file", "file", localFilePath)
					}
				}
				continue
			}

			fileID := g.extractFileID(localFilePath)
			if fileID == "" {
				continue
			}
			if !currentFileIDs[fileID] {
				logger.Info("file no longer in source, deleting from filestore", "file", localFilePath)
				if err := fs.Delete(ctx, localFilePath); err != nil {
					logger.Error(err, "failed to delete file from filestore", "file", localFilePath)
				}
			}
		}
	}

	errorMessage := ""
	for fileID, err := range errorList {
		errorMessage += fmt.Sprintf("fileID: %s, error: %v\n", fileID, err)
	}
	if len(errorMessage) > 0 {
		return storedFiles, errors.New(errorMessage)
	}

	return storedFiles, nil
}

// storeFile downloads a Google Drive file and stores it with metadata,
// then fetches and stores permissions. Permissions are always refreshed
// even when file content hasn't changed, since permissions can change
// independently. Returns true if the file content was newly stored or
// updated, false if file content was skipped due to dedup.
func (g *GDriveSource) storeFile(ctx context.Context, fs *filestore.FileStore, file *RawFileMetadata, fileID string) (bool, error) {
	logger := log.FromContext(ctx)
	filePath := file.FilePath
	metadataPath := MetadataPath(filePath)

	// Check dedup via metadata sidecar — only for file content
	fileChanged := true
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
			logger.V(1).Info("gdrive file unchanged, skipping download",
				"fileID", fileID, "file", filePath)
			fileChanged = false
		}
	}

	if fileChanged {
		reader, err := g.GDriveClient.DownloadFile(ctx, fileID, g.MaxRetries)
		if err != nil {
			return false, fmt.Errorf("failed to download file %s: %w", fileID, err)
		}
		defer reader.Close()

		data, err := io.ReadAll(reader)
		if err != nil {
			return false, fmt.Errorf("failed to read file %s: %w", fileID, err)
		}

		if err := fs.Store(ctx, filePath, data); err != nil {
			return false, fmt.Errorf("failed to store file %s: %w", fileID, err)
		}

		metadataData, err := json.Marshal(file)
		if err != nil {
			return false, fmt.Errorf("failed to marshal metadata for %s: %w", fileID, err)
		}
		if err := fs.Store(ctx, metadataPath, metadataData); err != nil {
			return false, fmt.Errorf("failed to store metadata for %s: %w", fileID, err)
		}
	}

	// Always fetch and store permissions — they can change independently of file content
	permissions, warnings, err := g.GDriveClient.GetFilePermissions(ctx, fileID, g.MaxRetries)
	if err != nil {
		return false, fmt.Errorf("failed to get permissions for %s: %w", fileID, err)
	}
	if len(warnings) > 0 {
		logger.Info("permission warnings", "fileID", fileID, "warnings", warnings)
	}

	permissionsData, err := json.Marshal(permissions)
	if err != nil {
		return false, fmt.Errorf("failed to marshal permissions for %s: %w", fileID, err)
	}
	permissionsPath := path.Join(g.OutputDir, "permissions", "permissions_"+fileID+".json")
	if err := fs.Store(ctx, permissionsPath, permissionsData); err != nil {
		return false, fmt.Errorf("failed to store permissions for %s: %w", fileID, err)
	}

	return fileChanged, nil
}

// extractFileID extracts the Google Drive file ID from a filestore
// path of the form "<outputDir>/<fileID>/<fileName>".
func (g *GDriveSource) extractFileID(filestorePath string) string {
	rel := strings.TrimPrefix(filestorePath, g.OutputDir)
	rel = strings.TrimPrefix(rel, "/")
	parts := strings.SplitN(rel, "/", 2)
	if len(parts) < 2 {
		return ""
	}
	return parts[0]
}
