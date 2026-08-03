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

const maxFileSize int64 = 128 << 20 // 128 MB — Snowflake external stage limit

var errFileExceedsMaxSize = errors.New("file exceeds max size limit (128 MB)")

type DataSource interface {
	// SyncFilesToFilestore will store all files from the source to the filestore and return the list of file paths
	SyncFilesToFilestore(ctx context.Context, fs *filestore.FileStore) ([]RawFileMetadata, error)
}

// UnsupportedFiles tracks files skipped during a crawl because their type is
// unsupported. It is owned by the SourceCrawler controller and handed down
// into whichever DataSource is in use, since "which files were skipped" is a
// property of the crawl itself, not of a specific source implementation like
// S3 or Google Drive.
type UnsupportedFiles struct {
	files []string
}

// Add records a file as skipped due to an unsupported type.
func (u *UnsupportedFiles) Add(fileName string) {
	u.files = append(u.files, fileName)
}

// Reset clears previously recorded files, e.g. at the start of a new sync.
func (u *UnsupportedFiles) Reset() {
	u.files = nil
}

// List returns the file names recorded since the last Reset.
func (u *UnsupportedFiles) List() []string {
	return u.files
}

type S3BucketSource struct {
	S3Client         *s3.Client
	Bucket           string
	Prefix           string
	OutputDir        string
	UnsupportedFiles *UnsupportedFiles
}

func (s *S3BucketSource) SyncFilesToFilestore(ctx context.Context, fs *filestore.FileStore) ([]RawFileMetadata, error) {
	logger := log.FromContext(ctx)
	logger.Info("listing objects in prefix", "bucket", s.Bucket, "prefix", s.Prefix)
	objects, err := awsclienthandler.ListObjectsInPrefix(ctx, s.S3Client, s.Bucket, s.Prefix)
	if err != nil {
		return nil, err
	}

	s.UnsupportedFiles.Reset()
	storedFiles := []RawFileMetadata{}
	errorList := map[string]error{}
	sourceFileMap := map[string]bool{}
	catalogIDs := map[string]bool{}
	outputPrefix := s.outputPrefix()
	catalogPrefix := path.Join(outputPrefix, CrawlMetadataDir) + "/"

	for _, object := range objects {
		// skip S3 folder marker objects (keys ending with "/") — storing these
		// as regular files would block directory creation for real files underneath
		if strings.HasSuffix(*object.Key, "/") {
			continue
		}

		rel := strings.TrimPrefix(*object.Key, s.Prefix)
		fileID := strings.ReplaceAll(rel, "/", "__")
		catalogIDs[fileID] = true
		baseName := path.Base(*object.Key)
		sourcePath := fmt.Sprintf("s3://%s/%s", s.Bucket, *object.Key)

		if object.Size != nil && *object.Size > maxFileSize {
			logger.Info("WARNING: skipping file exceeding max file size limit",
				"key", *object.Key, "sizeMB", *object.Size/(1<<20))
			if err := storeCrawlResult(ctx, fs, outputPrefix, CrawlResult{
				FileID: fileID, FileName: baseName, SourcePath: sourcePath,
				Status: CrawlStatusSkipped, Reason: "file exceeds max size limit (128 MB)",
				SourceType: "s3",
			}); err != nil {
				logger.Error(err, "failed to store crawl catalog result", "fileID", fileID)
			}
			continue
		}

		if !IsSupportedFileType(*object.Key) {
			logger.Info("skipping unsupported file type",
				"file", *object.Key,
				"extension", FileExtension(*object.Key),
			)
			s.UnsupportedFiles.Add(*object.Key)
			if err := storeCrawlResult(ctx, fs, outputPrefix, CrawlResult{
				FileID: fileID, FileName: baseName, SourcePath: sourcePath,
				Status:     CrawlStatusSkipped,
				Reason:     fmt.Sprintf("unsupported file type %q", FileExtension(*object.Key)),
				SourceType: "s3",
			}); err != nil {
				logger.Error(err, "failed to store crawl catalog result", "fileID", fileID)
			}
			continue
		}

		file := RawFileMetadata{
			FilePath: s.filestorePath(*object.Key),
			UID:      *object.ETag,
		}
		logger.Info("storing file", "file", file.FilePath)
		sourceFileMap[file.FilePath] = true

		stored, err := s.storeFile(ctx, fs, &file)
		status, reason := CrawlStatusSuccessful, ""
		if err != nil {
			logger.Error(err, "failed to store file", "file", file.FilePath)
			errorList[file.FilePath] = err
			status, reason = CrawlStatusError, err.Error()
		} else if stored {
			logger.Info("successfully stored file", "file", file.FilePath)
			storedFiles = append(storedFiles, file)
		}
		if err := storeCrawlResult(ctx, fs, outputPrefix, CrawlResult{
			FileID: fileID, FileName: baseName, SourcePath: sourcePath,
			Status: status, Reason: reason, SourceType: "s3",
		}); err != nil {
			logger.Error(err, "failed to store crawl catalog result", "fileID", fileID)
		}
	}
	// Listing all the file in the local s3 filestore
	localFiles, err := fs.ListFilesInPath(ctx, outputPrefix)
	if err != nil {
		logger.Error(err, "failed to list files in filestore", "prefix", s.Prefix)
		return nil, err
	}

	// logic to delete files and its respective files if the file is removed from upstream bucket
	for _, localFilePath := range localFiles {
		// catalog/<id>.json — GC separately from raw files / sidecars
		if baseName, ok := strings.CutPrefix(localFilePath, catalogPrefix); ok {
			catalogID := strings.TrimSuffix(baseName, ".json")
			if _, exists := catalogIDs[catalogID]; !exists {
				logger.Info("catalog entry no longer in source, deleting", "file", localFilePath)
				if err := fs.Delete(ctx, localFilePath); err != nil {
					logger.Error(err, "failed to delete catalog file from filestore", "file", localFilePath)
					errorList[localFilePath] = err
				}
			}
			continue
		}

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

type FailedRootFolder struct {
	FolderID string
	Error    string
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
	FailedRootFolders   []FailedRootFolder
	UnsupportedFiles    *UnsupportedFiles
}

// Close releases resources held by the underlying clients.
func (g *GDriveSource) Close() {
	g.GDriveClient.Close()
}

type folderResult struct {
	result *gdrive.CrawlResult
	err    error
}

func (g *GDriveSource) filterCrawlRecords(
	ctx context.Context, fs *filestore.FileStore,
	results []folderResult,
) ([]gdrive.CrawlRecord, map[string]bool) {
	logger := log.FromContext(ctx)
	var fileRecords []gdrive.CrawlRecord
	seen := make(map[string]bool)
	catalogIDs := map[string]bool{}

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
			catalogIDs[record.FileID] = true
			if record.FileSize > 0 && record.FileSize > maxFileSize {
				logger.Info("WARNING: skipping file exceeding max file size limit",
					"fileID", record.FileID, "fileName", record.FileName,
					"sizeMB", record.FileSize/(1<<20))
				if err := storeCrawlResult(ctx, fs, g.OutputDir, CrawlResult{
					FileID: record.FileID, FileName: record.FileName,
					FileURL:   GDriveFileURL(record.FileID),
					MediaType: record.MimeType, Status: CrawlStatusSkipped,
					Reason: errFileExceedsMaxSize.Error(), SourceType: "googleDrive",
				}); err != nil {
					logger.Error(err, "failed to store crawl catalog result", "fileID", record.FileID)
				}
				continue
			}
			if !seen[record.FileID] {
				seen[record.FileID] = true
				fileRecords = append(fileRecords, record)
			}
		}
	}
	return fileRecords, catalogIDs
}

func (g *GDriveSource) garbageCollect(
	ctx context.Context, fs *filestore.FileStore,
	currentFiles map[string]string, catalogIDs map[string]bool,
) {
	logger := log.FromContext(ctx)
	localFiles, err := fs.ListFilesInPath(ctx, g.OutputDir)
	if err != nil {
		logger.Error(err, "failed to list files in filestore for gc", "outputDir", g.OutputDir)
		return
	}

	permissionsPrefix := path.Join(g.OutputDir, "permissions") + "/"
	catalogPrefix := path.Join(g.OutputDir, CrawlMetadataDir) + "/"
	for _, localFilePath := range localFiles {
		if baseName, ok := strings.CutPrefix(localFilePath, permissionsPrefix); ok {
			permFileID := strings.TrimSuffix(baseName, ".json")
			if _, exists := currentFiles[permFileID]; !exists {
				logger.Info("permissions file no longer in source, deleting", "file", localFilePath)
				if err := fs.Delete(ctx, localFilePath); err != nil {
					logger.Error(err, "failed to delete permissions file", "file", localFilePath)
				}
			}
			continue
		}

		if baseName, ok := strings.CutPrefix(localFilePath, catalogPrefix); ok {
			catalogID := strings.TrimSuffix(baseName, ".json")
			if _, exists := catalogIDs[catalogID]; !exists {
				logger.Info("catalog entry no longer in source, deleting", "file", localFilePath)
				if err := fs.Delete(ctx, localFilePath); err != nil {
					logger.Error(err, "failed to delete catalog file", "file", localFilePath)
				}
			}
			continue
		}

		fileID := g.extractFileID(localFilePath)
		if fileID == "" {
			continue
		}
		if _, exists := currentFiles[fileID]; !exists {
			logger.Info("file no longer in source, deleting from filestore", "file", localFilePath)
			if err := fs.Delete(ctx, localFilePath); err != nil {
				logger.Error(err, "failed to delete file from filestore", "file", localFilePath)
			}
		}
	}
}

func (g *GDriveSource) SyncFilesToFilestore(ctx context.Context, fs *filestore.FileStore) ([]RawFileMetadata, error) {
	logger := log.FromContext(ctx)

	// Phase 1: Crawl all root folders concurrently
	logger.Info("starting gdrive folder crawl",
		"folderCount", len(g.FolderIDs),
		"concurrentFolders", g.ConcurrentFolders,
	)

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

	fileRecords, catalogIDs := g.filterCrawlRecords(ctx, fs, results)

	if len(g.FailedRootFolders) == len(g.FolderIDs) {
		return nil, errors.New("all configured root folders are inaccessible (service account may lack access)")
	}

	logger.Info("gdrive crawl complete, starting file download",
		"discoveredFiles", len(fileRecords),
		"concurrentDownloads", g.ConcurrentDownloads,
	)

	// Phase 2: Download files, fetch permissions, store to filestore
	g.UnsupportedFiles.Reset()
	var mu sync.Mutex
	var storedFiles []RawFileMetadata
	errorList := map[string]error{}
	// Maps fileID → expected extension for GC rename detection
	currentFiles := make(map[string]string, len(fileRecords))

	dlGroup, _ := errgroup.WithContext(ctx)
	dlGroup.SetLimit(g.ConcurrentDownloads)

	for _, record := range fileRecords {
		ext := path.Ext(record.FileName)
		if strings.HasPrefix(record.MimeType, "application/vnd.google-apps.") {
			ext = ".pdf"
		}

		if !SupportedFileExtensions[strings.ToLower(ext)] {
			logger.Info("skipping unsupported file type",
				"fileID", record.FileID,
				"fileName", record.FileName,
				"extension", FileExtension(record.FileName),
				"mimeType", record.MimeType,
			)
			g.UnsupportedFiles.Add(record.FileName)
			if err := storeCrawlResult(ctx, fs, g.OutputDir, CrawlResult{
				FileID: record.FileID, FileName: record.FileName,
				FileURL:   GDriveFileURL(record.FileID),
				MediaType: record.MimeType, Extension: strings.ToLower(ext),
				Status:     CrawlStatusSkipped,
				Reason:     fmt.Sprintf("unsupported file type %q", FileExtension(record.FileName)),
				SourceType: "googleDrive",
			}); err != nil {
				logger.Error(err, "failed to store crawl catalog result", "fileID", record.FileID)
			}
			continue
		}

		currentFiles[record.FileID] = ext
		dlGroup.Go(func() error {
			filestorePath := path.Join(g.OutputDir, record.FileID+ext)
			uid := record.FileID + ":" + record.UpdatedAt
			file := RawFileMetadata{
				FilePath: filestorePath,
				UID:      uid,
			}

			stored, err := g.storeFile(ctx, fs, &file, record.FileID)
			status, reason := CrawlStatusSuccessful, ""
			if err != nil {
				if errors.Is(err, errFileExceedsMaxSize) {
					logger.Info("WARNING: skipping file exceeding max file size limit",
						"fileID", record.FileID, "fileName", record.FileName)
					status, reason = CrawlStatusSkipped, err.Error()
				} else {
					logger.Error(err, "failed to store gdrive file",
						"fileID", record.FileID, "fileName", record.FileName)
					mu.Lock()
					errorList[record.FileID] = err
					mu.Unlock()
					status, reason = CrawlStatusError, err.Error()
				}
			} else if stored {
				logger.Info("stored gdrive file",
					"fileID", record.FileID, "fileName", record.FileName)
			}

			// still append the file to the storedFiles list,
			mu.Lock()
			storedFiles = append(storedFiles, file)
			mu.Unlock()

			if storeErr := storeCrawlResult(ctx, fs, g.OutputDir, CrawlResult{
				FileID: record.FileID, FileName: record.FileName,
				FileURL:   GDriveFileURL(record.FileID),
				MediaType: record.MimeType, Extension: strings.ToLower(ext),
				Status: status, Reason: reason, SourceType: "googleDrive",
			}); storeErr != nil {
				logger.Error(storeErr, "failed to store crawl catalog result", "fileID", record.FileID)
			}
			return nil
		})
	}
	_ = dlGroup.Wait()

	// Phase 3: Garbage collection
	g.garbageCollect(ctx, fs, currentFiles, catalogIDs)

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
func (g *GDriveSource) storeFile(
	ctx context.Context, fs *filestore.FileStore, file *RawFileMetadata, fileID string,
) (bool, error) {
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
		defer func() { _ = reader.Close() }()

		data, err := io.ReadAll(io.LimitReader(reader, maxFileSize+1))
		if err != nil {
			return false, fmt.Errorf("failed to read file %s: %w", fileID, err)
		}

		if int64(len(data)) > maxFileSize {
			logger.Info("WARNING: skipping file exceeding max file size limit",
				"fileID", fileID, "sizeMB", len(data)/(1<<20))
			return false, errFileExceedsMaxSize
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
	permissionsPath := path.Join(g.OutputDir, "permissions", fileID+".json")
	if err := fs.Store(ctx, permissionsPath, permissionsData); err != nil {
		return false, fmt.Errorf("failed to store permissions for %s: %w", fileID, err)
	}

	return fileChanged, nil
}

// extractFileID extracts the Google Drive file ID from a filestore
// path like "<outputDir>/<fileID>.pdf" or "<outputDir>/<fileID>.pdf.json".
func (g *GDriveSource) extractFileID(filestorePath string) string {
	rel := strings.TrimPrefix(filestorePath, g.OutputDir)
	rel = strings.TrimPrefix(rel, "/")
	if rel == "" || strings.Contains(rel, "/") {
		return ""
	}
	// Strip .json metadata suffix first, then the file extension
	name := strings.TrimSuffix(rel, ".json")
	fileID := strings.TrimSuffix(name, path.Ext(name))
	if fileID == "" {
		return name
	}
	return fileID
}
