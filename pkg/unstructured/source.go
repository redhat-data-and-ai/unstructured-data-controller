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
	"strings"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type DataSource interface {
	// SyncFilesToFilestore will store all files from the source to the filestore and return the list of file paths
	SyncFilesToFilestore(ctx context.Context, fs *filestore.FileStore) ([]RawFileMetadata, error)
}

type S3BucketSource struct {
	Bucket string
	Prefix string
}

func (s *S3BucketSource) SyncFilesToFilestore(ctx context.Context, fs *filestore.FileStore) ([]RawFileMetadata, error) {
	logger := log.FromContext(ctx)
	logger.Info("listing objects in prefix", "bucket", s.Bucket, "prefix", s.Prefix)
	objects, err := awsclienthandler.ListObjectsInPrefix(ctx, s.Bucket, s.Prefix)
	if err != nil {
		return nil, err
	}

	storedFiles := []RawFileMetadata{}
	errorList := map[string]error{}
	sourceFileMap := map[string]bool{}

	for _, object := range objects {
		file := RawFileMetadata{
			FilePath: *object.Key,
			UID:      *object.ETag,
		}
		// file: testunstructureddataproduct/12.pdf
		logger.Info("storing file", "file", file.FilePath)
		sourceFileMap[file.FilePath] = true

		if err := s.storeFile(ctx, fs, &file); err != nil {
			logger.Error(err, "failed to store file", "file", file.FilePath)
			errorList[file.FilePath] = err
			continue
		}
		logger.Info("successfully stored file", "file", file.FilePath)
		storedFiles = append(storedFiles, file)
	}

	// TODO: delete files from the filestore that are not present in the source (shikhar)
	// also, delete the metadata files, chunked files and vector embeddings files if the raw file deleted
	// Listing all the file in the local s3 filestore
	localFiles, err := fs.ListFilesInPath(ctx, s.Prefix)
	if err != nil {
		logger.Error(err, "failed to list files in filestore", "prefix", s.Prefix)
		return nil, err
	}
	for _, localFilePath := range localFiles {
		// check if has suffix .pdf
		if strings.HasSuffix(localFilePath, ".pdf") {
			if _, exists := sourceFileMap[localFilePath]; !exists {
				logger.Info("file doesnot exist in the source, deleting from the filestore", "file", localFilePath)
				err := fs.Delete(ctx, localFilePath)
				if err != nil {
					logger.Error(err, "failed to delete file from filestore", "file", localFilePath)
					errorList[localFilePath] = err
					continue
				}
				err = fs.Delete(ctx, GetMetadataFilePath(localFilePath))
				if err != nil {
					logger.Error(err, "failed to delete metadata file from filestore", "file", GetMetadataFilePath(localFilePath))
					errorList[localFilePath] = err
					continue
				}
				err = fs.Delete(ctx, GetConvertedFilePath(localFilePath))
				if err != nil {
					logger.Error(err, "failed to delete converted file from filestore", "file", GetConvertedFilePath(localFilePath))
					errorList[localFilePath] = err
					continue
				}
				err = fs.Delete(ctx, GetChunksFilePath(localFilePath))
				if err != nil {
					logger.Error(err, "failed to delete chunks file from filestore", "file", GetChunksFilePath(localFilePath))
					errorList[localFilePath] = err
					continue
				}
				logger.Info("successfully deleted file and associated files from the filestore", "file", localFilePath)
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
func (s *S3BucketSource) storeFile(ctx context.Context, fs *filestore.FileStore, file *RawFileMetadata) error {
	logger := log.FromContext(ctx)
	logger.Info("storing file", "file", file.FilePath)

	filePath := file.FilePath
	metadataPath := GetMetadataFilePath(filePath)

	// check if the file exists in the filestore

	// for a file to exist in the filestore, both, the file and the metadata file must exist
	fileExists, err := fs.Exists(ctx, filePath)
	if err != nil {
		logger.Error(err, "failed to check if file exists in filestore", "file", filePath)
		return err
	}

	metadataExists, err := fs.Exists(ctx, metadataPath)
	if err != nil {
		logger.Error(err, "failed to check if metadata file exists in filestore", "file", metadataPath)
		return err
	}

	if fileExists && metadataExists {
		logger.Info("file and metadata file exist in filestore, checking if they are the same", "file",
			filePath, "metadataFile", metadataPath)

		// then compare the metadata file's ETag with the object's ETag
		metadata, err := fs.Retrieve(ctx, metadataPath)
		if err != nil {
			logger.Error(err, "failed to retrieve metadata file from filestore", "file", metadataPath)
			return err
		}

		// unmarshal the metadata file into a FileMetadata struct
		var existingFile RawFileMetadata
		err = json.Unmarshal(metadata, &existingFile)
		if err != nil {
			logger.Error(err, "failed to unmarshal metadata file", "file", metadataPath)
			return err
		}

		if existingFile.UID == file.UID {
			// the file and the metadata file are the same, so we can skip storing it
			logger.Info("file and metadata file are the same, skipping ...", "file", filePath)
			return nil
		}
	}

	// we are here because the file or the metadata file does not exist
	// so we can safely store the file and the corresponding metadata file

	// store the file first
	objectOutput, err := awsclienthandler.GetObject(ctx, s.Bucket, filePath)
	if err != nil {
		logger.Error(err, "failed to get object from S3", "file", filePath)
		return err
	}

	data, err := io.ReadAll(objectOutput.Body)
	if err != nil {
		logger.Error(err, "failed to read object from S3", "file", filePath)
		return err
	}
	if err = fs.Store(ctx, filePath, data); err != nil {
		logger.Error(err, "failed to store file in filestore", "file", filePath)
		return err
	}

	metadataData, err := json.Marshal(file)
	if err != nil {
		logger.Error(err, "failed to marshal metadata file", "file", metadataPath)
		return err
	}
	if err = fs.Store(ctx, metadataPath, metadataData); err != nil {
		logger.Error(err, "failed to store metadata file in filestore", "file", metadataPath)
		return err
	}

	logger.Info("successfully stored file and metadata file in filestore", "file", filePath, "metadataFile", metadataPath)
	return nil
}
