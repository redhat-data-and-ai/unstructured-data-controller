package unstructured

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
	"github.com/snowflakedb/gosnowflake"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

const (
	// Default artifact paths for destination sync
	DefaultProcessedDocumentsPath = "processed-documents"
	DefaultChunksPath             = "chunks"
	DefaultVectorEmbeddingsPath   = "vector-embeddings"
)

type ArtifactFiles struct {
	Files []string // File paths in filestore
	Path  string   // Destination path (e.g., "processed-documents", "chunks")
}

type Destination interface {
	// SyncFilesToDestination will sync the data to the destination
	SyncFilesToDestination(ctx context.Context, fs *filestore.FileStore, artifactFiles []ArtifactFiles) error
}

type SnowflakeInternalStage struct {
	Client          *snowflake.Client
	Role            string
	Database        string
	Schema          string
	Stage           string
	DataProductName string
}

func (d *SnowflakeInternalStage) SyncFilesToDestination(ctx context.Context,
	fs *filestore.FileStore, artifactFiles []ArtifactFiles) error {
	logger := log.FromContext(ctx)

	// get all files currently in the stage
	type row struct {
		Filename string `db:"filename"`
		Data     string `db:"data"`
	}
	rows := []row{}
	err := d.Client.ListFilesFromStage(ctx, d.Role, d.Database, d.Schema, d.Stage, &rows)
	if err != nil {
		// The query fails when non-JSON files exist in the stage
		// (e.g. manually uploaded test data). Log the error and proceed with
		// an empty stage — all local files will be re-uploaded (PUT uses
		// OVERWRITE=TRUE) and no extra-file cleanup will happen this cycle.
		logger.Error(err, "failed to list files from stage, will re-upload all files")
	}

	// map of stage path to file data (JSON string) in stage
	filesInStage := make(map[string]string)
	for _, row := range rows {
		// Normalize Snowflake filename to match our stagePath format
		// Snowflake returns: path/to/file.json/file.json.gz
		// We need: path/to/file.json
		if row.Filename != "" {
			normalizedPath := normalizeSnowflakeFilename(row.Filename)
			filesInStage[normalizedPath] = row.Data
		}
	}

	logger.Info("files currently in the snowflake internal stage", "count", len(filesInStage))

	errorList := []error{}

	// Process each artifact group
	for _, artifact := range artifactFiles {
		logger.Info("syncing files to snowflake internal stage", "path", artifact.Path, "files", artifact.Files)

		for _, filePathInFilestore := range artifact.Files {
			// read the file from filestore
			fileBytesInFilestore, err := fs.Retrieve(ctx, filePathInFilestore)
			if err != nil {
				logger.Error(err, "failed to retrieve file from filestore", "file", filePathInFilestore)
				errorList = append(errorList, err)
				continue
			}

			// Calculate stage path using artifact.Path directly
			stagePath := buildDestinationPath(filePathInFilestore, artifact.Path)

			// Check if file already exists in the stage
			if existingFileData, exists := filesInStage[stagePath]; exists {
				logger.Info("file already exists in the stage", "file", stagePath)

				// Mark as processed (will be removed from deletion list)
				delete(filesInStage, stagePath)

				// Compare metadata based on file type to see if upload is needed
				if filesAreEqual(existingFileData, fileBytesInFilestore) {
					logger.Info("file metadata unchanged, skipping upload", "file", stagePath)
					continue
				}
			}

			// Upload the file to the stage

			// this is needed to pass the file stream to the snowflake client without creating a local temporary file
			streamCtx := gosnowflake.WithFileStream(ctx, bytes.NewReader(fileBytesInFilestore))

			fileRows := []snowflake.UploadedFileStatus{}

			if err := d.Client.Put(streamCtx,
				d.Role,
				// this file path does not matter as we are using the stream context to pass the file stream to the snowflake client
				filePathInFilestore,
				// database name is the database name
				d.Database,
				// schema name is the data product name
				d.Schema,
				// stage name is the internal stage name
				d.Stage,
				// subpath is the file name
				stagePath,
				&fileRows); err != nil {
				logger.Error(err, "failed to upload file to snowflake internal stage", "file", stagePath)
				errorList = append(errorList, err)
				continue
			}
			logger.Info("successfully uploaded file to snowflake internal stage", "file", stagePath)

			if len(fileRows) == 0 {
				err := fmt.Errorf("no file rows returned while uploading file to snowflake internal stage: %s", stagePath)
				logger.Error(err, "no file rows returned from snowflake put operation", "file", stagePath)
				errorList = append(errorList, err)
				continue
			}
		}
	}

	// Delete extra files that are in the stage but not in filestore
	extraFiles := make([]string, 0, len(filesInStage))
	for stagePath := range filesInStage {
		logger.Info("found extra file in the stage, marking for deletion", "file", stagePath)
		extraFiles = append(extraFiles, stagePath)
	}

	if err := d.Client.DeleteFilesFromStage(ctx, d.Role, d.Database, d.Schema, d.Stage, extraFiles); err != nil {
		logger.Error(err, "failed to delete extra files from snowflake internal stage")
		errorList = append(errorList, err)
	}

	if len(errorList) > 0 {
		return fmt.Errorf("encountered errors while syncing files to snowflake internal stage: %v", errorList)
	}

	return nil
}

// normalizeSnowflakeFilename converts Snowflake's internal filename format to our expected format.
// Snowflake stores files as: path/to/file.json/file.json.gz
// We need: path/to/file.json
func normalizeSnowflakeFilename(snowflakeFilename string) string {
	// Find last slash
	lastSlashIdx := strings.LastIndex(snowflakeFilename, "/")
	if lastSlashIdx == -1 {
		// No slash, return as-is
		return snowflakeFilename
	}

	// Get the part before the last slash
	pathWithoutLast := snowflakeFilename[:lastSlashIdx]

	// Get the last part (should be basename.gz)
	lastPart := snowflakeFilename[lastSlashIdx+1:]

	// Remove .gz suffix if present
	expectedBasename := strings.TrimSuffix(lastPart, ".gz")

	// Verify that pathWithoutLast ends with expectedBasename
	if strings.HasSuffix(pathWithoutLast, expectedBasename) {
		return pathWithoutLast
	}

	// If it doesn't match expected pattern, return original
	return snowflakeFilename
}

// buildDestinationPath builds the destination path for a file artifact
// Extracts prefix from filestore path to preserve source structure (e.g., /source-data/)
// Format: {extractedPrefix}/stages/{artifact-path}/{filename}
// Example: testproduct/source-data/stages/chunks/test.pdf-chunks.json
func buildDestinationPath(filePathInFilestore string, artifactPath string) string {
	baseName := filepath.Base(filePathInFilestore)

	// filePathInFilestore example: "testunstructureddataproduct/source-data/file.pdf-converted.json"
	// "testunstructureddataproduct/source-data"
	var extractedPrefix string
	if idx := strings.LastIndex(filePathInFilestore, "/"); idx != -1 {
		extractedPrefix = filePathInFilestore[:idx]
	}

	// Build path: {extractedPrefix}/stages/{artifact-path}/{filename}
	var path string
	if extractedPrefix != "" && artifactPath != "" {
		path = filepath.Join(extractedPrefix, "stages", artifactPath, baseName)
	} else if artifactPath != "" {
		path = filepath.Join("stages", artifactPath, baseName)
	} else {
		path = baseName
	}

	if filepath.Separator != '/' {
		path = filepath.ToSlash(path)
	}
	return path
}

// filesAreEqual compares files using generic JSON comparison
func filesAreEqual(existingData string, newData []byte) bool {
	var existing, incoming any

	if err := json.Unmarshal([]byte(existingData), &existing); err != nil {
		return false
	}
	if err := json.Unmarshal(newData, &incoming); err != nil {
		return false
	}

	return reflect.DeepEqual(existing, incoming)
}

// S3Destination syncs processed JSON artifacts to an S3 bucket (converted, chunks,
// and vector embeddings — whichever paths the controller passes, based on artifacts configuration).
type S3Destination struct {
	S3Client        *s3.Client
	Bucket          string
	Prefix          string
	DataProductName string            // used as default prefix when Prefix is empty (CR name)
	ArtifactPathMap map[string]string // map[fileSuffix]artifactPath for stages structure
}

func (d *S3Destination) getPrefix() string {
	if d.Prefix != "" {
		return d.Prefix
	}
	return d.DataProductName
}

// buildDestinationPath builds the destination path for a file artifact
// Format: {prefix}/stages/{artifact-path}/{filename}
// Example: testproduct/stages/chunks/test.pdf-chunks.json
func (d *S3Destination) buildDestinationPath(filePathInFilestore string, artifactPath string) string {
	prefix := d.getPrefix()
	baseName := filepath.Base(filePathInFilestore)

	var path string
	if prefix != "" {
		path = filepath.Join(prefix, "stages", artifactPath, baseName)
	} else {
		path = filepath.Join("stages", artifactPath, baseName)
	}

	if filepath.Separator != '/' {
		path = filepath.ToSlash(path)
	}

	return path
}

func (d *S3Destination) SyncFilesToDestination(ctx context.Context, fs *filestore.FileStore,
	artifactFiles []ArtifactFiles) error {
	logger := log.FromContext(ctx)
	logger.Info("syncing data to S3 destination",
		"bucket", d.Bucket, "prefix", d.getPrefix(), "filePaths", artifactFiles)

	s3Client := d.S3Client
	if s3Client == nil {
		var err error
		s3Client, err = awsclienthandler.GetDestinationS3Client()
		if err != nil {
			return fmt.Errorf("failed to get S3 client: %w", err)
		}
	}

	// Keys currently in the destination (same idea as Snowflake: one map, trim as we sync, delete the rest).
	keysInDestination := make(map[string]bool)
	paginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(d.Bucket),
		Prefix: aws.String(d.getPrefix()),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return fmt.Errorf("list objects s3://%s prefix %q: %w", d.Bucket, d.getPrefix(), err)
		}
		for _, obj := range page.Contents {
			if obj.Key != nil {
				keysInDestination[*obj.Key] = true
			}
		}
	}

	for _, artifact := range artifactFiles {
		logger.Info("syncing artifact", "path", artifact.Path, "fileCount", len(artifact.Files))

		for _, filePath := range artifact.Files {
			data, err := fs.Retrieve(ctx, filePath)
			if err != nil {
				logger.Error(err, "failed to retrieve file from filestore", "file", filePath)
				return fmt.Errorf("retrieve %s: %w", filePath, err)
			}

			key := d.buildDestinationPath(filePath, artifact.Path)
			delete(keysInDestination, key)

			// Calculate SHA256 of local file
			hash := sha256.Sum256(data)
			localSHA256 := base64.StdEncoding.EncodeToString(hash[:])

			// Check if file exists and compare SHA256 from metadata
			headResp, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
				Bucket:       aws.String(d.Bucket),
				Key:          aws.String(key),
				ChecksumMode: types.ChecksumModeEnabled,
			})

			var notFoundErr *types.NotFound
			if err != nil && errors.As(err, &notFoundErr) {
				if notFoundErr.ErrorCode() != "NotFound" {
					logger.Info("error while fetching the object",
						"key", key, "error", err.Error())
					return err
				}
			}

			if err == nil && headResp.ChecksumSHA256 != nil && *headResp.ChecksumSHA256 == localSHA256 {
				logger.Info("file unchanged, skipping upload",
					"file", filePath, "key", key)
				continue
			}

			// File is new or changed, upload it with SHA256
			_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket:            aws.String(d.Bucket),
				Key:               aws.String(key),
				Body:              bytes.NewReader(data),
				ChecksumAlgorithm: types.ChecksumAlgorithmSha256,
			})
			if err != nil {
				logger.Error(err, "failed to upload file to S3", "bucket", d.Bucket, "key", key)
				return fmt.Errorf("put object s3://%s/%s: %w", d.Bucket, key, err)
			}
			logger.Info("uploaded file to S3 destination", "key", key)
		}
	}

	// Remaining keys = files in destination but no longer in ingestion; delete
	for key := range keysInDestination {
		logger.Info("found file in destination no longer in ingestion, deleting", "key", key)
		_, err := s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(d.Bucket),
			Key:    aws.String(key),
		})
		if err != nil {
			logger.Error(err, "failed to delete file from S3 destination", "key", key)
			return fmt.Errorf("delete object s3://%s/%s: %w", d.Bucket, key, err)
		}
	}

	return nil
}
