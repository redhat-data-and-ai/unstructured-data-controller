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

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
	"github.com/snowflakedb/gosnowflake"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type Destination interface {
	// SyncFilesToDestination will sync the data to the destination
	SyncFilesToDestination(ctx context.Context, fs *filestore.FileStore, filePaths []string) error
}

type SnowflakeInternalStage struct {
	Client   *snowflake.Client
	Role     string
	Database string
	Schema   string
	Stage    string
}

func (d *SnowflakeInternalStage) SyncFilesToDestination(ctx context.Context,
	fs *filestore.FileStore, embeddingsFilePaths []string) error {
	logger := log.FromContext(ctx)
	logger.Info("ingesting data to snowflake internal stage", "filePaths", embeddingsFilePaths)

	// get file name and uid for all files in the stage
	type row struct {
		Data string `db:"data"`
	}
	rows := []row{}
	err := d.Client.ListFilesFromStage(ctx, d.Role, d.Database, d.Schema, d.Stage, &rows)
	if err != nil {
		// The SELECT $1 query fails when non-JSON files exist in the stage
		// (e.g. manually uploaded test data). Log the error and proceed with
		// an empty stage — all local files will be re-uploaded (PUT uses
		// OVERWRITE=TRUE) and no extra-file cleanup will happen this cycle.
		logger.Error(err, "failed to list files from stage, will re-upload all files")
	}

	// map of raw file path to embeddings file
	embeddingsFilesInStage := make(map[string]EmbeddingsFile)
	embeddingsFilesList := []string{}
	for _, row := range rows {
		embeddingsFile := EmbeddingsFile{}
		err := json.Unmarshal([]byte(row.Data), &embeddingsFile)
		if err != nil {
			logger.Info("skipping non-embeddings file in stage", "error", err)
			continue
		}
		if embeddingsFile.ConvertedDocument != nil &&
			embeddingsFile.ConvertedDocument.Metadata != nil &&
			embeddingsFile.ConvertedDocument.Metadata.RawFilePath != "" {
			embeddingsFilesInStage[embeddingsFile.ConvertedDocument.Metadata.RawFilePath] = embeddingsFile
			embeddingsFilesList = append(embeddingsFilesList, embeddingsFile.ConvertedDocument.Metadata.RawFilePath)
		}
	}

	logger.Info("files currently in the snowflake internal stage", "files", embeddingsFilesList)
	logger.Info("list of files in the local file store to be stored", "files", embeddingsFilePaths)
	errorList := []error{}
	for _, embeddingsFilePathInFilestore := range embeddingsFilePaths {
		// read the file from filestore
		embeddingsFileBytesInFilestore, err := fs.Retrieve(ctx, embeddingsFilePathInFilestore)
		if err != nil {
			logger.Error(err, "failed to retrieve file from filestore", "file", embeddingsFilePathInFilestore)
			errorList = append(errorList, err)
		}

		embeddingsFileInFilestore := EmbeddingsFile{}
		err = json.Unmarshal(embeddingsFileBytesInFilestore, &embeddingsFileInFilestore)
		if err != nil {
			logger.Error(err, "failed to unmarshal file", "file", embeddingsFilePathInFilestore)
			errorList = append(errorList, err)
			continue
		}

		// check if embeddings file already exists in the stage
		if _, exists := embeddingsFilesInStage[embeddingsFileInFilestore.ConvertedDocument.Metadata.RawFilePath]; exists {
			logger.Info("file already exists in the stage",
				"file", embeddingsFileInFilestore.ConvertedDocument.Metadata.RawFilePath)

			embeddingsFileInStage := embeddingsFilesInStage[embeddingsFileInFilestore.ConvertedDocument.Metadata.RawFilePath]

			// delete the file from the map as we will use this map to delete extra files from the stage
			delete(embeddingsFilesInStage, embeddingsFileInFilestore.ConvertedDocument.Metadata.RawFilePath)

			if embeddingsFileInStage.EmbeddingDocument.Metadata.Equal(embeddingsFileInFilestore.EmbeddingDocument.Metadata) {
				logger.Info("file is already in the stage and the configuration is the same, skipping ...",
					"file", embeddingsFileInFilestore.ConvertedDocument.Metadata.RawFilePath)
				// nothing to do, file is already in the stage
				continue
			}
		}

		// upload the file to the stage

		// this is needed to pass the file stream to the snowflake client without creating a local temporary file
		streamCtx := gosnowflake.WithFileStream(ctx, bytes.NewReader(embeddingsFileBytesInFilestore))

		fileRows := []snowflake.UploadedFileStatus{}

		if err := d.Client.Put(streamCtx,
			d.Role,
			// this file path does not matter as we are using the stream context to pass the file stream to the snowflake client
			embeddingsFilePathInFilestore,
			// database name is the database name
			d.Database,
			// schema name is the data product name
			d.Schema,
			// stage name is the internal stage name
			d.Stage,
			// subpath is the file name
			embeddingsFilePathInFilestore,
			&fileRows); err != nil {
			logger.Error(err, "failed to upload file to snowflake internal stage", "file", embeddingsFilePathInFilestore)
			errorList = append(errorList, err)
			continue
		}
		logger.Info("successfully uploaded file to snowflake internal stage",
			"file", embeddingsFileInFilestore.ConvertedDocument.Metadata.RawFilePath)

		if len(fileRows) == 0 {
			logger.Error(fmt.Errorf("no file rows returned while uploading file to snowflake internal stage: %s",
				embeddingsFileInFilestore.ConvertedDocument.Metadata.RawFilePath),
				"no file rows returned",
				"file", embeddingsFileInFilestore.ConvertedDocument.Metadata.RawFilePath)
			errorList = append(errorList,
				fmt.Errorf("no file rows returned while uploading file to snowflake internal stage: %s",
					embeddingsFileInFilestore.ConvertedDocument.Metadata.RawFilePath))
			continue
		}
	}

	// delete extra files which are the files in the stage that are not present in the filestore
	// at this point, whatever is left in the filesInStage map are the extra files that need to be deleted

	extraFiles := make([]string, 0, len(embeddingsFilesInStage))
	for extraFilePath := range embeddingsFilesInStage {
		logger.Info("found extra file in the stage, marking for deletion", "file", extraFilePath)
		extraFiles = append(extraFiles, extraFilePath)
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

// S3Destination syncs chunk files to an S3 bucket (e.g. LocalStack or AWS).
type S3Destination struct {
	S3Client        *s3.Client
	Bucket          string
	Prefix          string
	DataProductName string // used as default prefix when Prefix is empty (CR name)
}

func (d *S3Destination) getPrefix() string {
	if d.Prefix != "" {
		return d.Prefix
	}
	return d.DataProductName
}

// s3KeyForChunksFile returns the S3 object key for a chunks file path
// When Prefix is not set, uses DataProductName/file_name as default.
func (d *S3Destination) s3KeyForChunksFile(chunksFilePath string) string {
	baseName := filepath.Base(chunksFilePath)
	prefix := d.getPrefix()
	key := baseName
	if prefix != "" {
		key = filepath.Join(prefix, baseName)
	}
	if filepath.Separator != '/' {
		key = filepath.ToSlash(key)
	}
	return key
}

func (d *S3Destination) SyncFilesToDestination(ctx context.Context, fs *filestore.FileStore,
	chunksFilePaths []string) error {
	logger := log.FromContext(ctx)
	logger.Info("syncing data to S3 destination",
		"bucket", d.Bucket, "prefix", d.getPrefix(), "filePaths", chunksFilePaths)

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

	for _, chunksFilePath := range chunksFilePaths {
		data, err := fs.Retrieve(ctx, chunksFilePath)
		if err != nil {
			logger.Error(err, "failed to retrieve file from filestore", "file", chunksFilePath)
			return fmt.Errorf("retrieve %s: %w", chunksFilePath, err)
		}

		key := d.s3KeyForChunksFile(chunksFilePath)
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
				"file", chunksFilePath, "key", key)
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
