package unstructured

import (
	"bytes"
	"context"
	"crypto/md5"
	"encoding/json"
	"fmt"
	"path/filepath"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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
	fs *filestore.FileStore, chunksFilePaths []string) error {
	logger := log.FromContext(ctx)
	logger.Info("ingesting data to snowflake internal stage", "filePaths", chunksFilePaths)

	// get file name and uid for all files in the stage
	type row struct {
		Data string `db:"data"`
	}
	rows := []row{}
	err := d.Client.ListFilesFromStage(ctx, d.Role, d.Database, d.Schema, d.Stage, &rows)
	if err != nil {
		return err
	}

	// map of raw file path to chunks file
	chunksFilesInStage := make(map[string]ChunksFile)
	chunkFilesList := []string{}
	for _, row := range rows {
		chunksFile := ChunksFile{}
		err := json.Unmarshal([]byte(row.Data), &chunksFile)
		if err != nil {
			return err
		}
		if chunksFile.ConvertedDocument != nil &&
			chunksFile.ConvertedDocument.Metadata != nil &&
			chunksFile.ConvertedDocument.Metadata.RawFilePath != "" {
			chunksFilesInStage[chunksFile.ConvertedDocument.Metadata.RawFilePath] = chunksFile
			chunkFilesList = append(chunkFilesList, chunksFile.ConvertedDocument.Metadata.RawFilePath)
		}
	}

	logger.Info("files currently in the snowflake internal stage", "files", chunkFilesList)
	logger.Info("list of files in the local file store to be stored", "files", chunksFilePaths)
	errorList := []error{}
	for _, chunksFilePathInFilestore := range chunksFilePaths {
		// read the file from filestore
		chunksFileBytesInFilestore, err := fs.Retrieve(ctx, chunksFilePathInFilestore)
		if err != nil {
			logger.Error(err, "failed to retrieve file from filestore", "file", chunksFilePathInFilestore)
			errorList = append(errorList, err)
		}

		chunksFileInFilestore := ChunksFile{}
		err = json.Unmarshal(chunksFileBytesInFilestore, &chunksFileInFilestore)
		if err != nil {
			logger.Error(err, "failed to unmarshal file", "file", chunksFilePathInFilestore)
			errorList = append(errorList, err)
			continue
		}

		// check if chunks file already exists in the stage
		if _, exists := chunksFilesInStage[chunksFileInFilestore.ConvertedDocument.Metadata.RawFilePath]; exists {
			logger.Info("file already exists in the stage", "file", chunksFileInFilestore.ConvertedDocument.Metadata.RawFilePath)

			chunksFileInStage := chunksFilesInStage[chunksFileInFilestore.ConvertedDocument.Metadata.RawFilePath]

			// delete the file from the map as we will use this map to delete extra files from the stage
			delete(chunksFilesInStage, chunksFileInFilestore.ConvertedDocument.Metadata.RawFilePath)

			if chunksFileInStage.ChunksDocument.Metadata.Equal(chunksFileInFilestore.ChunksDocument.Metadata) {
				logger.Info("file is already in the stage and the configuration is the same, skipping ...",
					"file", chunksFileInFilestore.ConvertedDocument.Metadata.RawFilePath)
				// nothing to do, file is already in the stage
				continue
			}
		}

		// upload the file to the stage

		// this is needed to pass the file stream to the snowflake client without creating a local temporary file
		streamCtx := gosnowflake.WithFileStream(ctx, bytes.NewReader(chunksFileBytesInFilestore))

		fileRows := []snowflake.UploadedFileStatus{}

		if err := d.Client.Put(streamCtx,
			d.Role,
			// this file path does not matter as we are using the stream context to pass the file stream to the snowflake client
			chunksFilePathInFilestore,
			// database name is the database name
			d.Database,
			// schema name is the data product name
			d.Schema,
			// stage name is the internal stage name
			d.Stage,
			// subpath is the file name
			chunksFilePathInFilestore,
			&fileRows); err != nil {
			logger.Error(err, "failed to upload file to snowflake internal stage", "file", chunksFilePathInFilestore)
			errorList = append(errorList, err)
			continue
		}
		logger.Info("successfully uploaded file to snowflake internal stage",
			"file", chunksFileInFilestore.ConvertedDocument.Metadata.RawFilePath)

		if len(fileRows) == 0 {
			logger.Error(fmt.Errorf("no file rows returned while uploading file to snowflake internal stage: %s",
				chunksFileInFilestore.ConvertedDocument.Metadata.RawFilePath),
				"file", chunksFileInFilestore.ConvertedDocument.Metadata.RawFilePath)
			errorList = append(errorList,
				fmt.Errorf("no file rows returned while uploading file to snowflake internal stage: %s",
					chunksFileInFilestore.ConvertedDocument.Metadata.RawFilePath))
			continue
		}
	}

	// delete extra files which are the files in the stage that are not present in the filestore
	// at this point, whatever is left in the filesInStage map are the extra files that need to be deleted

	extraFiles := []string{}
	for extraFilePath := range chunksFilesInStage {
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
		s3Client, err = awsclienthandler.GetS3Client()
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

		// Check if file exists and compare ETag (MD5) to skip unchanged files
		shouldUpload := true
		headResp, err := s3Client.HeadObject(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(d.Bucket),
			Key:    aws.String(key),
		})
		if err == nil && headResp.ETag != nil {
			// S3 ETag is MD5 hash wrapped in quotes for single-part uploads
			localMD5 := fmt.Sprintf("\"%x\"", md5.Sum(data))
			if localMD5 == *headResp.ETag {
				logger.Info("file unchanged, skipping upload",
					"file", chunksFilePath, "key", key)
				shouldUpload = false
			}
		}

		if shouldUpload {
			// File is new or changed, upload it
			_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(d.Bucket),
				Key:    aws.String(key),
				Body:   bytes.NewReader(data),
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
