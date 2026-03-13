package unstructured

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

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
		return err
	}

	// map of raw file path to embeddings file
	embeddingsFilesInStage := make(map[string]EmbeddingsFile)
	embeddingsFilesList := []string{}
	for _, row := range rows {
		embeddingsFile := EmbeddingsFile{}
		err := json.Unmarshal([]byte(row.Data), &embeddingsFile)
		if err != nil {
			return err
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
				"file", embeddingsFileInFilestore.ConvertedDocument.Metadata.RawFilePath)
			errorList = append(errorList,
				fmt.Errorf("no file rows returned while uploading file to snowflake internal stage: %s",
					embeddingsFileInFilestore.ConvertedDocument.Metadata.RawFilePath))
			continue
		}
	}

	// delete extra files which are the files in the stage that are not present in the filestore
	// at this point, whatever is left in the filesInStage map are the extra files that need to be deleted

	extraFiles := []string{}
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
