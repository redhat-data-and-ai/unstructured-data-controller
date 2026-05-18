//go:build e2e
// +build e2e

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

package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
	operatorUtils "github.com/redhat-data-and-ai/unstructured-data-controller/test/utils"
	v1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerywait "k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestUnstructuredDataLoad(t *testing.T) {
	feature := features.New("Unstructured Data Load")

	// generate a unique string
	uniqueTestString := operatorUtils.RandomStringGenerator(10)
	unstructuredBucketName := "unstructured-bucket"
	unstructuredDataStorageBucketName := "data-storage-bucket"
	unstructuredQueueName := "unstructured-queue"

	operatorControllerConfig := operatorUtils.GetControllerConfigResource()
	databaseName := "unstructured_db"
	schemaName := "unstructured"
	internalStageName := fmt.Sprintf("%s_internal_stg_%s", schemaName, uniqueTestString)
	dataProductCRName := schemaName

	queueURL := "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/" + unstructuredQueueName
	unstructuredFilesDirectory := "test/resources/unstructured/unstructured-files"

	//clients
	secret := &v1.Secret{}
	sfClient := &snowflake.Client{}
	var kubeClient klient.Client

	feature.Setup(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			kubeClient = cfg.Client()

			err := v1alpha1.AddToScheme(kubeClient.Resources(testNamespace).GetScheme())
			if err != nil {
				t.Fatalf("Failed to add scheme: %s", err)
			}

			// get key secret
			if err := kubeClient.Resources().Get(ctx, unstructuredSecretName, testNamespace, secret); err != nil {
				t.Fatalf("Failed to get secret: %s", err)
			}

			// create snowflake client from ControllerConfig spec (same account/user/role/warehouse as controller)
			sfClientConfig := operatorUtils.NewClientConfigFromSnowflakeSpec(operatorControllerConfig.Spec.SnowflakeConfig, secret)
			if sfClientConfig == nil {
				t.Fatalf("Failed to build Snowflake client config from ControllerConfig and secret")
			}
			snowflake.SfConfig = sfClientConfig
			sfClient, err = snowflake.NewClient(ctx)
			if err != nil {
				t.Fatalf("Failed to create snowflake client: %s", err)
			}

			// create AWS clients
			err = awsclienthandler.NewSourceS3ClientFromConfig(ctx, &awsclienthandler.AWSConfig{
				Region:          "us-east-1",
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				Endpoint:        localstackURL,
			})
			if err != nil {
				t.Fatal(err)
			}

			// create SQS client
			sqsClient, err := awsclienthandler.NewSQSClientFromConfig(ctx, &awsclienthandler.AWSConfig{
				Region:          "us-east-1",
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				Endpoint:        localstackURL,
			})
			if err != nil {
				t.Fatal(err)
			}

			s3Client, err := awsclienthandler.GetSourceS3Client()
			if err != nil {
				t.Fatal(err)
			}
			// create unstructured bucket
			_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(unstructuredBucketName),
			})
			if err != nil {
				t.Fatal(err)
			}

			// create unstructured data storage bucket
			_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(unstructuredDataStorageBucketName),
			})
			if err != nil {
				t.Fatal(err)
			}

			// create SQS queue
			_, err = sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(unstructuredQueueName),
			})
			if err != nil {
				t.Fatal(err)
			}

			// create S3 --> SQS notification integration
			_, err = s3Client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
				Bucket: aws.String(unstructuredBucketName),
				NotificationConfiguration: &types.NotificationConfiguration{
					QueueConfigurations: []types.QueueConfiguration{
						{
							QueueArn: aws.String("arn:aws:sqs:us-east-1:000000000000:" + unstructuredQueueName),
							Events:   []types.Event{types.EventS3ObjectCreated, types.EventS3ObjectRemoved},
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			// create SQSInformer CR
			SQSInformer := &v1alpha1.SQSInformer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sqs-informer",
					Namespace: testNamespace,
				},
				Spec: v1alpha1.SQSInformerSpec{
					QueueURL: queueURL,
				},
			}
			err = kubeClient.Resources(testNamespace).Create(ctx, SQSInformer)
			if err != nil {
				t.Fatal(err)
			}

			// wait for SQSInformer CR to be ready
			if err := operatorUtils.WaitForResourceReady(v1alpha1.SQSInformerCondition, "SQSInformers.operator.dataverse.redhat.com", "test-sqs-informer", testNamespace); err != nil {
				t.Error(err)
			}

			// create internal stage in Snowflake with JSON file format
			if err := sfClient.CreateSnowflakeStage(ctx, operatorControllerConfig.Spec.SnowflakeConfig.Warehouse, operatorControllerConfig.Spec.SnowflakeConfig.Role, databaseName, schemaName, internalStageName); err != nil {
				t.Fatalf("Failed to create internal stage: %s", err)
			}
			t.Logf("created internal stage: %s", internalStageName)

			// register cleanup function to ensure stage is dropped even if test fails with t.Fatal()
			t.Cleanup(func() {
				if err := sfClient.DropSnowflakeStage(ctx, operatorControllerConfig.Spec.SnowflakeConfig.Warehouse, operatorControllerConfig.Spec.SnowflakeConfig.Role, databaseName, schemaName, internalStageName); err != nil {
					t.Logf("Warning: failed to drop stage in cleanup: %v", err)
				} else {
					t.Logf("Successfully dropped stage in cleanup: %s", internalStageName)
				}
			})
			// create unstructured data pipeline CR
			unstructuredDataPipeline := operatorUtils.GetUnstructuredDataPipelineResourceWithStage(dataProductCRName, testNamespace, internalStageName)
			t.Log("create unstructured datapipeline CR ...")
			if err := kubeClient.Resources(testNamespace).Create(ctx, &unstructuredDataPipeline); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					t.Fatal(err)
				}
			}

			// wait for unstructured data pipeline CR to be healthy
			t.Log("wait for unstructured data pipeline CR to be healthy")
			if err := operatorUtils.WaitForResourceReady(v1alpha1.UnstructuredDataPipelineCondition, "unstructureddatapipelines.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
				t.Error(err)
			}
			t.Log("unstructured data pipeline CR is healthy")

			return ctx
		},
	)

	feature.Assess("upload files to unstructured bucket and verify they land up in snowflake stage and ingested to snowflake table", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		// create AWS clients for file operations
		err := awsclienthandler.NewSourceS3ClientFromConfig(ctx, &awsclienthandler.AWSConfig{
			Region:          "us-east-1",
			AccessKeyID:     "test",
			SecretAccessKey: "test",
			Endpoint:        localstackURL,
		})
		if err != nil {
			t.Error(err)
		}

		// get all files in the directory
		files, err := os.ReadDir(unstructuredFilesDirectory)
		if err != nil {
			t.Error(err)
		}

		if len(files) == 0 {
			t.Error("no files found in the directory")
		}

		s3Client, err := awsclienthandler.GetSourceS3Client()
		if err != nil {
			t.Error(err)
		}

		// upload files to unstructured S3 bucket
		for _, file := range files {
			if file.IsDir() {
				t.Errorf("subdirectories are not allowed in the unstructured test files directory: %s", unstructuredFilesDirectory)
			}

			fileContent, err := os.ReadFile(filepath.Join(unstructuredFilesDirectory, file.Name()))
			if err != nil {
				t.Error(err)
			}

			key := fmt.Sprintf("%s/%s", schemaName, file.Name())
			_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(unstructuredBucketName),
				Key:    aws.String(key),
				Body:   bytes.NewReader(fileContent),
			})
			if err != nil {
				t.Error(err)
			}
			t.Logf("uploaded test file: %s", key)
		}

		// wait for files to be ingested
		t.Log("wait for files to be ingested ...")

		// verify stage files by querying for filenames from JSON content
		type stageFileData struct {
			FileName string `db:"file_name"`
		}
		stageFiles := []stageFileData{}
		stageFileQuery := fmt.Sprintf("SELECT $1:convertedDocument.metadata.rawFilePath::string AS \"file_name\" FROM @%s.%s.%s", databaseName, schemaName, internalStageName)

		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			5*time.Second,
			10*time.Minute,
			false,
			func(ctx context.Context) (done bool, err error) {
				filesInStage := []stageFileData{}
				if err = sfClient.QueryTableUsingRole(
					ctx,
					stageFileQuery,
					operatorControllerConfig.Spec.SnowflakeConfig.Warehouse,
					operatorControllerConfig.Spec.SnowflakeConfig.Role,
					&filesInStage,
				); err != nil {
					t.Error(err)
					return false, nil
				}
				if len(filesInStage) != len(files) {
					t.Logf("expected %d files in stage, got %d, retrying ...", len(files), len(filesInStage))
					return false, nil
				}
				// we're doing this circus to prevent global stageFiles from being appended over and over
				stageFiles = filesInStage
				return true, nil
			},
		); err != nil {
			t.Error(err)
		}

		t.Logf("stage files: %+v", stageFiles)

		// make sure all the files are ingested
		for _, file := range files {
			found := false
			expectedFilePath := fmt.Sprintf("%s/%s", schemaName, file.Name())
			for _, ingestedFile := range stageFiles {
				if ingestedFile.FileName == expectedFilePath {
					t.Logf("file %s ingested successfully", file.Name())
					found = true
					break
				}
			}
			if !found {
				t.Errorf("file %s not ingested (expected path: %s)", file.Name(), expectedFilePath)
			}
		}

		type data struct {
			FileName        string `db:"file_name"`
			MarkdownContent string `db:"markdown_content"`
		}

		t.Log("verifying data in stage ...")
		stageSqlQuery := fmt.Sprintf("select $1:convertedDocument.metadata.rawFilePath::string as \"file_name\", $1:convertedDocument.content.markdown::string as \"markdown_content\" from @%s.%s.%s", databaseName, schemaName, internalStageName)

		stageRows := []data{}
		if err := sfClient.QueryTableUsingRole(
			ctx,
			stageSqlQuery,
			operatorControllerConfig.Spec.SnowflakeConfig.Warehouse,
			operatorControllerConfig.Spec.SnowflakeConfig.Role,
			&stageRows,
		); err != nil {
			t.Error(err)
		}

		for _, row := range stageRows {
			mdContent := row.MarkdownContent
			fileName := row.FileName
			if len(mdContent) < 10 {
				t.Errorf("markdown content is almost empty for file %s", fileName)
			}
			// verify the ingested files rawFilePath matches the file name
			matched := false
			for _, file := range files {
				expectedFilePath := fmt.Sprintf("%s/%s", schemaName, file.Name())
				if expectedFilePath == fileName {
					matched = true
					break
				}
			}
			if !matched {
				t.Errorf("the ingested file rawFilePath %s does not match with any of the files in the directory", fileName)
			}

		}

		return ctx
	})

	feature.Assess("Deletion the file from the bucket and verifying in snowflake internal stage", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		// create a new s3 client
		t.Log("Creating s3 client ...")
		err := awsclienthandler.NewSourceS3ClientFromConfig(ctx, &awsclienthandler.AWSConfig{
			Region:          "us-east-1",
			AccessKeyID:     "test",
			SecretAccessKey: "test",
			Endpoint:        localstackURL,
		})
		if err != nil {
			t.Error(err)
		}

		s3Client, err := awsclienthandler.GetSourceS3Client()
		if err != nil {
			t.Error(err)
		}

		// list all the files in the unstructured-Bucket as we have ingested files in the last step
		t.Log("Listing objects from unstructured bucket ...")
		output, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(unstructuredBucketName),
			Prefix: aws.String(schemaName + "/"),
		})
		if err != nil {
			t.Errorf("Unable to list objects from the unstructured bucket: %s", err)
		}

		// Store the file name in a slice - filesinBucket
		filesinBucket := []string{}
		for _, file := range output.Contents {
			t.Logf("file: %s", *file.Key)
			filesinBucket = append(filesinBucket, *file.Key)
		}

		// verify the count is atleast 1
		if len(filesinBucket) == 0 {
			t.Error("Unable to list file from the bucket")
		}

		// store the file name at 0th index of the slice in a variable - fileToDelete
		fileToDelete := filesinBucket[0]

		// delete file from the bucket on the 0th index of the slice
		_, err = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(unstructuredBucketName),
			Key:    aws.String(fileToDelete),
		})
		if err != nil {
			t.Errorf("Unable to delete file from the bucket: %s", err)
		}

		t.Logf("deleted file: %s", fileToDelete)

		// delete the 0th index element from the slice as well
		filesinBucket = filesinBucket[1:]

		// wait for 10 seconds with the log message "waiting for 10 seconds"
		t.Log("waiting for 10 seconds")
		time.Sleep(10 * time.Second)

		// wait for files to be ingested
		t.Log("wait for files to be updated in the internal stage ...")

		// verify stage files by querying for filenames from JSON content
		type stageFileData struct {
			FileName string `db:"file_name"`
		}
		stageFiles := []stageFileData{}
		stageFileQuery := fmt.Sprintf("SELECT $1:convertedDocument.metadata.rawFilePath::string AS \"file_name\" FROM @%s.%s.%s", databaseName, schemaName, internalStageName)

		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			5*time.Second,
			10*time.Minute,
			false,
			func(ctx context.Context) (done bool, err error) {
				filesInStage := []stageFileData{}
				if err = sfClient.QueryTableUsingRole(
					ctx,
					stageFileQuery,
					operatorControllerConfig.Spec.SnowflakeConfig.Warehouse,
					operatorControllerConfig.Spec.SnowflakeConfig.Role,
					&filesInStage,
				); err != nil {
					t.Error(err)
					return false, nil
				}
				if len(filesInStage) != len(filesinBucket) {
					t.Logf("expected %d files in stage, got %d, retrying ...", len(filesinBucket), len(filesInStage))
					return false, nil
				}
				// we're doing this circus to prevent global stageFiles from being appended over and over
				stageFiles = filesInStage
				return true, nil
			},
		); err != nil {
			t.Error(err)
		}

		t.Logf("stage files: %+v", stageFiles)

		// Now iterate over the stageFiles and check if deleted file is still present
		for _, ingestedFile := range stageFiles {
			if strings.Contains(ingestedFile.FileName, fileToDelete) {
				t.Errorf("deleted file %s is still present in the internal stage", fileToDelete)
			}
		}

		t.Logf("deleted file %s is not present in the internal stage", fileToDelete)

		return ctx
	})

	feature.Assess("Will change docling config and verify the updation of files in the stage", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		// fetch the files from the snowflake internal stage
		stageSqlQuery := fmt.Sprintf("select $1:convertedDocument.metadata.rawFilePath::string as \"file_name\", $1:convertedDocument.metadata.doclingConfig::string as \"docling_config\", $1:convertedDocument.content.markdown::string as \"markdown_content\" from @%s.%s.%s", databaseName, schemaName, internalStageName)
		type data struct {
			FileName        string `db:"file_name"`
			DoclingConfig   string `db:"docling_config"`
			MarkDownContent string `db:"markdown_content"`
		}

		type RowData struct {
			FileName        string                `db:"file_name"`
			DoclingConfig   docling.DoclingConfig `db:"docling_config"`
			MarkDownContent string                `db:"markdown_content"`
		}

		rows := []data{}
		if err := sfClient.QueryTableUsingRole(ctx, stageSqlQuery, operatorControllerConfig.Spec.SnowflakeConfig.Warehouse, operatorControllerConfig.Spec.SnowflakeConfig.Role, &rows); err != nil {
			t.Error(err)
		}

		rowsData := []RowData{}
		for _, row := range rows {
			doclingConfig := docling.DoclingConfig{}
			if err := json.Unmarshal([]byte(row.DoclingConfig), &doclingConfig); err != nil {
				t.Error(err)
			}
			rowsData = append(rowsData, RowData{
				FileName:        row.FileName,
				DoclingConfig:   doclingConfig,
				MarkDownContent: row.MarkDownContent,
			})
		}

		t.Logf("len of rows: %d", len(rows))
		t.Log("Successfully fetched the row data from the snowflake internal stage")

		t.Log("Updating the docling config for the data product")
		doclingConfig := &v1alpha1.DoclingConfig{
			FromFormats:     []string{"pdf", "docx", "pptx", "xlsx"},
			ImageExportMode: "embedded",
			DoOCR:           true,
			ForceOCR:        false,
			OCREngine:       "easyocr",
			OCRLang:         []string{"en"},
			PDFBackend:      "dlparse_v4",
			TableMode:       "accurate",
		}

		// fetch the latest version of the unstructured data pipeline CR
		unstructuredDataPipelineCR := &v1alpha1.UnstructuredDataPipeline{}
		if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, unstructuredDataPipelineCR); err != nil {
			t.Error(err)
		}
		unstructuredDataPipelineCR.Spec.DocumentProcessorConfig.DoclingConfig = *doclingConfig
		if err := kubeClient.Resources().WithNamespace(testNamespace).Update(ctx, unstructuredDataPipelineCR); err != nil {
			t.Error(err)
		}
		t.Log("Successfully updated the docling config in the unstructured data pipeline CR")

		// wait for the unstructured data pipeline CR to be ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.UnstructuredDataPipelineCondition, "unstructureddatapipelines.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}

		t.Log("UnstructuredDataPipeline successfully reconciled")

		// wait for the document processor to be ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.DocumentProcessorCondition, "documentprocessors.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}

		t.Log("DocumentProcessor successfully reconciled")

		// wait until the chunksgenerator CR is ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.ChunksGeneratorCondition, "chunksgenerators.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("ChunksGenerator successfully reconciled")

		// wait for the vector embeddings generator to be ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.VectorEmbeddingGenerationConditionType, "vectorembeddingsgenerators.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("VectorEmbeddingsGenerator successfully reconciled")

		if err := operatorUtils.WaitForResourceReady(v1alpha1.UnstructuredDataPipelineCondition, "unstructureddatapipelines.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("UnstructuredDataPipeline successfully reconciled, now files are up to date in the internal stage")

		// now fetch the latest data file name , docling config and markdown content from the snowflake internal stage command
		stageRows := []data{}
		if err := sfClient.QueryTableUsingRole(ctx, stageSqlQuery, operatorControllerConfig.Spec.SnowflakeConfig.Warehouse, operatorControllerConfig.Spec.SnowflakeConfig.Role, &stageRows); err != nil {
			t.Error(err)
		}

		stageRowsData := []RowData{}
		for _, row := range stageRows {
			doclingConfig := docling.DoclingConfig{}
			if err := json.Unmarshal([]byte(row.DoclingConfig), &doclingConfig); err != nil {
				t.Error(err)
			}
			stageRowsData = append(stageRowsData, RowData{
				FileName:        row.FileName,
				DoclingConfig:   doclingConfig,
				MarkDownContent: row.MarkDownContent,
			})
		}

		t.Logf("len of stage rows: %d", len(stageRows))
		t.Log("Successfully fetched the latest data from the snowflake internal stage")

		// now iterate over the rows and for each row iterate over stage rows haiving same file name, check if docling config is updated or not
		for _, row := range rowsData {
			for _, stageRow := range stageRowsData {
				if row.FileName == stageRow.FileName {
					if reflect.DeepEqual(row.DoclingConfig, stageRow.DoclingConfig) {
						t.Errorf("docling config is not updated for the file: %s", row.FileName)
					} else {
						t.Logf("docling config is updated for the file: %s", row.FileName)
					}
				}
			}
		}

		t.Log("Successfully verified the updation of docling config for the files in the stage")

		return ctx
	})

	feature.Assess("Will change chunking config and verify the updation of files in the stage", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		// fetch the files from the snowflake internal stage
		stageSqlQuery := fmt.Sprintf("select $1:convertedDocument.metadata.rawFilePath::string as \"file_name\", $1:chunksDocument.metadata.chunksGeneratorConfig::string as \"chunking_config\" from @%s.%s.%s", databaseName, schemaName, internalStageName)
		type data struct {
			FileName       string `db:"file_name"`
			ChunkingConfig string `db:"chunking_config"`
		}

		type RowData struct {
			FileName       string                         `db:"file_name"`
			ChunkingConfig v1alpha1.ChunksGeneratorConfig `db:"chunking_config"`
		}

		rows := []data{}
		if err := sfClient.QueryTableUsingRole(ctx, stageSqlQuery, operatorControllerConfig.Spec.SnowflakeConfig.Warehouse, operatorControllerConfig.Spec.SnowflakeConfig.Role, &rows); err != nil {
			t.Error(err)
		}

		rowsData := []RowData{}
		for _, row := range rows {
			chunkingConfig := v1alpha1.ChunksGeneratorConfig{}
			if err := json.Unmarshal([]byte(row.ChunkingConfig), &chunkingConfig); err != nil {
				t.Error(err)
			}
			rowsData = append(rowsData, RowData{
				FileName:       row.FileName,
				ChunkingConfig: chunkingConfig,
			})
		}

		t.Logf("len of rows: %d", len(rows))
		t.Log("Successfully fetched the row data from the snowflake internal stage")

		// update the chunking config for the data product
		t.Log("Updating the chunking config for the data product")
		chunkingConfig := &v1alpha1.ChunksGeneratorConfig{
			Strategy: "markdownTextSplitter",
			MarkdownSplitterConfig: v1alpha1.MarkdownSplitterConfig{
				ChunkSize:        1500,
				ChunkOverlap:     300,
				CodeBlocks:       true,
				ReferenceLinks:   true,
				HeadingHierarchy: true,
				JoinTableRows:    true,
			},
		}

		// fetch the latest version of the unstructured data pipeline CR
		unstructuredDataPipelineCR := &v1alpha1.UnstructuredDataPipeline{}
		if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, unstructuredDataPipelineCR); err != nil {
			t.Error(err)
		}
		unstructuredDataPipelineCR.Spec.ChunksGeneratorConfig = *chunkingConfig
		if err := kubeClient.Resources().WithNamespace(testNamespace).Update(ctx, unstructuredDataPipelineCR); err != nil {
			t.Error(err)
		}
		t.Log("Successfully updated the chunking config in the unstructured data pipeline CR")

		// wait until the unstructured data pipeline CR is ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.UnstructuredDataPipelineCondition, "unstructureddatapipelines.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("UnstructuredDataPipeline successfully reconciled")

		// wait for the document processor to be ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.DocumentProcessorCondition, "documentprocessors.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("DocumentProcessor successfully reconciled")

		// wait for the chunks generator to be ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.ChunksGeneratorCondition, "chunksgenerators.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("ChunksGenerator successfully reconciled")

		// wait for the vector embeddings generator to be ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.VectorEmbeddingGenerationConditionType, "vectorembeddingsgenerators.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("VectorEmbeddingsGenerator successfully reconciled")

		// now fetch unstructured data pipeline CR and wait until it is ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.UnstructuredDataPipelineCondition, "unstructureddatapipelines.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("UnstructuredDataPipeline successfully reconciled, now files are up to date in the internal stage")

		// fetch the latest data from the snowflake internal stage after all the files has been propcessed and check if chunking config is updated or not
		stageRows := []data{}
		if err := sfClient.QueryTableUsingRole(ctx, stageSqlQuery, operatorControllerConfig.Spec.SnowflakeConfig.Warehouse, operatorControllerConfig.Spec.SnowflakeConfig.Role, &stageRows); err != nil {
			t.Error(err)
		}

		stageRowsData := []RowData{}
		for _, row := range stageRows {
			chunkingConfig := v1alpha1.ChunksGeneratorConfig{}
			if err := json.Unmarshal([]byte(row.ChunkingConfig), &chunkingConfig); err != nil {
				t.Error(err)
			}
			stageRowsData = append(stageRowsData, RowData{
				FileName:       row.FileName,
				ChunkingConfig: chunkingConfig,
			})
		}

		t.Logf("len of stage rows: %d", len(stageRows))
		t.Log("Successfully fetched the latest data from the snowflake internal stage")

		// now iterate over files and check if chunking config is updated or not
		for _, row := range rowsData {
			for _, stageRow := range stageRowsData {
				if row.FileName == stageRow.FileName {
					if reflect.DeepEqual(row.ChunkingConfig, stageRow.ChunkingConfig) {
						t.Errorf("chunking config is not updated for the file: %s", row.FileName)
					} else {
						t.Logf("chunking config is updated for the file: %s", row.FileName)
					}
				}
			}
		}

		t.Log("Successfully verified the updation of chunking config for the files in the stage")

		return ctx
	})

	feature.Teardown(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// delete SQSInformer CR
			SQSInformer := &v1alpha1.SQSInformer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sqs-informer",
					Namespace: testNamespace,
				},
			}
			if err := kubeClient.Resources(testNamespace).Delete(ctx, SQSInformer); err != nil {
				t.Fatal(err)
			}

			// delete unstructured data pipeline CR
			unstructuredDataPipeline := &v1alpha1.UnstructuredDataPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dataProductCRName,
					Namespace: testNamespace,
				},
			}
			if err := kubeClient.Resources(testNamespace).Delete(ctx, unstructuredDataPipeline); err != nil {
				t.Fatal(err)
			}
			return ctx
		},
	)

	testenv.Test(t, feature.Feature())
}

func TestS3Destination(t *testing.T) {
	feature := features.New("S3 Destination E2E")

	sourceBucketName := "unstructured-bucket"
	dataStorageBucket := "data-storage-bucket"
	destinationBucketName := "result-bucket"
	queueName := "s3-destination-queue"
	dataProductCRName := "testunstructureddataproduct"
	sourcePrefix := "testunstructureddataproduct/source-data"
	destinationPrefix := "testunstructureddataproduct/processed-data"

	var queueURL string
	testFilesDirectory := "test/resources/unstructured/unstructured-files"

	var kubeClient klient.Client

	feature.Setup(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			kubeClient = cfg.Client()

			err := v1alpha1.AddToScheme(kubeClient.Resources(testNamespace).GetScheme())
			if err != nil {
				t.Fatalf("Failed to add scheme: %s", err)
			}

			// create AWS clients
			err = awsclienthandler.NewSourceS3ClientFromConfig(ctx, &awsclienthandler.AWSConfig{
				Region:          "us-east-1",
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				Endpoint:        localstackURL,
			})
			if err != nil {
				t.Fatal(err)
			}

			// create destination S3 client
			err = awsclienthandler.NewDestinationS3ClientFromConfig(ctx, &awsclienthandler.AWSConfig{
				Region:          "us-east-1",
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				Endpoint:        localstackURL,
			})
			if err != nil {
				t.Fatal(err)
			}

			// create SQS client
			sqsClient, err := awsclienthandler.NewSQSClientFromConfig(ctx, &awsclienthandler.AWSConfig{
				Region:          "us-east-1",
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				Endpoint:        localstackURL,
			})
			if err != nil {
				t.Fatal(err)
			}

			s3Client, err := awsclienthandler.GetSourceS3Client()
			if err != nil {
				t.Fatal(err)
			}

			destS3Client, err := awsclienthandler.GetDestinationS3Client()
			if err != nil {
				t.Fatal(err)
			}

			// create source bucket
			_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(sourceBucketName),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("Created source bucket: %s", sourceBucketName)

			// create data storage bucket
			_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(dataStorageBucket),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("Created data storage bucket: %s", dataStorageBucket)

			// create destination bucket
			_, err = destS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(destinationBucketName),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("Created destination bucket: %s", destinationBucketName)

			// create SQS queue
			createQueueOutput, err := sqsClient.CreateQueue(ctx, &sqs.CreateQueueInput{
				QueueName: aws.String(queueName),
			})
			if err != nil {
				t.Fatal(err)
			}
			queueURL = *createQueueOutput.QueueUrl
			t.Logf("Created SQS queue: %s", queueName)

			// create S3 --> SQS notification integration
			_, err = s3Client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
				Bucket: aws.String(sourceBucketName),
				NotificationConfiguration: &types.NotificationConfiguration{
					QueueConfigurations: []types.QueueConfiguration{
						{
							QueueArn: aws.String("arn:aws:sqs:us-east-1:000000000000:" + queueName),
							Events:   []types.Event{types.EventS3ObjectCreated, types.EventS3ObjectRemoved},
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			// create SQSInformer CR
			SQSInformer := &v1alpha1.SQSInformer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-s3-destination-sqs-informer",
					Namespace: testNamespace,
				},
				Spec: v1alpha1.SQSInformerSpec{
					QueueURL: queueURL,
				},
			}
			err = kubeClient.Resources(testNamespace).Create(ctx, SQSInformer)
			if err != nil {
				t.Fatal(err)
			}

			// wait for SQSInformer CR to be ready
			if err := operatorUtils.WaitForResourceReady(v1alpha1.SQSInformerCondition, "SQSInformers.operator.dataverse.redhat.com", "test-s3-destination-sqs-informer", testNamespace); err != nil {
				t.Fatal(err)
			}

			// create unstructured data product CR with S3 destination
			unstructuredDataProduct := operatorUtils.GetUnstructuredDataProductResource(dataProductCRName, testNamespace)

			// override source config
			unstructuredDataProduct.Spec.SourceConfig = v1alpha1.SourceConfig{
				Type: v1alpha1.TypeS3,
				S3Config: v1alpha1.S3Config{
					Bucket: sourceBucketName,
					Prefix: sourcePrefix,
				},
			}

			// override destination config to use S3 instead of Snowflake
			unstructuredDataProduct.Spec.DestinationConfig = v1alpha1.DestinationConfig{
				Type: v1alpha1.TypeS3,
				S3DestinationConfig: v1alpha1.S3Config{
					Bucket: destinationBucketName,
					Prefix: destinationPrefix,
				},
			}

			t.Log("Creating unstructured dataproduct CR with S3 destination...")
			if err := kubeClient.Resources(testNamespace).Create(ctx, &unstructuredDataProduct); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					t.Fatal(err)
				}
			}

			// wait for unstructured data product CR to be healthy
			t.Log("Waiting for unstructured data product CR to be healthy")
			if err := operatorUtils.WaitForResourceReady(v1alpha1.UnstructuredDataProductCondition, "unstructureddataproducts.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
				t.Error(err)
			}
			t.Log("Unstructured data product CR is healthy")

			return ctx
		},
	)

	feature.Assess("upload files to source bucket and verify embeddings in S3 destination", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		// get all files in the directory
		files, err := os.ReadDir(testFilesDirectory)
		if err != nil {
			t.Error(err)
		}

		if len(files) == 0 {
			t.Error("no files found in the directory")
		}

		s3Client, err := awsclienthandler.GetSourceS3Client()
		if err != nil {
			t.Error(err)
		}

		// upload files to source S3 bucket
		for _, file := range files {
			if file.IsDir() {
				t.Errorf("subdirectories are not allowed in the test files directory: %s", testFilesDirectory)
			}

			fileContent, err := os.ReadFile(filepath.Join(testFilesDirectory, file.Name()))
			if err != nil {
				t.Error(err)
			}

			key := fmt.Sprintf("%s/%s", sourcePrefix, file.Name())
			_, err = s3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(sourceBucketName),
				Key:    aws.String(key),
				Body:   bytes.NewReader(fileContent),
			})
			if err != nil {
				t.Error(err)
			}
			t.Logf("Uploaded test file: %s", key)
		}

		// wait for all controllers to be ready
		t.Log("Waiting for all controllers to be ready...")

		// wait for DocumentProcessor to be ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.DocumentProcessorCondition, "documentprocessors.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("DocumentProcessor is ready")

		// wait for ChunksGenerator to be ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.ChunksGeneratorCondition, "chunksgenerators.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("ChunksGenerator is ready")

		// wait for VectorEmbeddingsGenerator to be ready
		if err := operatorUtils.WaitForResourceReady(v1alpha1.VectorEmbeddingGenerationConditionType, "vectorembeddingsgenerators.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("VectorEmbeddingsGenerator is ready")

		// wait for UnstructuredDataProduct to be ready (final check)
		if err := operatorUtils.WaitForResourceReady(v1alpha1.UnstructuredDataProductCondition, "unstructureddataproducts.operator.dataverse.redhat.com", dataProductCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("UnstructuredDataProduct is ready - all files should be processed")

		// verify embeddings files exist in destination bucket
		t.Log("Verifying embeddings files in destination S3 bucket...")

		destS3Client, err := awsclienthandler.GetDestinationS3Client()
		if err != nil {
			t.Error(err)
		}

		// poll until embeddings files are found in destination bucket
		if err := apimachinerywait.PollUntilContextTimeout(
			ctx,
			5*time.Second,
			10*time.Minute,
			false,
			func(ctx context.Context) (done bool, err error) {
				// list objects in destination bucket
				output, err := destS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
					Bucket: aws.String(destinationBucketName),
					Prefix: aws.String(destinationPrefix),
				})
				if err != nil {
					t.Logf("Error listing objects in destination bucket: %s, retrying...", err)
					return false, nil
				}

				if len(output.Contents) == 0 {
					t.Logf("No files found in destination bucket yet, retrying...")
					return false, nil
				}

				// look for embeddings files (should have .json extension and contain embeddings)
				foundCount := 0
				for _, obj := range output.Contents {
					key := *obj.Key
					// embeddings files typically end with .json
					if strings.HasSuffix(key, ".json") {
						t.Logf("Found embeddings file: %s", key)
						foundCount++
					}
				}

				// we expect exactly as many embeddings files as input files
				if foundCount == len(files) {
					t.Logf("Found %d embeddings files in destination bucket", foundCount)
					return true, nil
				}

				t.Logf("Found %d embeddings files, expected %d, retrying...", foundCount, len(files))
				return false, nil
			},
		); err != nil {
			t.Fatal("Timeout waiting for embeddings files in destination bucket")
		}

		t.Log("Successfully verified embeddings files in S3 destination")

		return ctx
	})

	feature.Teardown(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// delete SQSInformer CR
			SQSInformer := &v1alpha1.SQSInformer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-s3-destination-sqs-informer",
					Namespace: testNamespace,
				},
			}
			if err := kubeClient.Resources(testNamespace).Delete(ctx, SQSInformer); err != nil {
				t.Logf("Warning: Failed to delete SQSInformer: %s", err)
			}

			// delete unstructured data product CR
			unstructuredDataProduct := &v1alpha1.UnstructuredDataProduct{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dataProductCRName,
					Namespace: testNamespace,
				},
			}
			if err := kubeClient.Resources(testNamespace).Delete(ctx, unstructuredDataProduct); err != nil {
				t.Logf("Warning: Failed to delete UnstructuredDataProduct: %s", err)
			}

			// clean up S3 buckets
			s3Client, err := awsclienthandler.GetSourceS3Client()
			if err != nil {
				t.Logf("Warning: Failed to get S3 client for cleanup: %s", err)
				return ctx
			}

			destS3Client, err := awsclienthandler.GetDestinationS3Client()
			if err != nil {
				t.Logf("Warning: Failed to get destination S3 client for cleanup: %s", err)
			}

			// delete all objects in source bucket
			sourceOutput, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(sourceBucketName),
			})
			if err == nil {
				for _, obj := range sourceOutput.Contents {
					_, _ = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
						Bucket: aws.String(sourceBucketName),
						Key:    obj.Key,
					})
				}
			}

			// delete all objects in data storage bucket
			dataStorageOutput, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(dataStorageBucket),
			})
			if err == nil {
				for _, obj := range dataStorageOutput.Contents {
					_, _ = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
						Bucket: aws.String(dataStorageBucket),
						Key:    obj.Key,
					})
				}
			}

			// delete all objects in destination bucket
			if destS3Client != nil {
				destOutput, err := destS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
					Bucket: aws.String(destinationBucketName),
				})
				if err == nil {
					for _, obj := range destOutput.Contents {
						_, _ = destS3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
							Bucket: aws.String(destinationBucketName),
							Key:    obj.Key,
						})
					}
				}
			}

			// delete buckets
			_, _ = s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(sourceBucketName),
			})
			_, _ = s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(dataStorageBucket),
			})
			if destS3Client != nil {
				_, _ = destS3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
					Bucket: aws.String(destinationBucketName),
				})
			}

			// clean up SQS queue
			sqsClient, err := awsclienthandler.NewSQSClientFromConfig(ctx, &awsclienthandler.AWSConfig{
				Region:          "us-east-1",
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				Endpoint:        localstackURL,
			})
			if err == nil {
				_, _ = sqsClient.DeleteQueue(ctx, &sqs.DeleteQueueInput{
					QueueUrl: aws.String(queueURL),
				})
			}

			t.Log("Cleanup completed")

			return ctx
		},
	)

	testenv.Test(t, feature.Feature())
}
