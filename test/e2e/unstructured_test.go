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
	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
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
	"sigs.k8s.io/e2e-framework/support/utils"
)

func TestUnstructuredDataLoad(t *testing.T) {
	feature := features.New("Unstructured Data Load")

	unstructuredBucketName := "unstructured-bucket"
	unstructuredDataStorageBucketName := "data-storage-bucket"
	unstructuredQueueName := "unstructured-queue"

	operatorControllerConfig := operatorUtils.GetControllerConfigResource()
	databaseName := "testing_db"
	schemaName := "testingschema"
	// Must match spec.destinationConfig.snowflakeInternalStageConfig.stage in the sample YAML
	internalStageName := schemaName + "_internal_stg"
	dataProductCRName := schemaName

	queueURL := "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/" + unstructuredQueueName

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
			if err := kubeClient.Resources().Get(ctx, snowflakeSecretName, testNamespace, secret); err != nil {
				t.Fatalf("Failed to get secret: %s", err)
			}

			// create snowflake client from ControllerConfig spec (same account/user/role/warehouse as controller)
			sfClientConfig := operatorUtils.NewClientConfigFromSnowflakeSpec(&operatorControllerConfig.Spec.SnowflakeConfig, secret)
			if sfClientConfig == nil {
				t.Fatalf("Failed to build Snowflake client config from ControllerConfig and secret")
			}
			sfClient, err = snowflake.NewClient(ctx, sfClientConfig)
			if err != nil {
				t.Fatalf("Failed to create snowflake client: %s", err)
			}

			// create AWS clients
			s3Client, err := awsclienthandler.NewS3ClientFromConfig(ctx, &awsclienthandler.AWSConfig{
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
							Events:   []types.Event{types.EventS3ObjectCreated},
						},
					},
				},
			})
			if err != nil {
				t.Fatal(err)
			}

			// create SQSConsumer CR
			sqsConsumer := &v1alpha1.SQSConsumer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sqs-consumer",
					Namespace: testNamespace,
				},
				Spec: v1alpha1.SQSConsumerSpec{
					QueueURL: queueURL,
				},
			}
			err = kubeClient.Resources(testNamespace).Create(ctx, sqsConsumer)
			if err != nil {
				t.Fatal(err)
			}

			// wait for SQSConsumer CR to be ready
			waitCommand := fmt.Sprintf(
				"kubectl wait --for=condition=%s sqsconsumers.operator.dataverse.redhat.com/%s -n %s --timeout=10m",
				v1alpha1.SQSConsumerCondition,
				"test-sqs-consumer",
				testNamespace,
			)
			if p := utils.RunCommand(waitCommand); p.Err() != nil {
				t.Fatal(p.Err())
			}

			// create unstructured data product CR
			unstructuredDataProduct := operatorUtils.GetUnstructuredDataProductResource(dataProductCRName, testNamespace)

			t.Log("create unstructured dataproduct CR ...")
			if err := kubeClient.Resources(testNamespace).Create(ctx, &unstructuredDataProduct); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					t.Fatal(err)
				}
			}

			// wait for unstructured data product CR to be healthy
			t.Log("wait for unstructured data product CR to be healthy")
			sWaitCommand := fmt.Sprintf(
				"kubectl wait --for=condition=%s unstructureddataproducts.operator.dataverse.redhat.com/%s -n %s --timeout=10m", v1alpha1.UnstructuredDataProductCondition,
				dataProductCRName,
				testNamespace,
			)

			if p := utils.RunCommand(sWaitCommand); p.Err() != nil {
				t.Logf("Failed to meet condition for unstructured data product CR: %s\nOutput: %s", p.Err(), p.Stdout())
				t.Fatal(p.Err())
			}

			return ctx
		},
	)

	feature.Assess("upload files to unstructured bucket and verify they land up in snowflake stage and ingested to snowflake table", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		// create AWS clients for file operations
		s3Client, err := awsclienthandler.NewS3ClientFromConfig(ctx, &awsclienthandler.AWSConfig{
			Region:          "us-east-1",
			AccessKeyID:     "test",
			SecretAccessKey: "test",
			Endpoint:        localstackURL,
		})
		if err != nil {
			t.Error(err)
		}

		unstructuredFilesDirectory := "test/resources/unstructured/unstructured-files"

		// get all files in the directory
		files, err := os.ReadDir(unstructuredFilesDirectory)
		if err != nil {
			t.Error(err)
		}

		if len(files) == 0 {
			t.Error("no files found in the directory")
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
		s3Client, err := awsclienthandler.NewS3ClientFromConfig(ctx, &awsclienthandler.AWSConfig{
			Region:          "us-east-1",
			AccessKeyID:     "test",
			SecretAccessKey: "test",
			Endpoint:        localstackURL,
		})
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
			t.Errorf("Unable to list obejcts from the unstructured bucket: %s", err)
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

		// add a force reconcile label to the UnstructuredDataProduct CR
		t.Log("adding force reconcile label to the UnstructuredDataProduct CR")
		p := utils.RunCommand(fmt.Sprintf(`kubectl label unstructureddataproduct.operator.dataverse.redhat.com %s -n %s %s=true`, schemaName, testNamespace, controllerutils.ForceReconcileLabel))
		if p.Err() != nil {
			t.Errorf("Unable to add force reconcile label to the UnstructuredDataProduct CR: %s", p.Err())
		}

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

	feature.Assess("Multiple files upload testing", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		// create a new s3 client
		t.Log("Creating s3 client ...")
		s3Client, err := awsclienthandler.NewS3ClientFromConfig(ctx, &awsclienthandler.AWSConfig{
			Region:          "us-east-1",
			AccessKeyID:     "test",
			SecretAccessKey: "test",
			Endpoint:        localstackURL,
		})
		if err != nil {
			t.Error(err)
		}

		// list all the files in the unstructured-Bucket as we have ingested files in the last step
		t.Log("Listing objects from unstructured bucket ...")
		output, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(unstructuredBucketName),
			Prefix: aws.String(schemaName + "/"),
		})

		// take out all the files names from the output to the slice
		filesinBucket := []string{}
		for _, file := range output.Contents {
			filesinBucket = append(filesinBucket, *file.Key)
		}

		// list all the files in the snowflake internal stage
		type stageFileData struct {
			FileName string `db:"file_name"`
		}

		stageSqlQuery := fmt.Sprintf("select $1:convertedDocument.metadata.rawFilePath::string as \"file_name\" from @%s.%s.%s", databaseName, schemaName, internalStageName)
		stageFiles := []stageFileData{}
		if err := sfClient.QueryTableUsingRole(ctx, stageSqlQuery, operatorControllerConfig.Spec.SnowflakeConfig.Warehouse, operatorControllerConfig.Spec.SnowflakeConfig.Role, &stageFiles); err != nil {
			t.Error(err)
		}

		// compare the count of files in the bucket and the stage
		if len(filesinBucket) != len(stageFiles) {
			t.Errorf("count of files in the bucket and the stage do not match: %d != %d", len(filesinBucket), len(stageFiles))
		}

		t.Logf("Files are in sync even after deleting the file from the bucket")
		countofFilesAlreadyExists := len(filesinBucket)

		// Now read another directory and upload the files to the bucket
		newDirectory := "test/resources/unstructured/unstructured-files-2"
		files, err := os.ReadDir(newDirectory)
		if err != nil {
			t.Error(err)
		}

		if len(files) == 0 {
			t.Errorf("no files found in the directory: %s", newDirectory)
		}

		// take out all the file names from the files slice to the slice
		filesToUpload := []string{}
		for _, file := range files {
			filesToUpload = append(filesToUpload, file.Name())
		}

		// upload first 5 files to the bucket
		for i := 0; i < 5; i++ {
			file := filesToUpload[i]

			// read the file content
			fileContent, err := os.ReadFile(filepath.Join(newDirectory, file))
			if err != nil {
				t.Error(err)
			}

			// uploaad files in the unstructured bucket
			key := fmt.Sprintf("%s/%s", schemaName, file)
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

		// verify stage files
		stageFiles = []stageFileData{}
		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			5*time.Second,
			10*time.Minute,
			false,
			func(ctx context.Context) (done bool, err error) {
				filesInStage := []stageFileData{}
				if err = sfClient.QueryTableUsingRole(
					ctx,
					stageSqlQuery,
					operatorControllerConfig.Spec.SnowflakeConfig.Warehouse,
					operatorControllerConfig.Spec.SnowflakeConfig.Role,
					&filesInStage,
				); err != nil {
					t.Error(err)
				}
				if len(filesInStage) != countofFilesAlreadyExists+5 {
					t.Logf("expected %d files in stage, got %d, retrying ...", countofFilesAlreadyExists+5, len(filesInStage))
					return false, nil
				}
				// we're doing this circus to prevent global stageFiles from being appended over and over
				stageFiles = filesInStage
				return true, nil
			},
		); err != nil {
			t.Error(err)
		}

		t.Logf("stage files after uploading first 5 files: %+v", stageFiles)

		// make sure all the files are ingested (iterate over the filesToUpload slice and loop first 5 files)
		for i := 0; i < 5; i++ {
			file := filesToUpload[i]
			found := false
			expectedFilePath := fmt.Sprintf("%s/%s", schemaName, file)
			for _, ingestedFile := range stageFiles {
				if ingestedFile.FileName == expectedFilePath {
					t.Logf("file %s ingested successfully", file)
					found = true
					break
				}
			}
			if !found {
				t.Errorf("file %s not ingested", file)
			}
		}

		t.Logf("Perfectly processed 5 files from the new directory")

		// now wait for 10 seconds
		t.Log("waiting for 10 seconds, will upload more files...")
		time.Sleep(10 * time.Second)

		// now starting from the 6th file and upload to the unstructured bucket
		for i := 5; i < len(filesToUpload); i++ {
			file := filesToUpload[i]
			fileContent, err := os.ReadFile(filepath.Join(newDirectory, file))
			if err != nil {
				t.Error(err)
			}

			key := fmt.Sprintf("%s/%s", schemaName, file)
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

		// verify stage files
		stageFiles = []stageFileData{}
		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			time.Second*5,
			time.Minute*10,
			false,
			func(ctx context.Context) (done bool, err error) {
				filesInStage := []stageFileData{}
				if err = sfClient.QueryTableUsingRole(
					ctx,
					stageSqlQuery,
					operatorControllerConfig.Spec.SnowflakeConfig.Warehouse,
					operatorControllerConfig.Spec.SnowflakeConfig.Role,
					&filesInStage,
				); err != nil {
					t.Error(err)
				}
				if len(filesInStage) != countofFilesAlreadyExists+len(filesToUpload) {
					t.Logf("expected %d files in stage, got %d, retrying ...", countofFilesAlreadyExists+len(filesToUpload), len(filesInStage))
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

		// make sure all the files are ingested (iterate over the filesToUpload slice and loop from 6th file to the end)
		for i := 5; i < len(filesToUpload); i++ {
			file := filesToUpload[i]
			found := false
			expectedFilePath := fmt.Sprintf("%s/%s", schemaName, file)
			for _, ingestedFile := range stageFiles {
				if ingestedFile.FileName == expectedFilePath {
					t.Logf("file %s ingested successfully", file)
					found = true
					break
				}
			}
			if !found {
				t.Errorf("file %s not ingested", file)
			}
		}

		t.Logf("Perfectly processed all the files from the new directory")
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

		// fetch the latest version of the unstructured data product CR
		unstructuredDataProductCR := &v1alpha1.UnstructuredDataProduct{}
		if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, unstructuredDataProductCR); err != nil {
			t.Error(err)
		}
		unstructuredDataProductCR.Spec.DocumentProcessorConfig.DoclingConfig = *doclingConfig
		if err := kubeClient.Resources().WithNamespace(testNamespace).Update(ctx, unstructuredDataProductCR); err != nil {
			t.Error(err)
		}
		t.Log("Successfully updated the docling config in the unstructured data product CR")

		// wait until the unstructured data product CR is ready
		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			time.Second*5,
			time.Minute*10,
			false,
			func(ctx context.Context) (done bool, err error) {
				if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, unstructuredDataProductCR); err != nil {
					t.Error(err)
					return false, nil
				}
				if len(unstructuredDataProductCR.Status.Conditions) == 0 {
					return false, nil
				}
				if unstructuredDataProductCR.Status.Conditions[0].Status == metav1.ConditionTrue {
					return true, nil
				}
				t.Logf("UnstructuredDataProduct not ready: status=%s reason=%s message=%s", unstructuredDataProductCR.Status.Conditions[0].Status, unstructuredDataProductCR.Status.Conditions[0].Reason, unstructuredDataProductCR.Status.Conditions[0].Message)
				return false, nil
			},
		); err != nil {
			t.Error(err)
		}

		t.Log("UnstructuredDataProduct successfully reconciled")

		// wait for the document processor to be ready (increased timeout for reprocessing all 11 files)
		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			time.Second*5,
			time.Minute*20,
			false,
			func(ctx context.Context) (done bool, err error) {
				documentprocessorCR := &v1alpha1.DocumentProcessor{}
				if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, documentprocessorCR); err != nil {
					t.Error(err)
					return false, nil
				}
				if len(documentprocessorCR.Status.Conditions) == 0 {
					return false, nil
				}

				// wait for ready status and no pending jobs
				isReady := documentprocessorCR.Status.Conditions[0].Status == metav1.ConditionTrue
				noPendingJobs := len(documentprocessorCR.Status.Jobs) == 0

				if !noPendingJobs || !isReady {
					t.Logf("document processor is not ready or has pending jobs (jobs: %d, ready: %v)", len(documentprocessorCR.Status.Jobs), isReady)
					return false, nil
				}
				return true, nil
			},
		); err != nil {
			t.Error(err)
		}

		t.Log("Document Processor successfully reconciled, all the files are processed")

		// wait until the chunksgenerator CR is ready (as it will become ready after the final sync logic is executed)
		chunksgeneratorCR := &v1alpha1.ChunksGenerator{}
		if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, chunksgeneratorCR); err != nil {
			t.Error(err)
		}
		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			time.Second*5,
			time.Minute*20,
			false,
			func(ctx context.Context) (done bool, err error) {
				if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, chunksgeneratorCR); err != nil {
					t.Error(err)
				}
				if len(chunksgeneratorCR.Status.Conditions) == 0 {
					return false, nil
				}
				if chunksgeneratorCR.Status.Conditions[0].Status == metav1.ConditionTrue {
					return true, nil
				}
				t.Logf("chunksgenerator CR not ready: status=%s reason=%s message=%s", chunksgeneratorCR.Status.Conditions[0].Status, chunksgeneratorCR.Status.Conditions[0].Reason, chunksgeneratorCR.Status.Conditions[0].Message)
				return false, nil
			},
		); err != nil {
			t.Error(err)
		}
		t.Log("Successfully waited until the chunksgenerator CR is ready, now files are up to date in the internal stage")

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

		// fetch the latest version of the unstructured data product CR
		unstructuredDataProductCR := &v1alpha1.UnstructuredDataProduct{}
		if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, unstructuredDataProductCR); err != nil {
			t.Error(err)
		}
		unstructuredDataProductCR.Spec.ChunksGeneratorConfig = *chunkingConfig
		if err := kubeClient.Resources().WithNamespace(testNamespace).Update(ctx, unstructuredDataProductCR); err != nil {
			t.Error(err)
		}
		t.Log("Successfully updated the chunking config in the unstructured data product CR")

		// wait until the unstructured data product CR is ready
		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			time.Second*5,
			time.Minute*10,
			false,
			func(ctx context.Context) (done bool, err error) {
				if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, unstructuredDataProductCR); err != nil {
					t.Error(err)
					return false, nil
				}
				if len(unstructuredDataProductCR.Status.Conditions) == 0 {
					return false, nil
				}
				if unstructuredDataProductCR.Status.Conditions[0].Status == metav1.ConditionTrue {
					return true, nil
				}
				t.Logf("UnstructuredDataProduct not ready: status=%s reason=%s message=%s", unstructuredDataProductCR.Status.Conditions[0].Status, unstructuredDataProductCR.Status.Conditions[0].Reason, unstructuredDataProductCR.Status.Conditions[0].Message)
				return false, nil
			},
		); err != nil {
			t.Error(err)
		}

		t.Log("UnstructuredDataProduct successfully reconciled")

		// wait for the document processor to be ready (increased timeout for rechunking all 11 files)
		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			time.Second*5,
			time.Minute*20,
			false,
			func(ctx context.Context) (done bool, err error) {
				documentprocessorCR := &v1alpha1.DocumentProcessor{}
				if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, documentprocessorCR); err != nil {
					t.Error(err)
					return false, nil
				}
				if len(documentprocessorCR.Status.Conditions) == 0 {
					return false, nil
				}

				isReady := documentprocessorCR.Status.Conditions[0].Status == metav1.ConditionTrue
				noPendingJobs := len(documentprocessorCR.Status.Jobs) == 0

				if !noPendingJobs || !isReady {
					t.Logf("document processor is not ready or has pending jobs (jobs: %d, ready: %v)", len(documentprocessorCR.Status.Jobs), isReady)
					return false, nil
				}
				return true, nil
			},
		); err != nil {
			t.Error(err)
		}

		t.Log("Document Processor successfully reconciled, all the files are processed")

		chunksgeneratorCR := &v1alpha1.ChunksGenerator{}
		if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, chunksgeneratorCR); err != nil {
			t.Error(err)
		}
		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			time.Second*5,
			time.Minute*20,
			false,
			func(ctx context.Context) (done bool, err error) {
				if err := kubeClient.Resources().Get(ctx, dataProductCRName, testNamespace, chunksgeneratorCR); err != nil {
					t.Error(err)
				}
				if len(chunksgeneratorCR.Status.Conditions) == 0 {
					return false, nil
				}
				if chunksgeneratorCR.Status.Conditions[0].Status == metav1.ConditionTrue {
					return true, nil
				}
				t.Logf("chunksgenerator CR not ready: status=%s reason=%s message=%s", chunksgeneratorCR.Status.Conditions[0].Status, chunksgeneratorCR.Status.Conditions[0].Reason, chunksgeneratorCR.Status.Conditions[0].Message)
				return false, nil
			},
		); err != nil {
			t.Error(err)
		}
		t.Log("Successfully waited until the chunksgenerator CR is ready, now files are up to date in the internal stage")

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
			// delete SQSConsumer CR
			sqsConsumer := &v1alpha1.SQSConsumer{
				ObjectMeta: metav1.ObjectMeta{
					Name:      "test-sqs-consumer",
					Namespace: testNamespace,
				},
			}
			if err := kubeClient.Resources(testNamespace).Delete(ctx, sqsConsumer); err != nil {
				t.Fatal(err)
			}

			// delete unstructured data product CR
			unstructuredDataProduct := &v1alpha1.UnstructuredDataProduct{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dataProductCRName,
					Namespace: testNamespace,
				},
			}
			if err := kubeClient.Resources(testNamespace).Delete(ctx, unstructuredDataProduct); err != nil {
				t.Fatal(err)
			}

			// execute query to remove all the files from the stage
			removeFilesQuery := fmt.Sprintf("REMOVE '@%s.%s.%s'", databaseName, schemaName, internalStageName)
			if err := sfClient.ExecuteQueryWithRole(ctx, operatorControllerConfig.Spec.SnowflakeConfig.Warehouse, removeFilesQuery, operatorControllerConfig.Spec.SnowflakeConfig.Role); err != nil {
				t.Error(err)
			}

			return ctx
		},
	)

	testenv.Test(t, feature.Feature())
}
