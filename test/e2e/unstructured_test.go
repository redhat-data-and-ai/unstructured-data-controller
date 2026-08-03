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
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	operatorUtils "github.com/redhat-data-and-ai/unstructured-data-controller/test/utils"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	apimachinerywait "k8s.io/apimachinery/pkg/util/wait"
	"sigs.k8s.io/e2e-framework/klient"
	"sigs.k8s.io/e2e-framework/pkg/envconf"
	"sigs.k8s.io/e2e-framework/pkg/features"
)

func TestUnstructuredDataLoad(t *testing.T) {
	feature := features.New("Unstructured Data Load")

	unstructuredBucketName := "unstructured-bucket"
	unstructuredDataStorageBucketName := "data-storage-bucket"
	outputBucketName := "output-bucket"
	unstructuredQueueName := "unstructured-queue"

	dataPipelineCRName := "unstructured"

	queueURL := "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/" + unstructuredQueueName
	unstructuredFilesDirectory := "test/resources/unstructured/unstructured-files"
	destinationPrefix := dataPipelineCRName + "/processed-data"

	var kubeClient klient.Client
	var sourceS3Client *s3.Client
	var destS3Client *s3.Client
	var sqsClient *sqs.Client

	feature.Setup(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			kubeClient = cfg.Client()

			err := v1alpha1.AddToScheme(kubeClient.Resources(testNamespace).GetScheme())
			if err != nil {
				t.Fatalf("Failed to add scheme: %s", err)
			}

			// create AWS clients
			e2eAWS := &awsclienthandler.AWSConfig{
				Region:          "us-east-1",
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				Endpoint:        localstackURL,
			}

			err = awsclienthandler.NewSourceS3ClientFromConfig(ctx, e2eAWS)
			if err != nil {
				t.Fatal(err)
			}

			err = awsclienthandler.NewDestinationS3ClientFromConfig(ctx, e2eAWS)
			if err != nil {
				t.Fatal(err)
			}

			sqsClient, err = awsclienthandler.NewSQSClientFromConfig(ctx, e2eAWS)
			if err != nil {
				t.Fatal(err)
			}

			sourceS3Client, err = awsclienthandler.GetSourceS3Client()
			if err != nil {
				t.Fatal(err)
			}

			destS3Client, err = awsclienthandler.GetDestinationS3Client()
			if err != nil {
				t.Fatal(err)
			}

			// create source bucket
			_, err = sourceS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(unstructuredBucketName),
			})
			if err != nil {
				t.Fatal(err)
			}

			// create data storage bucket
			_, err = sourceS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(unstructuredDataStorageBucketName),
			})
			if err != nil {
				t.Fatal(err)
			}

			// create output bucket
			_, err = sourceS3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(outputBucketName),
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
			_, err = sourceS3Client.PutBucketNotificationConfiguration(ctx, &s3.PutBucketNotificationConfigurationInput{
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

			// create pipeline CR with SQS queue URL
			unstructuredDataPipeline := operatorUtils.GetUnstructuredDataPipelineResourceWithStage(dataPipelineCRName, testNamespace)
			unstructuredDataPipeline.Spec.SecretRef = "pipeline-secret"
			unstructuredDataPipeline.Spec.Stages[0].SourceCrawlerConfig.S3Config.SQSQueueURL = queueURL
			t.Log("create unstructured datapipeline CR ...")
			if err := kubeClient.Resources(testNamespace).Create(ctx, &unstructuredDataPipeline); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					t.Fatal(err)
				}
			}

			// wait for unstructured data pipeline CR to be healthy
			t.Log("wait for unstructured data pipeline CR to be healthy")
			if err := operatorUtils.WaitForResourceReady(ctx, v1alpha1.UnstructuredDataPipelineCondition, "unstructureddatapipelines.operator.dataverse.redhat.com", dataPipelineCRName, testNamespace); err != nil {
				t.Error(err)
			}
			t.Log("unstructured data pipeline CR is healthy")

			return ctx
		},
	)

	feature.Assess("Will upload files and verify they are processed through the pipeline", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
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

			key := fmt.Sprintf("%s/%s", dataPipelineCRName, file.Name())
			_, err = sourceS3Client.PutObject(ctx, &s3.PutObjectInput{
				Bucket: aws.String(unstructuredBucketName),
				Key:    aws.String(key),
				Body:   bytes.NewReader(fileContent),
			})
			if err != nil {
				t.Error(err)
			}
			t.Logf("uploaded test file: %s", key)
		}

		// poll until files appear in the output bucket — the full pipeline
		// (crawl → docling → chunk → embed → sync) can take 15+ minutes on CI
		t.Log("waiting for files to be processed through the pipeline ...")
		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			10*time.Second,
			30*time.Minute,
			false,
			func(ctx context.Context) (done bool, err error) {
				// check intermediate progress in data-storage bucket
				storageOutput, _ := sourceS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
					Bucket: aws.String(unstructuredDataStorageBucketName),
					Prefix: aws.String("pipelines/" + dataPipelineCRName + "/"),
				})
				storageCount := 0
				if storageOutput != nil {
					storageCount = len(storageOutput.Contents)
				}

				output, listErr := sourceS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
					Bucket: aws.String(outputBucketName),
				})
				if listErr != nil {
					t.Logf("failed to list objects in output bucket: %v", listErr)
					return false, nil
				}
				if len(output.Contents) == 0 {
					t.Logf("pipeline in progress: %d intermediate files, 0 output files", storageCount)
					if storageOutput != nil {
						for _, obj := range storageOutput.Contents {
							t.Logf("  intermediate file: %s", *obj.Key)
						}
					}

					// log DocumentProcessor CR status
					dpCR := &v1alpha1.DocumentProcessor{}
					if getErr := kubeClient.Resources(testNamespace).Get(ctx, dataPipelineCRName+"-convert", testNamespace, dpCR); getErr == nil {
						for _, cond := range dpCR.Status.Conditions {
							t.Logf("  DocumentProcessor condition: type=%s status=%s reason=%s message=%s", cond.Type, cond.Status, cond.Reason, cond.Message)
						}
						t.Logf("  DocumentProcessor jobs: %d, filesProcessed: %d", len(dpCR.Status.Jobs), dpCR.Status.FilesProcessed)
						for _, job := range dpCR.Status.Jobs {
							t.Logf("    job: file=%s taskID=%s status=%s attempts=%d", job.FilePath, job.TaskID, job.Status, job.Attempts)
						}
					}

					return false, nil
				}
				t.Logf("found %d files in output bucket", len(output.Contents))
				return true, nil
			},
		); err != nil {
			// dump controller and docling-serve logs to help debug CI failures
			dumpPodLogs(t, testNamespace, "control-plane=controller-manager", "manager")
			dumpPodLogs(t, testNamespace, "app=docling-serve", "api")
			dumpPodEvents(t, testNamespace, "app=docling-serve")
			t.Error(err)
		}

		return ctx
	})

	feature.Assess("Will delete a file from source and verify it is removed", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		// list all the files in the source bucket
		t.Log("Listing objects from unstructured bucket ...")
		output, err := sourceS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(unstructuredBucketName),
			Prefix: aws.String(dataPipelineCRName + "/"),
		})
		if err != nil {
			t.Errorf("Unable to list objects from the unstructured bucket: %s", err)
		}

		filesInBucket := []string{}
		for _, file := range output.Contents {
			t.Logf("file: %s", *file.Key)
			filesInBucket = append(filesInBucket, *file.Key)
		}

		if len(filesInBucket) == 0 {
			t.Error("Unable to list file from the bucket")
		}

		fileToDelete := filesInBucket[0]

		// delete file from the bucket
		_, err = sourceS3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
			Bucket: aws.String(unstructuredBucketName),
			Key:    aws.String(fileToDelete),
		})
		if err != nil {
			t.Errorf("Unable to delete file from the bucket: %s", err)
		}

		t.Logf("deleted file: %s", fileToDelete)
		remainingFiles := filesInBucket[1:]

		// wait for the source crawler to pick up the deletion
		t.Log("waiting for source crawler to reconcile after deletion ...")
		if err := operatorUtils.WaitForResourceReady(ctx, v1alpha1.SourceCrawlerCondition, "sourcecrawlers.operator.dataverse.redhat.com", dataPipelineCRName+"-crawl", testNamespace); err != nil {
			t.Error(err)
		}

		// verify the data storage bucket no longer has the deleted file
		t.Log("verifying deleted file is removed from data storage ...")
		if err := apimachinerywait.PollUntilContextTimeout(
			context.Background(),
			5*time.Second,
			5*time.Minute,
			false,
			func(ctx context.Context) (done bool, err error) {
				storageOutput, listErr := sourceS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
					Bucket: aws.String(unstructuredDataStorageBucketName),
					Prefix: aws.String("pipelines/" + dataPipelineCRName + "/stages/crawl/"),
				})
				if listErr != nil {
					t.Logf("failed to list objects: %v", listErr)
					return false, nil
				}
				for _, obj := range storageOutput.Contents {
					baseName := filepath.Base(*obj.Key)
					deletedBaseName := filepath.Base(fileToDelete)
					if strings.Contains(baseName, deletedBaseName) {
						t.Logf("deleted file still present: %s, retrying ...", *obj.Key)
						return false, nil
					}
				}
				t.Logf("deleted file removed, %d remaining files in source", len(remainingFiles))
				return true, nil
			},
		); err != nil {
			t.Error(err)
		}

		return ctx
	})

	feature.Assess("Will change docling config and verify re-processing", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		doclingConfig := &v1alpha1.DoclingConfig{
			FromFormats:     []string{"pdf", "docx", "pptx", "xlsx"},
			ImageExportMode: "embedded",
			OCRPreset:       "auto",
			OCRLang:         []string{"en"},
			PDFBackend:      "docling_parse",
			Pipeline:        "standard",
			TableMode:       "fast",
		}

		// fetch the latest version of the pipeline CR
		unstructuredDataPipelineCR := &v1alpha1.UnstructuredDataPipeline{}
		if err := kubeClient.Resources().Get(ctx, dataPipelineCRName, testNamespace, unstructuredDataPipelineCR); err != nil {
			t.Error(err)
		}

		// update the document processor stage config
		for i, stage := range unstructuredDataPipelineCR.Spec.Stages {
			if stage.Type == v1alpha1.StageTypeDocumentProcessor {
				unstructuredDataPipelineCR.Spec.Stages[i].DocumentProcessorConfig.DoclingConfig = *doclingConfig
				break
			}
		}
		if err := kubeClient.Resources().WithNamespace(testNamespace).Update(ctx, unstructuredDataPipelineCR); err != nil {
			t.Error(err)
		}
		t.Log("successfully updated the docling config in the pipeline CR")

		// wait for pipeline and downstream stages to re-reconcile
		if err := operatorUtils.WaitForResourceReady(ctx, v1alpha1.UnstructuredDataPipelineCondition, "unstructureddatapipelines.operator.dataverse.redhat.com", dataPipelineCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("pipeline successfully reconciled after docling config change")

		// verify the config change was propagated to the child CR
		dpCR := &v1alpha1.DocumentProcessor{}
		if err := kubeClient.Resources(testNamespace).Get(
			ctx, dataPipelineCRName+"-convert", testNamespace, dpCR,
		); err != nil {
			t.Error(err)
		}
		if dpCR.Spec.DocumentProcessorConfig.DoclingConfig.TableMode != "fast" {
			t.Errorf("expected table_mode fast, got %s",
				dpCR.Spec.DocumentProcessorConfig.DoclingConfig.TableMode)
		}
		t.Log("DocumentProcessor config change propagated successfully")

		return ctx
	})

	feature.Assess("Will change chunking config and verify re-processing", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		chunkingConfig := &v1alpha1.ChunksGeneratorConfig{
			Strategy: v1alpha1.ChunkingStrategyMarkdown,
			MarkdownSplitterConfig: v1alpha1.MarkdownSplitterConfig{
				ChunkSize:        1500,
				ChunkOverlap:     300,
				CodeBlocks:       true,
				ReferenceLinks:   true,
				HeadingHierarchy: true,
				JoinTableRows:    true,
			},
		}

		// fetch the latest version of the pipeline CR
		unstructuredDataPipelineCR := &v1alpha1.UnstructuredDataPipeline{}
		if err := kubeClient.Resources().Get(ctx, dataPipelineCRName, testNamespace, unstructuredDataPipelineCR); err != nil {
			t.Error(err)
		}

		// update the chunks generator stage config
		for i, stage := range unstructuredDataPipelineCR.Spec.Stages {
			if stage.Type == v1alpha1.StageTypeChunksGenerator {
				unstructuredDataPipelineCR.Spec.Stages[i].ChunksGeneratorConfig = chunkingConfig
				break
			}
		}
		if err := kubeClient.Resources().WithNamespace(testNamespace).Update(ctx, unstructuredDataPipelineCR); err != nil {
			t.Error(err)
		}
		t.Log("successfully updated the chunking config in the pipeline CR")

		// wait for pipeline and downstream stages to re-reconcile
		if err := operatorUtils.WaitForResourceReady(ctx, v1alpha1.UnstructuredDataPipelineCondition, "unstructureddatapipelines.operator.dataverse.redhat.com", dataPipelineCRName, testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("pipeline successfully reconciled after chunking config change")

		if err := operatorUtils.WaitForResourceReady(ctx, v1alpha1.ChunksGeneratorCondition, "chunksgenerators.operator.dataverse.redhat.com", dataPipelineCRName+"-chunk", testNamespace); err != nil {
			t.Error(err)
		}
		t.Log("ChunksGenerator successfully reconciled after config change")

		return ctx
	})

	feature.Assess("patch pipeline destination to S3 and verify embeddings in result bucket", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		unstructuredDataPipelineCR := &v1alpha1.UnstructuredDataPipeline{}
		if err := kubeClient.Resources(testNamespace).Get(ctx, dataPipelineCRName, testNamespace, unstructuredDataPipelineCR); err != nil {
			t.Fatal(err)
		}
		for i, stage := range unstructuredDataPipelineCR.Spec.Stages {
			if stage.Type == v1alpha1.StageTypeDestinationSyncer {
				unstructuredDataPipelineCR.Spec.Stages[i].DestinationSyncerConfig = &v1alpha1.DestinationSyncerConfig{
					Type: v1alpha1.TypeS3,
					S3DestinationConfig: v1alpha1.S3Config{
						Bucket: unstructuredBucketName,
						Prefix: destinationPrefix,
					},
				}
				break
			}
		}
		if err := kubeClient.Resources(testNamespace).Update(ctx, unstructuredDataPipelineCR); err != nil {
			t.Fatal(err)
		}
		t.Log("Patched UnstructuredDataPipeline destination to S3")

		if err := operatorUtils.WaitForResourceReady(
			ctx,
			v1alpha1.UnstructuredDataPipelineCondition,
			"unstructureddatapipelines.operator.dataverse.redhat.com",
			dataPipelineCRName,
			testNamespace,
		); err != nil {
			t.Fatal(err)
		}
		t.Log("UnstructuredDataPipeline is ready after S3 destination patch")

		destOutput, err := destS3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
			Bucket: aws.String(unstructuredBucketName),
			Prefix: aws.String(destinationPrefix),
		})
		if err != nil {
			t.Fatal(err)
		}
		foundCount := 0
		for _, obj := range destOutput.Contents {
			if obj.Key != nil && strings.HasSuffix(*obj.Key, ".json") {
				t.Logf("Found embeddings file: %s", *obj.Key)
				foundCount++
			}
		}
		if foundCount == 0 {
			t.Fatal("no embeddings files found in destination bucket")
		}
		t.Logf("Found %d embeddings files in destination bucket", foundCount)

		t.Log("Successfully verified embeddings in S3 destination after pipeline patch")
		return ctx
	})

	feature.Assess("S3 hash: upload file2 and verify file1 was not re-uploaded", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		file2Content, err := os.ReadFile(filepath.Join(unstructuredFilesDirectory, "pdflatex-4-pages.pdf"))
		if err != nil {
			t.Fatalf("read file2 test PDF: %v", err)
		}

		file1 := "pdflatex-outline.pdf"
		hashTestFile2Key := fmt.Sprintf("%s/pdflatex-4-pages.pdf", dataPipelineCRName)

		t.Log("find existing file1 in destination and record hash/timestamp")
		file1DestKey, err := operatorUtils.FindDestinationKey(ctx, destS3Client, unstructuredBucketName, destinationPrefix, file1)
		if err != nil {
			t.Fatal(err)
		}

		headBefore, err := operatorUtils.GetS3ObjectMetadata(ctx, destS3Client, unstructuredBucketName, file1DestKey)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("file1 dest key: %s hash: %s time: %v", headBefore.Key, headBefore.ChecksumSHA256, headBefore.LastModified)

		t.Log("upload file2 to source bucket")
		_, err = sourceS3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(unstructuredBucketName),
			Key:    aws.String(hashTestFile2Key),
			Body:   bytes.NewReader(file2Content),
		})
		if err != nil {
			t.Fatalf("upload file2: %v", err)
		}
		if err := operatorUtils.WaitForResourceReady(
			ctx,
			v1alpha1.UnstructuredDataPipelineCondition,
			"unstructureddatapipelines.operator.dataverse.redhat.com",
			dataPipelineCRName,
			testNamespace,
		); err != nil {
			t.Fatalf("pipeline not ready after file2: %v", err)
		}

		t.Log("verify file1 was not re-uploaded")
		headAfter, err := operatorUtils.GetS3ObjectMetadata(ctx, destS3Client, unstructuredBucketName, file1DestKey)
		if err != nil {
			t.Fatal(err)
		}

		if headAfter.ChecksumSHA256 != headBefore.ChecksumSHA256 {
			t.Errorf("file1 re-uploaded when file2 added: hash %s -> %s", headBefore.ChecksumSHA256, headAfter.ChecksumSHA256)
		}
		if !headAfter.LastModified.Equal(headBefore.LastModified) {
			t.Errorf("file1 re-uploaded when file2 added: time %v -> %v", headBefore.LastModified, headAfter.LastModified)
		}
		t.Log("Verified file1 was not re-uploaded after file2 upload")

		return ctx
	})

	feature.Assess("S3 hash: modify file1 and verify re-upload", func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		file1ModifiedContent, err := os.ReadFile(filepath.Join(unstructuredFilesDirectory, "pdflatex-4-pages.pdf"))
		if err != nil {
			t.Fatalf("read modified file1 PDF: %v", err)
		}

		file1 := "pdflatex-outline.pdf"
		sourceKey := fmt.Sprintf("%s/%s", dataPipelineCRName, file1)

		t.Log("find existing file1 in destination and record hash/timestamp")
		destKey, err := operatorUtils.FindDestinationKey(ctx, destS3Client, unstructuredBucketName, destinationPrefix, file1)
		if err != nil {
			t.Fatal(err)
		}

		headBefore, err := operatorUtils.GetS3ObjectMetadata(ctx, destS3Client, unstructuredBucketName, destKey)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("file1 hash: %s time: %v", headBefore.ChecksumSHA256, headBefore.LastModified)

		t.Log("overwrite source file with modified content")
		_, err = sourceS3Client.PutObject(ctx, &s3.PutObjectInput{
			Bucket: aws.String(unstructuredBucketName),
			Key:    aws.String(sourceKey),
			Body:   bytes.NewReader(file1ModifiedContent),
		})
		if err != nil {
			t.Fatalf("upload modified file1: %v", err)
		}

		if err := operatorUtils.WaitForResourceReady(
			ctx,
			v1alpha1.UnstructuredDataPipelineCondition,
			"unstructureddatapipelines.operator.dataverse.redhat.com",
			dataPipelineCRName,
			testNamespace,
		); err != nil {
			t.Fatalf("pipeline not ready after modified file1: %v", err)
		}
		t.Log("UnstructuredDataPipeline successfully reconciled after modify")

		t.Log("verify file1 was re-uploaded")
		headAfter, err := operatorUtils.WaitForS3ObjectHashChange(ctx, destS3Client, unstructuredBucketName, destKey, headBefore.ChecksumSHA256)
		if err != nil {
			t.Fatal(err)
		}

		if !headAfter.LastModified.After(headBefore.LastModified) {
			t.Errorf("file1 not re-uploaded: time %v -> %v", headBefore.LastModified, headAfter.LastModified)
		}
		t.Logf("file1 hash changed: %s -> %s", headBefore.ChecksumSHA256, headAfter.ChecksumSHA256)
		t.Log("Verified file1 was re-uploaded after content change")

		return ctx
	})

	feature.Teardown(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// delete unstructured data pipeline CR
			unstructuredDataPipeline := &v1alpha1.UnstructuredDataPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      dataPipelineCRName,
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

func dumpPodLogs(t *testing.T, namespace, labelSelector, container string) {
	t.Helper()
	cmd := fmt.Sprintf(
		"kubectl logs -n %s -l %s -c %s --tail=100",
		namespace, labelSelector, container,
	)
	t.Logf("=== Pod logs (%s, container=%s) ===", labelSelector, container)
	out, err := exec.Command("sh", "-c", cmd).CombinedOutput()
	if err != nil {
		t.Logf("failed to get pod logs: %v", err)
		return
	}
	t.Logf("%s", string(out))
}

func dumpPodEvents(t *testing.T, namespace, labelSelector string) {
	t.Helper()
	// get pod name first
	nameCmd := fmt.Sprintf(
		"kubectl get pods -n %s -l %s -o jsonpath='{.items[0].metadata.name}'",
		namespace, labelSelector,
	)
	nameOut, err := exec.Command("sh", "-c", nameCmd).CombinedOutput()
	if err != nil {
		t.Logf("failed to get pod name: %v", err)
		return
	}
	podName := strings.Trim(string(nameOut), "'")

	t.Logf("=== Pod events and status (%s) ===", podName)
	descCmd := fmt.Sprintf(
		"kubectl describe pod -n %s %s | grep -A 20 'Events:\\|Containers:\\|State:\\|Restart Count:\\|Last State:'",
		namespace, podName,
	)
	out, err := exec.Command("sh", "-c", descCmd).CombinedOutput()
	if err != nil {
		t.Logf("failed to describe pod: %v", err)
		return
	}
	t.Logf("%s", string(out))
}
