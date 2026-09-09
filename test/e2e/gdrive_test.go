//go:build e2e

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
	"context"
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
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

func TestGDriveDataLoad(t *testing.T) {
	gdriveFolderURL := os.Getenv("GDRIVE_FOLDER_URL")
	if gdriveFolderURL == "" {
		t.Skip("skipping: GDRIVE_FOLDER_URL is not set")
	}

	feature := features.New("GDrive Data Load")

	pipelineName := "gdrive-pipeline"
	outputBucketName := "gdrive-output-bucket"
	dataStorageBucketName := "data-storage-bucket"

	var kubeClient klient.Client
	var s3Client *s3.Client
	var crawledFileCount int

	feature.Setup(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			kubeClient = cfg.Client()

			if err := v1alpha1.AddToScheme(kubeClient.Resources(testNamespace).GetScheme()); err != nil {
				t.Fatalf("Failed to add scheme: %s", err)
			}

			e2eAWS := &awsclienthandler.AWSConfig{
				Region:          "us-east-1",
				AccessKeyID:     "test",
				SecretAccessKey: "test",
				Endpoint:        localstackURL,
			}

			if err := awsclienthandler.NewSourceS3ClientFromConfig(ctx, e2eAWS); err != nil {
				t.Fatal(err)
			}

			var err error
			s3Client, err = awsclienthandler.GetSourceS3Client()
			if err != nil {
				t.Fatal(err)
			}

			// create data-storage bucket (used by operator as filestore for all stages)
			_, _ = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(dataStorageBucketName),
			})
			t.Logf("ensured S3 bucket exists: %s", dataStorageBucketName)

			// create output bucket for GDrive destination
			_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
				Bucket: aws.String(outputBucketName),
			})
			if err != nil {
				t.Fatal(err)
			}
			t.Logf("created S3 bucket: %s", outputBucketName)

			// create GDrive pipeline CR
			pipeline := operatorUtils.GetGDrivePipelineResource(pipelineName, testNamespace, gdriveFolderURL)
			pipeline.Spec.SecretRef = "pipeline-secret"
			t.Log("creating GDrive pipeline CR ...")
			if err := kubeClient.Resources(testNamespace).Create(ctx, &pipeline); err != nil {
				if !apierrors.IsAlreadyExists(err) {
					t.Fatal(err)
				}
			}

			// wait for pipeline to be healthy
			t.Log("waiting for GDrive pipeline CR to be healthy ...")
			if err := operatorUtils.WaitForResourceReady(ctx, v1alpha1.UnstructuredDataPipelineCondition,
				"unstructureddatapipelines.operator.dataverse.redhat.com", pipelineName, testNamespace); err != nil {
				t.Error(err)
			}
			t.Log("GDrive pipeline CR is healthy")

			return ctx
		},
	)

	feature.Assess("files are crawled from Google Drive and processed through pipeline",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// Step 1: wait for source crawler to finish and get the file count
			t.Log("waiting for source crawler to finish crawling ...")
			if err := apimachinerywait.PollUntilContextTimeout(
				context.Background(),
				10*time.Second,
				10*time.Minute,
				false,
				func(ctx context.Context) (done bool, err error) {
					scCR := &v1alpha1.SourceCrawler{}
					if getErr := kubeClient.Resources(testNamespace).Get(ctx,
						pipelineName+"-crawl", testNamespace, scCR); getErr != nil {
						return false, nil
					}
					for _, cond := range scCR.Status.Conditions {
						t.Logf("  SourceCrawler: type=%s status=%s message=%s",
							cond.Type, cond.Status, cond.Message)
					}
					t.Logf("  SourceCrawler filesProcessed: %d", scCR.Status.FilesProcessed)
					if scCR.Status.FilesProcessed > 0 {
						crawledFileCount = int(scCR.Status.FilesProcessed)
						return true, nil
					}
					return false, nil
				},
			); err != nil {
				dumpPodLogs(t, testNamespace, "control-plane=controller-manager", "manager")
				t.Fatalf("source crawler did not finish: %v", err)
			}
			t.Logf("source crawler finished: %d files crawled", crawledFileCount)

			// Step 2: wait for ALL crawled files to reach destination bucket
			t.Logf("waiting for all %d files to reach destination bucket ...", crawledFileCount)
			if err := apimachinerywait.PollUntilContextTimeout(
				context.Background(),
				10*time.Second,
				30*time.Minute,
				false,
				func(ctx context.Context) (done bool, err error) {
					output, listErr := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
						Bucket: aws.String(outputBucketName),
					})
					if listErr != nil {
						t.Logf("failed to list output bucket: %v", listErr)
						return false, nil
					}
					destCount := len(output.Contents)
					if destCount < crawledFileCount {
						t.Logf("destination has %d/%d files, waiting...", destCount, crawledFileCount)
						return false, nil
					}
					t.Logf("all %d files reached destination bucket", destCount)
					return true, nil
				},
			); err != nil {
				dumpPodLogs(t, testNamespace, "control-plane=controller-manager", "manager")
				t.Error(err)
			}

			return ctx
		})

	feature.Assess("crawled files exist in filestore",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			crawlPrefix := "pipelines/" + pipelineName + "/stages/crawl/"
			output, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(dataStorageBucketName),
				Prefix: aws.String(crawlPrefix),
			})
			if err != nil {
				t.Fatalf("failed to list crawled files: %v", err)
			}

			fileCount := 0
			for _, obj := range output.Contents {
				key := aws.ToString(obj.Key)
				if !strings.HasSuffix(key, ".json") && !strings.Contains(key, "/permissions/") {
					fileCount++
					t.Logf("crawled file: %s", key)
				}
			}

			if fileCount != crawledFileCount {
				t.Errorf("expected %d crawled files in S3, got %d", crawledFileCount, fileCount)
			}
			t.Logf("found %d crawled files in filestore (expected %d)", fileCount, crawledFileCount)
			return ctx
		})

	feature.Assess("permissions are stored for crawled files",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			permPrefix := "pipelines/" + pipelineName + "/stages/crawl/permissions/"
			output, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(dataStorageBucketName),
				Prefix: aws.String(permPrefix),
			})
			if err != nil {
				t.Fatalf("failed to list permission files: %v", err)
			}

			if len(output.Contents) != crawledFileCount {
				t.Fatalf("expected %d permission files, got %d", crawledFileCount, len(output.Contents))
			}
			t.Logf("found %d permission files (expected %d)", len(output.Contents), crawledFileCount)

			// verify at least one permission file has valid JSON
			for _, obj := range output.Contents {
				key := aws.ToString(obj.Key)
				getOut, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
					Bucket: aws.String(dataStorageBucketName),
					Key:    aws.String(key),
				})
				if err != nil {
					t.Errorf("failed to get permission file %s: %v", key, err)
					continue
				}

				var perms []map[string]any
				if err := json.NewDecoder(getOut.Body).Decode(&perms); err != nil {
					getOut.Body.Close()
					t.Errorf("invalid JSON in permission file %s: %v", key, err)
					continue
				}
				getOut.Body.Close()

				if len(perms) == 0 {
					t.Errorf("permission file %s has 0 entries", key)
					continue
				}
				for _, p := range perms {
					if p["type"] == nil || p["role"] == nil {
						t.Errorf("permission entry missing type or role in %s", key)
					}
				}
			}

			return ctx
		})

	feature.Assess("converted documents exist in filestore",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			convertPrefix := "pipelines/" + pipelineName + "/stages/convert/"
			output, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(dataStorageBucketName),
				Prefix: aws.String(convertPrefix),
			})
			if err != nil {
				t.Fatalf("failed to list converted files: %v", err)
			}

			if len(output.Contents) != crawledFileCount {
				t.Errorf("expected %d converted files, got %d", crawledFileCount, len(output.Contents))
			}
			t.Logf("found %d converted files (expected %d)", len(output.Contents), crawledFileCount)
			return ctx
		})

	feature.Assess("chunks exist in filestore",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			chunkPrefix := "pipelines/" + pipelineName + "/stages/chunk/"
			output, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(dataStorageBucketName),
				Prefix: aws.String(chunkPrefix),
			})
			if err != nil {
				t.Fatalf("failed to list chunk files: %v", err)
			}

			if len(output.Contents) != crawledFileCount {
				t.Errorf("expected %d chunk files, got %d", crawledFileCount, len(output.Contents))
			}
			t.Logf("found %d chunk files (expected %d)", len(output.Contents), crawledFileCount)
			return ctx
		})

	feature.Assess("embeddings are synced to destination S3",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			output, err := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(outputBucketName),
			})
			if err != nil {
				t.Fatalf("failed to list output bucket: %v", err)
			}

			if len(output.Contents) != crawledFileCount {
				t.Errorf("expected %d files in destination bucket, got %d", crawledFileCount, len(output.Contents))
			}
			t.Logf("found %d files in destination bucket (expected %d)", len(output.Contents), crawledFileCount)
			return ctx
		})

	feature.Teardown(
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			// delete pipeline CR
			pipeline := &v1alpha1.UnstructuredDataPipeline{
				ObjectMeta: metav1.ObjectMeta{
					Name:      pipelineName,
					Namespace: testNamespace,
				},
			}
			if err := kubeClient.Resources(testNamespace).Delete(ctx, pipeline); err != nil {
				t.Logf("failed to delete GDrive pipeline: %v", err)
			}

			// cleanup output bucket
			output, _ := s3Client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
				Bucket: aws.String(outputBucketName),
			})
			if output != nil {
				for _, obj := range output.Contents {
					_, _ = s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
						Bucket: aws.String(outputBucketName),
						Key:    obj.Key,
					})
				}
			}
			_, _ = s3Client.DeleteBucket(ctx, &s3.DeleteBucketInput{
				Bucket: aws.String(outputBucketName),
			})
			t.Log("cleaned up GDrive test resources")

			return ctx
		},
	)

	testenv.Test(t, feature.Feature())
}
