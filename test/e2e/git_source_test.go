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
	"context"
	"fmt"
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

func TestGitSourceCrawl(t *testing.T) {
	feature := features.New("Git Source Crawl")

	gitPipelineName := "git-source"
	dataStorageBucketName := "data-storage-bucket"

	var kubeClient klient.Client

	feature.Setup(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		kubeClient = cfg.Client()

		if err := v1alpha1.AddToScheme(kubeClient.Resources(testNamespace).GetScheme()); err != nil {
			t.Fatalf("Failed to add scheme: %s", err)
		}

		// Create S3 client pointing at localstack to create the data-storage bucket.
		// The controller writes crawled files to this bucket (configured in ControllerConfig).
		e2eAWS := &awsclienthandler.AWSConfig{
			Region:          "us-east-1",
			AccessKeyID:     "test",
			SecretAccessKey: "test",
			Endpoint:        localstackURL,
		}

		err := awsclienthandler.NewSourceS3ClientFromConfig(ctx, e2eAWS)
		if err != nil {
			t.Fatal(err)
		}

		s3Client, err := awsclienthandler.GetSourceS3Client()
		if err != nil {
			t.Fatal(err)
		}

		_, err = s3Client.CreateBucket(ctx, &s3.CreateBucketInput{
			Bucket: aws.String(dataStorageBucketName),
		})
		if err != nil {
			t.Fatal(fmt.Errorf("creating data-storage bucket: %w", err))
		}
		t.Log("data-storage bucket created")

		pipeline := operatorUtils.GetGitPipelineCrawlOnly(gitPipelineName, testNamespace)
		t.Log("creating git pipeline CR (crawl only) ...")
		if err := kubeClient.Resources(testNamespace).Create(ctx, &pipeline); err != nil {
			if !apierrors.IsAlreadyExists(err) {
				t.Fatal(err)
			}
		}

		t.Log("waiting for pipeline CR to be healthy")
		if err := operatorUtils.WaitForResourceReady(
			ctx, v1alpha1.UnstructuredDataPipelineCondition,
			"unstructureddatapipelines.operator.dataverse.redhat.com",
			gitPipelineName, testNamespace,
		); err != nil {
			dumpPodLogs(t, testNamespace, "control-plane=controller-manager", "manager")
			t.Fatal(err)
		}

		return ctx
	})

	feature.Assess("initial crawl succeeds with valid commit SHA",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			sourceCrawlerName := gitPipelineName + "-crawl"
			if err := operatorUtils.WaitForResourceReady(
				ctx, v1alpha1.SourceCrawlerCondition,
				"sourcecrawlers.operator.dataverse.redhat.com",
				sourceCrawlerName, testNamespace,
			); err != nil {
				dumpPodLogs(t, testNamespace, "control-plane=controller-manager", "manager")
				t.Fatal(err)
			}

			crawlerCR := &v1alpha1.SourceCrawler{}
			if err := kubeClient.Resources(testNamespace).Get(
				ctx, sourceCrawlerName, testNamespace, crawlerCR,
			); err != nil {
				t.Fatal(err)
			}

			if len(crawlerCR.Status.GitRepoStatus) == 0 {
				t.Fatal("expected GitRepoStatus to be populated")
			}
			repoStatus := crawlerCR.Status.GitRepoStatus[0]
			if repoStatus.CommitSHA == "" {
				t.Error("expected non-empty CommitSHA")
			}
			if repoStatus.Error != "" {
				t.Errorf("unexpected error in GitRepoStatus: %s", repoStatus.Error)
			}
			if crawlerCR.Status.FilesProcessed == 0 {
				t.Error("expected FilesProcessed > 0")
			}
			t.Logf("GitRepoStatus: repo=%s commitSHA=%s configHash=%s filesProcessed=%d",
				repoStatus.Repo, repoStatus.CommitSHA, repoStatus.ConfigHash, crawlerCR.Status.FilesProcessed)

			return ctx
		})

	feature.Assess("config change triggers re-crawl with docs path",
		func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
			sourceCrawlerName := gitPipelineName + "-crawl"

			// Record state before config change
			crawlerBefore := &v1alpha1.SourceCrawler{}
			if err := kubeClient.Resources(testNamespace).Get(
				ctx, sourceCrawlerName, testNamespace, crawlerBefore,
			); err != nil {
				t.Fatal(err)
			}
			if len(crawlerBefore.Status.GitRepoStatus) == 0 {
				t.Fatal("expected GitRepoStatus to be populated before config change")
			}
			configHashBefore := crawlerBefore.Status.GitRepoStatus[0].ConfigHash
			filesProcessedBefore := crawlerBefore.Status.FilesProcessed
			t.Logf("before config change: configHash=%s filesProcessed=%d", configHashBefore, filesProcessedBefore)

			// Broaden Paths to include docs/
			pipelineCR := &v1alpha1.UnstructuredDataPipeline{}
			if err := kubeClient.Resources(testNamespace).Get(
				ctx, gitPipelineName, testNamespace, pipelineCR,
			); err != nil {
				t.Fatal(err)
			}
			for i, stage := range pipelineCR.Spec.Stages {
				if stage.Type == v1alpha1.StageTypeSourceCrawler {
					pipelineCR.Spec.Stages[i].SourceCrawlerConfig.GitConfig.Repos[0].Paths =
						[]string{"README.md", "docs/"}
					break
				}
			}
			if err := kubeClient.Resources(testNamespace).Update(ctx, pipelineCR); err != nil {
				t.Fatal(err)
			}
			t.Log("updated Paths to include README.md and docs/")

			// Poll until the SourceCrawler reconciles the new config.
			// WaitForResourceReady is insufficient here because the condition
			// may still be Ready from the previous reconcile.
			var crawlerAfter *v1alpha1.SourceCrawler
			if err := apimachinerywait.PollUntilContextTimeout(
				context.Background(), 5*time.Second, 3*time.Minute, false,
				func(ctx context.Context) (bool, error) {
					crawler := &v1alpha1.SourceCrawler{}
					if getErr := kubeClient.Resources(testNamespace).Get(
						ctx, sourceCrawlerName, testNamespace, crawler,
					); getErr != nil {
						t.Logf("failed to get SourceCrawler: %v", getErr)
						return false, nil
					}
					if len(crawler.Status.GitRepoStatus) == 0 {
						return false, nil
					}
					if crawler.Status.GitRepoStatus[0].ConfigHash != configHashBefore {
						crawlerAfter = crawler
						return true, nil
					}
					t.Log("waiting for SourceCrawler to reconcile new config ...")
					return false, nil
				},
			); err != nil {
				dumpPodLogs(t, testNamespace, "control-plane=controller-manager", "manager")
				t.Fatal("timed out waiting for config change to be reconciled")
			}

			configHashAfter := crawlerAfter.Status.GitRepoStatus[0].ConfigHash
			if crawlerAfter.Status.FilesProcessed < filesProcessedBefore {
				t.Errorf("expected FilesProcessed >= %d, got %d",
					filesProcessedBefore, crawlerAfter.Status.FilesProcessed)
			}
			t.Logf("after config change: configHash=%s filesProcessed=%d",
				configHashAfter, crawlerAfter.Status.FilesProcessed)

			return ctx
		})

	feature.Teardown(func(ctx context.Context, t *testing.T, cfg *envconf.Config) context.Context {
		pipeline := &v1alpha1.UnstructuredDataPipeline{
			ObjectMeta: metav1.ObjectMeta{
				Name:      gitPipelineName,
				Namespace: testNamespace,
			},
		}
		if err := kubeClient.Resources(testNamespace).Delete(ctx, pipeline); err != nil {
			t.Fatal(err)
		}
		return ctx
	})

	testenv.Test(t, feature.Feature())
}
