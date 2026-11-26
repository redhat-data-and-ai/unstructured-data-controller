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

package controller

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

func TestMarkLostJobs(t *testing.T) {
	pending := string(docling.TaskStatusPending)
	tests := []struct {
		name     string
		attempts int
	}{
		{name: "under the limit keeps the job for resubmission", attempts: 1},
		// Giving up here would skip the check that a conversion stored before
		// the loss gets counted; the limit is applied at resubmission instead.
		{name: "at the limit still keeps the job", attempts: maxDoclingConversionAttempts},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lost := operatorv1alpha1.Job{FilePath: "a.pdf", TaskID: "dead-task", Status: pending, Attempts: test.attempts}
			unrelated := operatorv1alpha1.Job{FilePath: "b.pdf", TaskID: "live-task", Status: pending, Attempts: 1}
			documentProcessorCR := &operatorv1alpha1.DocumentProcessor{}
			documentProcessorCR.Status.Jobs = []operatorv1alpha1.Job{lost, unrelated}

			markLostJobs(context.Background(), documentProcessorCR, []operatorv1alpha1.Job{lost})

			assert.Equal(t, unrelated, *documentProcessorCR.GetJobByFilePath("b.pdf"), "unrelated job must be untouched")
			assert.False(t, documentProcessorCR.IsFilePermanentlyFailing("a.pdf"))
			job := documentProcessorCR.GetJobByFilePath("a.pdf")
			require.NotNil(t, job)
			assert.Empty(t, job.TaskID, "a lost task id must be cleared so it is never polled again")
			assert.Equal(t, string(docling.TaskStatusFailure), job.Status)
			assert.Equal(t, test.attempts, job.Attempts, "the attempt count must carry forward")
		})
	}
}

func TestExistingJobDecision(t *testing.T) {
	file := unstructured.ConvertedFileMetadata{
		RawFilePath:       "a.pdf",
		FileIdentifier:    "uid-1",
		DocumentConverter: unstructured.DocumentConverterDocling,
	}
	jobFor := func(status, fileIdentifier string, attempts int) *operatorv1alpha1.Job {
		return &operatorv1alpha1.Job{
			FilePath:          "a.pdf",
			FileIdentifier:    fileIdentifier,
			DocumentConverter: string(unstructured.DocumentConverterDocling),
			TaskID:            "task-1",
			Status:            status,
			Attempts:          attempts,
		}
	}
	failure, skipped := string(docling.TaskStatusFailure), string(docling.TaskStatusSkipped)

	tests := []struct {
		name       string
		job        *operatorv1alpha1.Job
		wantAction jobAction
		wantErr    string
	}{
		{name: "failed under the limit is resubmitted", job: jobFor(failure, "uid-1", 1), wantAction: jobResubmit},
		{name: "skipped under the limit is resubmitted", job: jobFor(skipped, "uid-1", 1), wantAction: jobResubmit},
		{name: "failed at the limit is given up on",
			job: jobFor(failure, "uid-1", maxDoclingConversionAttempts), wantAction: jobGiveUp},
		{name: "skipped at the limit is given up on",
			job: jobFor(skipped, "uid-1", maxDoclingConversionAttempts), wantAction: jobGiveUp},
		{name: "pending is left to finish", job: jobFor(string(docling.TaskStatusPending), "uid-1", 1), wantAction: jobWait},
		{name: "started is left to finish", job: jobFor(string(docling.TaskStatusStarted), "uid-1", 1), wantAction: jobWait},
		{name: "succeeded needs nothing", job: jobFor(string(docling.TaskStatusSuccess), "uid-1", 1), wantAction: jobWait},
		{name: "changed file identity replaces the job, even at the limit",
			job: jobFor(failure, "uid-2", maxDoclingConversionAttempts), wantAction: jobReplace},
		{name: "unknown status is an error naming the task", job: jobFor("bogus", "uid-1", 1),
			wantErr: `invalid task status "bogus" for task id task-1`},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			action, err := existingJobDecision(test.job, &file)
			if test.wantErr != "" {
				require.EqualError(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.wantAction, action)
		})
	}
}

// TestLostLastAttemptWithAStoredConversion covers the completion patch failing
// on the final attempt and docling then losing the task. The stored conversion
// must be counted once and the file must not be marked permanently failing.
func TestLostLastAttemptWithAStoredConversion(t *testing.T) {
	documentProcessorCR := &operatorv1alpha1.DocumentProcessor{}
	documentProcessorCR.Status.FilesProcessed = 4
	lost := operatorv1alpha1.Job{FilePath: "a.pdf", TaskID: "dead-task",
		Status: string(docling.TaskStatusPending), Attempts: maxDoclingConversionAttempts}
	documentProcessorCR.Status.Jobs = []operatorv1alpha1.Job{lost}

	// reconcileInFlightJobs on the 404, then needsConversion finding the stored file.
	markLostJobs(context.Background(), documentProcessorCR, []operatorv1alpha1.Job{lost})
	require.False(t, documentProcessorCR.IsFilePermanentlyFailing("a.pdf"),
		"the file must not be given up on before the stored conversion is seen")
	settleFinishedLostJob(documentProcessorCR, "a.pdf")

	assert.Equal(t, int64(5), documentProcessorCR.Status.FilesProcessed, "counted exactly once")
	assert.Nil(t, documentProcessorCR.GetJobByFilePath("a.pdf"))
	assert.False(t, documentProcessorCR.IsFilePermanentlyFailing("a.pdf"))
}

func TestSettleFinishedLostJob(t *testing.T) {
	documentProcessorCR := &operatorv1alpha1.DocumentProcessor{}
	documentProcessorCR.Status.FilesProcessed = 4
	documentProcessorCR.Status.Jobs = []operatorv1alpha1.Job{
		{FilePath: "a.pdf", Status: string(docling.TaskStatusFailure), Attempts: 1},
		{FilePath: "b.pdf", TaskID: "live-task", Status: string(docling.TaskStatusPending), Attempts: 1},
	}

	settleFinishedLostJob(documentProcessorCR, "a.pdf")

	assert.Nil(t, documentProcessorCR.GetJobByFilePath("a.pdf"), "the settled job must be removed")
	assert.NotNil(t, documentProcessorCR.GetJobByFilePath("b.pdf"), "an unrelated job must be kept")
	assert.Equal(t, int64(5), documentProcessorCR.Status.FilesProcessed,
		"the stored conversion must be counted, or downstream stages are never woken for it")
}
