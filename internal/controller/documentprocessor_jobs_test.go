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
		name              string
		attempts          int
		wantJob           bool
		wantPermanentFail bool
	}{
		{name: "under the limit keeps the job for resubmission", attempts: 1, wantJob: true},
		{name: "one short of the limit still keeps the job", attempts: maxDoclingConversionAttempts - 1, wantJob: true},
		{name: "at the limit gives up on the file", attempts: maxDoclingConversionAttempts, wantPermanentFail: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			lost := operatorv1alpha1.Job{FilePath: "a.pdf", TaskID: "dead-task", Status: pending, Attempts: test.attempts}
			unrelated := operatorv1alpha1.Job{FilePath: "b.pdf", TaskID: "live-task", Status: pending, Attempts: 1}
			documentProcessorCR := &operatorv1alpha1.DocumentProcessor{}
			documentProcessorCR.Status.Jobs = []operatorv1alpha1.Job{lost, unrelated}

			markLostJobs(context.Background(), documentProcessorCR, []operatorv1alpha1.Job{lost})

			assert.Equal(t, unrelated, *documentProcessorCR.GetJobByFilePath("b.pdf"), "unrelated job must be untouched")
			assert.Equal(t, test.wantPermanentFail, documentProcessorCR.IsFilePermanentlyFailing("a.pdf"))

			job := documentProcessorCR.GetJobByFilePath("a.pdf")
			if !test.wantJob {
				assert.Nil(t, job, "a file given up on must not keep a job")
				return
			}
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
	jobFor := func(status, fileIdentifier string) *operatorv1alpha1.Job {
		return &operatorv1alpha1.Job{
			FilePath:          "a.pdf",
			FileIdentifier:    fileIdentifier,
			DocumentConverter: string(unstructured.DocumentConverterDocling),
			TaskID:            "task-1",
			Status:            status,
		}
	}

	tests := []struct {
		name        string
		job         *operatorv1alpha1.Job
		wantConvert bool
		wantDelete  bool
		wantErr     string
	}{
		{
			name:        "failed job with matching config is resubmitted and kept",
			job:         jobFor(string(docling.TaskStatusFailure), "uid-1"),
			wantConvert: true,
		},
		{
			name:        "skipped job with matching config is resubmitted and kept",
			job:         jobFor(string(docling.TaskStatusSkipped), "uid-1"),
			wantConvert: true,
		},
		{
			name: "pending job is left to finish",
			job:  jobFor(string(docling.TaskStatusPending), "uid-1"),
		},
		{
			name: "started job is left to finish",
			job:  jobFor(string(docling.TaskStatusStarted), "uid-1"),
		},
		{
			name: "succeeded job needs nothing",
			job:  jobFor(string(docling.TaskStatusSuccess), "uid-1"),
		},
		{
			name:        "changed file identity deletes the job and converts afresh",
			job:         jobFor(string(docling.TaskStatusPending), "uid-2"),
			wantConvert: true,
			wantDelete:  true,
		},
		{
			name:    "unknown status is an error naming the task",
			job:     jobFor("bogus", "uid-1"),
			wantErr: `invalid task status "bogus" for task id task-1`,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			convert, deleteJob, err := existingJobDecision(test.job, &file)
			if test.wantErr != "" {
				require.EqualError(t, err, test.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.wantConvert, convert, "convert")
			assert.Equal(t, test.wantDelete, deleteJob, "deleteJob")
		})
	}
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
