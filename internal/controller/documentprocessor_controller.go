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
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

const (
	DocumentProcessorControllerName = "DocumentProcessor"
	requeueAfter                    = 15 * time.Second
)

var (
	maxDoclingConversionAttempts = 3
)

// DocumentProcessorReconciler reconciles a DocumentProcessor object
type DocumentProcessorReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	doclingConfig *docling.DoclingConfig
	fileStore     *filestore.FileStore
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=documentprocessors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=documentprocessors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=documentprocessors/finalizers,verbs=update

func (r *DocumentProcessorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling DocumentProcessor")

	// NOTE: Config CR health check is commented out as requested
	// This was the original dependency on config.spec
	isHealthy, err := IsConfigCRHealthy(ctx, r.Client, req.Namespace)
	if err != nil {
		logger.Error(err, "failed to check if Config CR is healthy")
		return ctrl.Result{}, err
	}
	if !isHealthy {
		logger.Info("Config CR is not ready yet, will try again in a bit ...")
		return ctrl.Result{
			RequeueAfter: 10 * time.Second,
		}, nil
	}

	documentProcessorCR := &operatorv1alpha1.DocumentProcessor{}
	if err := r.Get(ctx, req.NamespacedName, documentProcessorCR); err != nil {
		logger.Error(err, "failed to get DocumentProcessor CR")
		return ctrl.Result{}, err
	}

	// set status to waiting
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.DocumentProcessor{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.SetWaiting()
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update DocumentProcessor CR status")
		return ctrl.Result{}, err
	}

	r.doclingConfig = &docling.DoclingConfig{
		FromFormats:     documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig.FromFormats,
		ImageExportMode: documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig.ImageExportMode,
		DoOCR:           documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig.DoOCR,
		ForceOCR:        documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig.ForceOCR,
		OCREngine:       documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig.OCREngine,
		OCRLang:         documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig.OCRLang,
		PDFBackend:      documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig.PDFBackend,
		TableMode:       documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig.TableMode,
	}

	fs, err := filestore.New(ctx, cacheDirectory, dataStorageBucket)
	if err != nil {
		logger.Error(err, "failed to create filestore")
		return r.handleError(ctx, documentProcessorCR, err)
	}
	r.fileStore = fs

	// first, let's figure out the jobs that are currently running
	jobProcessingErrors := []error{}
	for _, job := range documentProcessorCR.Status.Jobs {
		logger.Info("reconciling job", "job", job)
		if err := r.reconcileJob(ctx, job, documentProcessorCR); err != nil {
			jobProcessingErrors = append(jobProcessingErrors, err)
			logger.Error(err, "failed to process job", "job", job.FilePath)
		}
	}

	// now let's process if there are any new raw files that need to be converted
	dataProductName := documentProcessorCR.Spec.DataProduct
	filePaths, err := r.fileStore.ListFilesInPath(ctx, dataProductName)
	if err != nil {
		logger.Error(err, "failed to list files in path")
		return r.handleError(ctx, documentProcessorCR, err)
	}

	// now that we have the list of files to process, we may remove the force reconcile label as we are ready to accept more events
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.DocumentProcessor{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		return controllerutils.RemoveForceReconcileLabel(ctx, r.Client, latest)
	}); err != nil {
		logger.Error(err, "failed to remove force reconcile label from DocumentProcessor CR")
		return r.handleError(ctx, documentProcessorCR, err)
	}

	documentProcessingErrors := []error{}
	rawFilePaths := unstructured.FilterRawFilePaths(filePaths)
	for _, rawFilePath := range rawFilePaths {
		logger.Info("processing document", "document", rawFilePath)
		if err := r.processDocument(ctx, rawFilePath, documentProcessorCR); err != nil {
			documentProcessingErrors = append(documentProcessingErrors, err)
			logger.Error(err, "failed to process document", "document", rawFilePath)
		}
	}

	// add force reconcile label to the ChunksGenerator CR
	chunksGeneratorKey := client.ObjectKey{Namespace: documentProcessorCR.Namespace, Name: documentProcessorCR.Name}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		chunksGeneratorCR := &operatorv1alpha1.ChunksGenerator{}
		if err := r.Get(ctx, chunksGeneratorKey, chunksGeneratorCR); err != nil {
			return err
		}
		return controllerutils.AddForceReconcileLabel(ctx, r.Client, chunksGeneratorCR)
	}); err != nil {
		logger.Error(err, "failed to add force reconcile label to ChunksGenerator CR")
		return r.handleError(ctx, documentProcessorCR, err)
	}

	if len(documentProcessingErrors) > 0 || len(jobProcessingErrors) > 0 {
		return r.handleError(ctx, documentProcessorCR, errors.New("failed to process jobs or documents"))
	}

	toRequeue := len(documentProcessorCR.Status.Jobs) > 0

	if toRequeue {
		logger.Info("some jobs are still pending or running, will requeue after a bit ...")
		return ctrl.Result{
			RequeueAfter: requeueAfter,
		}, nil
	}

	logger.Info("all jobs are completed, no need to requeue")

	// all done, let's update the status to ready
	successMessage := fmt.Sprintf("successfully reconciled document processor: %s", documentProcessorCR.Name)
	documentProcessorKey := client.ObjectKeyFromObject(documentProcessorCR)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		res := &operatorv1alpha1.DocumentProcessor{}
		if err := r.Get(ctx, documentProcessorKey, res); err != nil {
			return err
		}
		res.UpdateStatus(successMessage, nil)
		return r.Status().Update(ctx, res)
	}); err != nil {
		logger.Error(err, "failed to update DocumentProcessor CR status", "namespace", documentProcessorKey.Namespace, "name", documentProcessorKey.Name)
		return r.handleError(ctx, documentProcessorCR, err)
	}
	logger.Info("successfully updated DocumentProcessor CR status", "status", documentProcessorCR.Status)

	return ctrl.Result{}, nil
}

func (r *DocumentProcessorReconciler) reconcileJob(ctx context.Context, job operatorv1alpha1.Job, documentProcessorCR *operatorv1alpha1.DocumentProcessor) (err error) {
	logger := log.FromContext(ctx)
	// recover from panic semaphore panic
	defer func() {
		if panicErr := recover(); panicErr != nil {
			logger.Info("recovered from panic in processJob, likely stale job from previous session", "panic", panicErr, "taskID", job.TaskID, "filePath", job.FilePath)
			if panicMessage, ok := panicErr.(string); ok && strings.Contains(panicMessage, docling.SemaphorePanicError) {
				logger.Info("semaphore panic detected, removing stale job", "taskID", job.TaskID)
				documentProcessorKey := client.ObjectKeyFromObject(documentProcessorCR)
				if updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
					latest := &operatorv1alpha1.DocumentProcessor{}
					if getErr := r.Get(ctx, documentProcessorKey, latest); getErr != nil {
						return getErr
					}
					latest.DeleteJobByFilePath(job.FilePath)
					return r.Status().Update(ctx, latest)
				}); updateErr != nil {
					logger.Error(updateErr, "failed to delete stale job after panic", "filePath", job.FilePath)
					err = updateErr
					return
				}

				logger.Info("successfully removed stale job after semaphore panic", "filePath", job.FilePath)
				err = fmt.Errorf("reconcileJob() function exited with panic: %v", panicErr)
				return
			}
			// we don't want to recover from other types of panic
			panic(panicErr)
		}
	}()

	doclingTaskStatus, doclingResponse, err := doclingClient.GetConvertedFile(ctx, job.TaskID)
	if err != nil {
		return err
	}

	switch doclingTaskStatus {
	case docling.TaskStatusSuccess, docling.TaskStatusPartialSuccess:
		logger.Info("docling task has been completed successfully, storing the converted file in the filestore", "taskID", job.TaskID, "filePath", job.FilePath)
		// store the converted file in the filestore
		convertedFileMetadata := unstructured.ConvertedFileMetadata{
			RawFilePath:       job.FilePath,
			DocumentConverter: unstructured.DocumentConverterDocling,
			DoclingConfig:     documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig,
		}
		convertedFile := unstructured.ConvertedFile{
			ConvertedDocument: &unstructured.ConvertedDocument{
				Metadata: &convertedFileMetadata,
				Content: &unstructured.Content{
					Markdown: doclingResponse.Document.MDContent,
				},
			},
		}

		convertedFileBytes, err := json.Marshal(convertedFile)
		if err != nil {
			return err
		}
		convertedFilePath := unstructured.GetConvertedFilePath(job.FilePath)
		if err := r.fileStore.Store(ctx, convertedFilePath, convertedFileBytes); err != nil {
			return err
		}
		logger.Info("successfully stored the converted file in the filestore", "filePath", convertedFilePath)

		// remove the job from the status as nothing needs to be done for this job
		documentProcessorKey := client.ObjectKeyFromObject(documentProcessorCR)
		if updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &operatorv1alpha1.DocumentProcessor{}
			if getErr := r.Get(ctx, documentProcessorKey, latest); getErr != nil {
				return getErr
			}
			latest.DeleteJobByFilePath(job.FilePath)
			return r.Status().Update(ctx, latest)
		}); updateErr != nil {
			logger.Error(updateErr, "failed to delete job from status as it has completed successfully", "filePath", job.FilePath)
			return updateErr
		}
		logger.Info("successfully removed job from status as it has completed successfully", "filePath", job.FilePath)

	case docling.TaskStatusFailure, docling.TaskStatusSkipped:
		logger.Info("docling task has failed or skipped, will be retried", "taskID", job.TaskID, "filePath", job.FilePath)
		// if the attempts > max attempts, remove the job from the status
		if job.Attempts >= maxDoclingConversionAttempts {
			logger.Error(fmt.Errorf("failed to convert file, max attempts reached for file: %s", job.FilePath), "failed to convert file, max attempts reached")
			documentProcessorKey := client.ObjectKeyFromObject(documentProcessorCR)
			if updateErr := retry.RetryOnConflict(retry.DefaultRetry, func() error {
				latest := &operatorv1alpha1.DocumentProcessor{}
				if getErr := r.Get(ctx, documentProcessorKey, latest); getErr != nil {
					return getErr
				}
				latest.AddPermanentlyFailingFile(job.FilePath)
				latest.DeleteJobByFilePath(job.FilePath)
				return r.Status().Update(ctx, latest)
			}); updateErr != nil {
				logger.Error(updateErr, "failed to delete job from status as it has failed or skipped", "filePath", job.FilePath)
				return updateErr
			}
			logger.Info("successfully deleted job from status as it has failed or skipped", "filePath", job.FilePath)
		}

	case docling.TaskStatusPending, docling.TaskStatusStarted:
		// nothing needs to be done for this job
		logger.Info("job is still pending or started", "task id", job.TaskID, "status", doclingTaskStatus, "file", job.FilePath)

	default:
		// invalid task status
		return fmt.Errorf("invalid task status received for task id: %s, status: %s, file: %s", job.TaskID, doclingTaskStatus, job.FilePath)
	}

	return nil
}

func (r *DocumentProcessorReconciler) processDocument(ctx context.Context, rawFilePath string, documentProcessorCR *operatorv1alpha1.DocumentProcessor) error {
	logger := log.FromContext(ctx)
	logger.Info("processing document", "rawFilePath", rawFilePath)

	// check if the document needs to be converted
	needsConversion, err := r.needsConversion(ctx, rawFilePath, documentProcessorCR)
	if err != nil {
		logger.Error(err, "failed to check if document needs conversion")
		return err
	}
	if !needsConversion {
		logger.Info("document does not need conversion, skipping ...")
		return nil
	}

	// create a new job for the document
	fileURL, err := r.fileStore.GetFileURL(ctx, rawFilePath)
	if err != nil {
		logger.Error(err, "failed to get file URL")
		return err
	}
	response, err := doclingClient.ConvertFile(ctx, fileURL, *r.doclingConfig)
	if err != nil {
		logger.Error(err, "failed to convert file")
		if strings.Contains(err.Error(), docling.SemaphoreAcquireError) {
			logger.Error(err, "failed to convert file, semaphore acquire error, will try again later")
			return nil // no error, just skip the conversion this time
		}
		return err
	}

	logger.Info("docling task has been accepted, adding job to status", "taskID", response.TaskID, "filePath", rawFilePath)

	// if the job already exists, then get the conversion attempt count else set to 1
	conversionAttempt := 1
	job := documentProcessorCR.GetJobByFilePath(rawFilePath)
	if job != nil {
		conversionAttempt = job.Attempts + 1
	}

	documentProcessorKey := client.ObjectKey{Namespace: documentProcessorCR.Namespace, Name: documentProcessorCR.Name}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.DocumentProcessor{}
		if getErr := r.Get(ctx, documentProcessorKey, latest); getErr != nil {
			return getErr
		}
		latest.AddOrUpdateJob(operatorv1alpha1.Job{
			FilePath:          rawFilePath,
			DocumentConverter: string(unstructured.DocumentConverterDocling),
			DoclingConfig:     latest.Spec.DocumentProcessorConfig.DoclingConfig,
			TaskID:            response.TaskID,
			Status:            response.TaskStatus,
			CreatedOn:         time.Now().Format(time.RFC3339),
			Attempts:          conversionAttempt,
		})
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update DocumentProcessor CR status")
		return err
	}

	return nil
}

func (r *DocumentProcessorReconciler) needsConversion(ctx context.Context, rawFilePath string, documentProcessorCR *operatorv1alpha1.DocumentProcessor) (bool, error) {
	logger := log.FromContext(ctx)

	logger.Info("checking if document should be converted", "filePath", rawFilePath)

	// does the raw file even exist? this is unlikely to happen, but just in case
	rawFileExists, err := r.fileStore.Exists(ctx, rawFilePath)
	if err != nil {
		return false, err
	}
	if !rawFileExists {
		err := fmt.Errorf("raw file %s does not exist", rawFilePath)
		logger.Error(err, "raw file does not exist", "filePath", rawFilePath)
		return false, err
	}

	// does the converted file exist?
	convertedFileExists, err := r.fileStore.Exists(ctx, unstructured.GetConvertedFilePath(rawFilePath))
	if err != nil {
		return false, err
	}
	if convertedFileExists {
		// if the converted file exists, then we need to check if it has the same configuration
		convertedFileRaw, err := r.fileStore.Retrieve(ctx, unstructured.GetConvertedFilePath(rawFilePath))
		if err != nil {
			return false, err
		}

		convertedFile := unstructured.ConvertedFile{}
		err = json.Unmarshal(convertedFileRaw, &convertedFile)
		if err != nil {
			return false, err
		}
		currentConvertedFileMetadata := convertedFile.ConvertedDocument.Metadata

		fileToConvertMetadata := unstructured.ConvertedFileMetadata{
			RawFilePath:       rawFilePath,
			DocumentConverter: unstructured.DocumentConverterDocling,
			DoclingConfig:     documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig,
		}

		if currentConvertedFileMetadata.Equal(&fileToConvertMetadata) {
			logger.Info("converted file has the same configuration, no conversion needed", "filePath", rawFilePath)
			return false, nil
		}
	}

	// let's check if the file exists in the permanently failing files list
	if documentProcessorCR.IsFilePermanentlyFailing(rawFilePath) {
		logger.Info("file is permanently failing, no conversion needed", "filePath", rawFilePath)
		return false, nil
	}

	// now let's check if the job exists in status for this file
	job := documentProcessorCR.GetJobByFilePath(rawFilePath)
	if job != nil {
		// job already exists for this document
		logger.Info("job already exists for the file, checking if it's the same configuration ...", "filePath", rawFilePath)
		fileToConvert := unstructured.ConvertedFileMetadata{
			RawFilePath:       rawFilePath,
			DocumentConverter: unstructured.DocumentConverterDocling,
			DoclingConfig:     documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig,
		}
		fileInJob := unstructured.ConvertedFileMetadata{
			RawFilePath:       job.FilePath,
			DocumentConverter: unstructured.DocumentConverter(job.DocumentConverter),
			DoclingConfig:     job.DoclingConfig,
		}

		// first, we will check if the current job is still valid (same configuration)
		if fileInJob.Equal(&fileToConvert) {
			// we will check if the job is running or completed, if yes, we will return
			// but if the job is failed, we will send it for conversion again and increment the attempt count
			switch job.Status {
			case string(docling.TaskStatusFailure), string(docling.TaskStatusSkipped):
				logger.Info("job is failed or skipped, will be sent for conversion again ...", "filePath", rawFilePath)
			case string(docling.TaskStatusPending), string(docling.TaskStatusStarted), string(docling.TaskStatusPartialSuccess), string(docling.TaskStatusSuccess):
				logger.Info("job is running or completed, no need to create a new job", "filePath", rawFilePath)
				return false, nil
			default:
				return false, fmt.Errorf("invalid task status received for task id: %s", job.TaskID)
			}
		}

		// if the current job is not valid (different configuration), then it is of no use to us irrespective of the status
		// we will delete the job from status and create a new job with the new configuration
		logger.Info("job already exists, but with a different configuration, let's delete the current job from status", "filePath", rawFilePath)
		documentProcessorKey := client.ObjectKeyFromObject(documentProcessorCR)
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			latest := &operatorv1alpha1.DocumentProcessor{}
			if getErr := r.Get(ctx, documentProcessorKey, latest); getErr != nil {
				return getErr
			}
			latest.DeleteJobByFilePath(rawFilePath)
			return r.Status().Update(ctx, latest)
		}); err != nil {
			logger.Error(err, "failed to update DocumentProcessor CR status")
			return false, err
		}
	}

	// job does not exist for this file, let's create a new job
	logger.Info("job does not exist for the file, it will be converted ...", "filePath", rawFilePath)
	return true, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *DocumentProcessorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	labelPredicate := controllerutils.ForceReconcilePredicate()
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.DocumentProcessor{}).
		WithEventFilter(predicate.Or(predicate.GenerationChangedPredicate{}, labelPredicate)).
		Named("documentprocessor").
		Complete(r)
}

func (r *DocumentProcessorReconciler) handleError(ctx context.Context, documentProcessorCR *operatorv1alpha1.DocumentProcessor, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	documentProcessorKey := client.ObjectKeyFromObject(documentProcessorCR)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.DocumentProcessor{}
		if getErr := r.Get(ctx, documentProcessorKey, latest); getErr != nil {
			return getErr
		}
		latest.UpdateStatus("", reconcileErr)
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update DocumentProcessor CR status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, reconcileErr
}
