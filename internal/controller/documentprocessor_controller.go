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
	"strconv"
	"strings"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

const (
	requeueAfter                    = 15 * time.Second
	DocumentProcessorControllerName = "DocumentProcessor"
)

var (
	maxDoclingConversionAttempts = 3
)

// DocumentProcessorReconciler reconciles a DocumentProcessor object
type DocumentProcessorReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	fileStore *filestore.FileStore
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=documentprocessors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=documentprocessors/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=documentprocessors/finalizers,verbs=update

func (r *DocumentProcessorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling", "controller", DocumentProcessorControllerName)

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
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	documentProcessorCR = documentProcessorCR.DeepCopy()
	documentProcessorCR.Spec.DocumentProcessorConfig.SetDefaults()

	// set status to waiting
	if err := controllerutils.StatusPatch(ctx, r.Client, documentProcessorCR, func() {
		documentProcessorCR.SetWaiting()
	}); err != nil {
		logger.Error(err, "failed to update DocumentProcessor CR status")
		return ctrl.Result{}, err
	}

	cfg := documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig
	doclingCfg := &docling.DoclingConfig{
		FromFormats:                     cfg.FromFormats,
		ToFormats:                       cfg.ToFormats,
		ImageExportMode:                 cfg.ImageExportMode,
		DoOCR:                           cfg.DoOCR,
		ForceOCR:                        cfg.ForceOCR,
		OCREngine:                       cfg.OCREngine, //nolint:staticcheck // backwards compatibility
		OCRLang:                         cfg.OCRLang,
		OCRPreset:                       cfg.OCRPreset,
		PDFBackend:                      cfg.PDFBackend,
		Pipeline:                        cfg.Pipeline,
		TableMode:                       cfg.TableMode,
		TableCellMatching:               cfg.TableCellMatching,
		DoTableStructure:                cfg.DoTableStructure,
		IncludeImages:                   cfg.IncludeImages,
		ImagesScale:                     parseFloat64Ptr(cfg.ImagesScale),
		DoCodeEnrichment:                cfg.DoCodeEnrichment,
		DoFormulaEnrichment:             cfg.DoFormulaEnrichment,
		DoPictureClassification:         cfg.DoPictureClassification,
		DoPictureDescription:            cfg.DoPictureDescription,
		DoChartExtraction:               cfg.DoChartExtraction,
		PictureDescriptionAreaThreshold: parseFloat64Ptr(cfg.PictureDescriptionAreaThreshold),
		DocumentTimeout:                 parseFloat64Ptr(cfg.DocumentTimeout),
		PageRange:                       cfg.PageRange,
		MdPageBreakPlaceholder:          cfg.MdPageBreakPlaceholder,
	}

	fs, err := filestore.New(ctx, cacheDirectory, dataStorageBucket)
	if err != nil {
		if IsAWSClientNotInitializedError(err) {
			logger.Info("ControllerConfig has not initialized AWS clients yet, will try again in a bit ...")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		logger.Error(err, "failed to create filestore")
		return r.handleError(ctx, documentProcessorCR, err)
	}
	r.fileStore = fs

	// read from upstream stage directory, write to own stage directory
	pipelineName, err := controllerutils.ParentPipelineNameFromOwnerReference(documentProcessorCR)
	if err != nil {
		return r.handleError(ctx, documentProcessorCR, err)
	}
	inputPath := unstructured.StagePath(pipelineName, documentProcessorCR.Spec.DependsOn[0].Name)
	outputPath := unstructured.StagePath(pipelineName, documentProcessorCR.Spec.StageName)

	// first, let's figure out the jobs that are currently running
	jobProcessingErrors := []error{}
	for _, job := range documentProcessorCR.Status.Jobs {
		logger.Info("reconciling job", "job", job)
		if err := r.reconcileJob(ctx, job, documentProcessorCR, inputPath, outputPath); err != nil {
			jobProcessingErrors = append(jobProcessingErrors, err)
			logger.Error(err, "failed to process job", "job", job.FilePath)
		}
	}

	filePaths, err := r.fileStore.ListFilesInPath(ctx, inputPath)
	if err != nil {
		logger.Error(err, "failed to list files in path")
		return r.handleError(ctx, documentProcessorCR, err)
	}

	documentProcessingErrors := []error{}
	rawFilePaths := unstructured.FilterRawFilePaths(filePaths)
	for _, rawFilePath := range rawFilePaths {
		logger.Info("processing document", "document", rawFilePath)
		if err := r.processDocument(ctx, rawFilePath, documentProcessorCR, doclingCfg, inputPath, outputPath); err != nil {
			documentProcessingErrors = append(documentProcessingErrors, err)
			logger.Error(err, "failed to process document", "document", rawFilePath)
		}
	}

	if len(documentProcessingErrors) > 0 || len(jobProcessingErrors) > 0 {
		return r.handleError(ctx, documentProcessorCR, errors.New("failed to process jobs or documents"))
	}

	if len(documentProcessorCR.Status.Jobs) > 0 {
		logger.Info("some jobs are still pending or running, will requeue after a bit ...")
		return ctrl.Result{
			RequeueAfter: requeueAfter,
		}, nil
	}

	logger.Info("all jobs are completed, no need to requeue")

	successMessage := fmt.Sprintf("successfully reconciled document processor: %s", documentProcessorCR.Name)
	if err := controllerutils.StatusPatch(ctx, r.Client, documentProcessorCR, func() {
		documentProcessorCR.UpdateStatus(successMessage, nil)
	}); err != nil {
		logger.Error(err, "failed to update DocumentProcessor CR status", "namespace", documentProcessorCR.Namespace, "name", documentProcessorCR.Name)
		return r.handleError(ctx, documentProcessorCR, err)
	}
	logger.Info("successfully updated DocumentProcessor CR status", "status", documentProcessorCR.Status)

	return ctrl.Result{}, nil
}

func (r *DocumentProcessorReconciler) reconcileJob(ctx context.Context, job operatorv1alpha1.Job, documentProcessorCR *operatorv1alpha1.DocumentProcessor, inputPath, outputPath string) (err error) {
	logger := log.FromContext(ctx)
	// recover from panic semaphore panic
	defer func() {
		if panicErr := recover(); panicErr != nil {
			logger.Info("recovered from panic in processJob, likely stale job from previous session", "panic", panicErr, "taskID", job.TaskID, "filePath", job.FilePath)
			if panicMessage, ok := panicErr.(string); ok && strings.Contains(panicMessage, docling.SemaphorePanicError) {
				logger.Info("semaphore panic detected, removing stale job", "taskID", job.TaskID)
				if updateErr := controllerutils.StatusPatch(ctx, r.Client, documentProcessorCR, func() {
					documentProcessorCR.DeleteJobByFilePath(job.FilePath)
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
			FileIdentifier:    job.FileIdentifier,
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
		convertedFilePath := unstructured.RemapToOutputDir(job.FilePath+".json", inputPath, outputPath)
		if err := r.fileStore.Store(ctx, convertedFilePath, convertedFileBytes); err != nil {
			return err
		}
		logger.Info("successfully stored the converted file in the filestore", "filePath", convertedFilePath)

		// remove the job from the status as nothing needs to be done for this job
		if updateErr := controllerutils.StatusPatch(ctx, r.Client, documentProcessorCR, func() {
			documentProcessorCR.DeleteJobByFilePath(job.FilePath)
			documentProcessorCR.Status.FilesProcessed++
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
			if updateErr := controllerutils.StatusPatch(ctx, r.Client, documentProcessorCR, func() {
				documentProcessorCR.AddPermanentlyFailingFile(job.FilePath)
				documentProcessorCR.DeleteJobByFilePath(job.FilePath)
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

func (r *DocumentProcessorReconciler) processDocument(ctx context.Context, rawFilePath string, documentProcessorCR *operatorv1alpha1.DocumentProcessor, doclingCfg *docling.DoclingConfig, inputPath, outputPath string) error {
	logger := log.FromContext(ctx)
	logger.Info("processing document", "rawFilePath", rawFilePath)

	// check if the document needs to be converted
	needsConversion, err := r.needsConversion(ctx, rawFilePath, documentProcessorCR, inputPath, outputPath)
	if err != nil {
		logger.Error(err, "failed to check if document needs conversion")
		return err
	}
	if !needsConversion {
		logger.Info("document does not need conversion, skipping ...")
		return nil
	}

	// fetch the File UID
	fileUID, err := r.getFileUID(ctx, rawFilePath)
	if err != nil {
		logger.Error(err, "Failed to get file UID")
		return err
	}

	// create a new job for the document
	fileURL, err := r.fileStore.GetFileURL(ctx, rawFilePath)
	if err != nil {
		logger.Error(err, "failed to get file URL")
		return err
	}
	response, err := doclingClient.ConvertFile(ctx, fileURL, *doclingCfg)
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

	if err := controllerutils.StatusPatch(ctx, r.Client, documentProcessorCR, func() {
		documentProcessorCR.AddOrUpdateJob(operatorv1alpha1.Job{
			FilePath:          rawFilePath,
			FileIdentifier:    fileUID,
			DocumentConverter: string(unstructured.DocumentConverterDocling),
			DoclingConfig:     documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig,
			TaskID:            response.TaskID,
			Status:            response.TaskStatus,
			CreatedOn:         time.Now().Format(time.RFC3339),
			Attempts:          conversionAttempt,
		})
	}); err != nil {
		logger.Error(err, "failed to update DocumentProcessor CR status")
		return err
	}

	return nil
}

func (r *DocumentProcessorReconciler) needsConversion(ctx context.Context, rawFilePath string, documentProcessorCR *operatorv1alpha1.DocumentProcessor, inputPath, outputPath string) (bool, error) {
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

	// check whether metadata file exists
	rawMetadataFileExists, err := r.fileStore.Exists(ctx, unstructured.MetadataPath(rawFilePath))
	if err != nil {
		return false, err
	}
	if !rawMetadataFileExists {
		logger.Info("metadata file does not exist", "filePath", rawFilePath)
		return false, nil
	}

	// fetch the File UID
	fileUID, err := r.getFileUID(ctx, rawFilePath)
	if err != nil {
		logger.Error(err, "Failed to get file UID")
		return false, err
	}

	// does the converted file exist in the output directory?
	convertedFilePath := unstructured.RemapToOutputDir(rawFilePath+".json", inputPath, outputPath)
	convertedFileExists, err := r.fileStore.Exists(ctx, convertedFilePath)
	if err != nil {
		return false, err
	}
	if convertedFileExists {
		// if the converted file exists, then we need to check if it has the same configuration
		convertedFileRaw, err := r.fileStore.Retrieve(ctx, convertedFilePath)
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
			FileIdentifier:    fileUID,
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
			FileIdentifier:    fileUID,
			DocumentConverter: unstructured.DocumentConverterDocling,
			DoclingConfig:     documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig,
		}
		fileInJob := unstructured.ConvertedFileMetadata{
			RawFilePath:       job.FilePath,
			FileIdentifier:    job.FileIdentifier,
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
		if err := controllerutils.StatusPatch(ctx, r.Client, documentProcessorCR, func() {
			documentProcessorCR.DeleteJobByFilePath(rawFilePath)
		}); err != nil {
			logger.Error(err, "failed to update DocumentProcessor CR status")
			return false, err
		}
	}

	// job does not exist for this file, let's create a new job
	logger.Info("job does not exist for the file, it will be converted ...", "filePath", rawFilePath)
	return true, nil
}

func (r *DocumentProcessorReconciler) getFileUID(ctx context.Context, rawFilePath string) (string, error) {
	logger := log.FromContext(ctx)
	metaDataFileRaw, err := r.fileStore.Retrieve(ctx, unstructured.MetadataPath(rawFilePath))
	if err != nil {
		logger.Error(err, "Failed to retrieve metadata file")
		return "", err
	}

	metaDataFile := unstructured.RawFileMetadata{}
	err = json.Unmarshal(metaDataFileRaw, &metaDataFile)
	if err != nil {
		logger.Error(err, "Failed to unmarshal metadata file")
		return "", err
	}

	return metaDataFile.UID, nil
}

func (r *DocumentProcessorReconciler) findDependents(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &operatorv1alpha1.DocumentProcessorList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	changedName := obj.GetName()
	var requests []reconcile.Request
	for _, item := range list.Items {
		pipelineName, err := controllerutils.ParentPipelineNameFromOwnerReference(&item)
		if err != nil {
			continue
		}
		for _, dep := range item.Spec.DependsOn {
			if pipelineName+"-"+dep.Name == changedName {
				requests = append(requests, reconcile.Request{
					NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace},
				})
				break
			}
		}
	}
	return requests
}

// SetupWithManager sets up the controller with the Manager.
// SetupWithManager registers this controller. Spec changes trigger via GenerationChangedPredicate.
// Watches on other stage types trigger reconcile when an upstream dependency's status changes.
func (r *DocumentProcessorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.DocumentProcessor{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&operatorv1alpha1.SourceCrawler{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.ChunksGenerator{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.VectorEmbeddingsGenerator{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.DestinationSyncer{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Named("documentprocessor").
		Complete(r)
}

func (r *DocumentProcessorReconciler) handleError(ctx context.Context, documentProcessorCR *operatorv1alpha1.DocumentProcessor, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	if err := controllerutils.StatusPatch(ctx, r.Client, documentProcessorCR, func() {
		documentProcessorCR.UpdateStatus("", reconcileErr)
	}); err != nil {
		logger.Error(err, "failed to update DocumentProcessor CR status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, reconcileErr
}

func parseFloat64Ptr(s string) *float64 {
	if s == "" {
		return nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return nil
	}
	return &v
}
