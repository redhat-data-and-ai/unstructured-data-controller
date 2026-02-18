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
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/langchain"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
	"github.com/tmc/langchaingo/textsplitter"
)

const (
	ChunksGeneratorControllerName = "ChunksGenerator"
)

// ChunksGeneratorReconciler reconciles a ChunksGenerator object
type ChunksGeneratorReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	fileStore *filestore.FileStore
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=chunksgenerators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=chunksgenerators/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=chunksgenerators/finalizers,verbs=update
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddataproducts,verbs=get;update;patch

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ChunksGenerator object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *ChunksGeneratorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling ChunksGenerator")

	// check if config CR is healthy
	isHealthy, err := IsConfigCRHealthy(ctx, r.Client, req.Namespace)
	if err != nil {
		logger.Error(err, "failed to check if ControllerConfig CR is healthy")
		return ctrl.Result{}, err
	}

	if !isHealthy {
		logger.Info("ControllerConfig CR is not ready yet, will try again in a bit ...")
		return ctrl.Result{
			RequeueAfter: 10 * time.Second,
		}, nil
	}

	chunksGeneratorCR := &operatorv1alpha1.ChunksGenerator{}
	if err := r.Get(ctx, req.NamespacedName, chunksGeneratorCR); err != nil {
		logger.Error(err, "failed to get ChunksGenerator CR")
		return ctrl.Result{}, err
	}

	// set status to waiting
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.ChunksGenerator{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.SetWaiting()
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update ChunksGenerator CR status")
		return ctrl.Result{}, err
	}

	fs, err := filestore.New(ctx, cacheDirectory, dataStorageBucket)
	if err != nil {
		logger.Error(err, "failed to create filestore")
		return r.handleError(ctx, chunksGeneratorCR, err)
	}
	r.fileStore = fs

	// first fetch the converted files from the filestore for the data product
	dataProductName := chunksGeneratorCR.Spec.DataProduct
	filePaths, err := r.fileStore.ListFilesInPath(ctx, dataProductName)
	if err != nil {
		logger.Error(err, "failed to list files in path")
		return r.handleError(ctx, chunksGeneratorCR, err)
	}

	// now that we have the list of files to process, we may remove the force reconcile label as we are ready to accept more events
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.ChunksGenerator{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		return controllerutils.RemoveForceReconcileLabel(ctx, r.Client, latest)
	}); err != nil {
		logger.Error(err, "failed to remove force reconcile label")
		return r.handleError(ctx, chunksGeneratorCR, err)
	}

	chunkingErrors := []error{}
	skippedFiles := []string{}

	convertedFilePaths := unstructured.FilterConvertedFilePaths(filePaths)

	for _, convertedFilePath := range convertedFilePaths {
		logger.Info("processing converted file", "file", convertedFilePath)

		if err := r.processConvertedFile(ctx, convertedFilePath, chunksGeneratorCR); err != nil {
			if strings.Contains(err.Error(), langchain.SemaphoreAcquireError) {
				logger.Error(err, "failed to process converted file, semaphore acquire error, will try again later", "file", convertedFilePath)
				skippedFiles = append(skippedFiles, convertedFilePath)
				continue
			}

			chunkingErrors = append(chunkingErrors, err)
			logger.Error(err, "failed to process converted file", "file", convertedFilePath)
		}
		logger.Info("successfully processed converted file", "file", convertedFilePath)
	}

	if len(chunkingErrors) > 0 {
		err := fmt.Errorf("failed to chunk some files: %v", chunkingErrors)
		logger.Error(err, "chunking error")
		return r.handleError(ctx, chunksGeneratorCR, err)
	}

	if len(skippedFiles) > 0 {
		logger.Info("some files were skipped, will requeue after a bit ...")
		return ctrl.Result{
			RequeueAfter: requeueAfter,
		}, nil
	}

	successMessage := fmt.Sprintf("successfully reconciled chunks generator: %s", chunksGeneratorCR.Name)
	key := client.ObjectKey{Namespace: chunksGeneratorCR.Namespace, Name: chunksGeneratorCR.Name}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		res := &operatorv1alpha1.ChunksGenerator{}
		if err := r.Get(ctx, key, res); err != nil {
			return err
		}
		res.UpdateStatus(successMessage, nil)
		return r.Status().Update(ctx, res)
	}); err != nil {
		logger.Error(err, "failed to update ChunksGenerator CR status", "namespace", key.Namespace, "name", key.Name)
		return r.handleError(ctx, chunksGeneratorCR, err)
	}
	logger.Info("successfully updated ChunksGenerator CR status", "status", chunksGeneratorCR.Status)

	// Trigger UnstructuredDataProduct to sync chunk files to destination (destination-only reconcile).
	unstructuredDataProductKey := client.ObjectKey{Namespace: chunksGeneratorCR.Namespace, Name: chunksGeneratorCR.Spec.DataProduct}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		unstructuredDataProductCR := &operatorv1alpha1.UnstructuredDataProduct{}
		if err := r.Get(ctx, unstructuredDataProductKey, unstructuredDataProductCR); err != nil {
			return err
		}
		return controllerutils.AddForceReconcileAndSyncDestinationLabel(ctx, r.Client, unstructuredDataProductCR)
	}); err != nil {
		logger.Error(err, "failed to add force reconcile label to UnstructuredDataProduct CR")
		return r.handleError(ctx, chunksGeneratorCR, err)
	}

	return ctrl.Result{}, nil
}

func (r *ChunksGeneratorReconciler) processConvertedFile(ctx context.Context, convertedFilePath string, chunksGeneratorCR *operatorv1alpha1.ChunksGenerator) error {
	logger := log.FromContext(ctx)
	logger.Info("processing converted file", "file", convertedFilePath)

	rawFilePath := unstructured.GetRawFilePathFromConvertedFilePath(convertedFilePath)
	chunksFilePath := unstructured.GetChunksFilePath(rawFilePath)

	// figure out if the file is already chunked
	needsChunking, err := r.needsChunking(ctx, convertedFilePath, chunksGeneratorCR)
	if err != nil {
		logger.Error(err, "failed to check if file needs chunking")
		return err
	}
	if !needsChunking {
		logger.Info("file is already chunked, skipping ...")
		return nil
	}

	// chunk the file
	chunksFile, err := r.chunkFile(ctx, convertedFilePath, chunksGeneratorCR)
	if err != nil {
		logger.Error(err, "failed to chunk file")
		return err
	}

	// store the chunks in the filestore
	chunksFileBytes, err := json.Marshal(chunksFile)
	if err != nil {
		logger.Error(err, "failed to marshal chunks file")
		return err
	}
	if err := r.fileStore.Store(ctx, chunksFilePath, chunksFileBytes); err != nil {
		logger.Error(err, "failed to store chunks file")
		return err
	}
	return nil
}

func (r *ChunksGeneratorReconciler) needsChunking(ctx context.Context, convertedFilePath string, chunksGeneratorCR *operatorv1alpha1.ChunksGenerator) (bool, error) {
	logger := log.FromContext(ctx)
	logger.Info("checking if file needs chunking", "file", convertedFilePath)

	rawFilePath := unstructured.GetRawFilePathFromConvertedFilePath(convertedFilePath)
	chunksFilePath := unstructured.GetChunksFilePath(rawFilePath)

	// fetch the converted file from the filestore
	// this will also make sure that the converted file exists in the filestore
	convertedFileRaw, err := r.fileStore.Retrieve(ctx, convertedFilePath)
	if err != nil {
		return false, err
	}

	convertedFileMetadata := unstructured.ConvertedFileMetadata{}
	err = json.Unmarshal(convertedFileRaw, &convertedFileMetadata)
	if err != nil {
		return false, err
	}

	// check if the chunked file does not exist in the filestore then return true
	chunksFileExists, err := r.fileStore.Exists(ctx, chunksFilePath)
	if err != nil {
		return false, err
	}
	if !chunksFileExists {
		return true, nil
	}

	// if the chunked file exists, then we need to check if it has the same configuration
	chunksFileRaw, err := r.fileStore.Retrieve(ctx, chunksFilePath)
	if err != nil {
		return false, err
	}

	chunksFile := unstructured.ChunksFile{}
	err = json.Unmarshal(chunksFileRaw, &chunksFile)
	if err != nil {
		return false, err
	}

	// now the chunks file should be the same as the current chunks file in filestore
	newChunksFileMetadata := unstructured.ChunksFileMetadata{
		ConvertedFileMetadata: &convertedFileMetadata,
		ChunkingTool:          unstructured.LangchainChunkingTool,
		ChunksGeneratorConfig: chunksGeneratorCR.Spec.ChunksGeneratorConfig,
	}
	if !chunksFile.ChunksDocument.Metadata.Equal(&newChunksFileMetadata) {
		logger.Info("chunks file is the same as the current chunks file in filestore, no need to chunk again", "file", convertedFilePath)
		return true, nil
	}

	return false, nil
}

func (r *ChunksGeneratorReconciler) chunkFile(ctx context.Context, convertedFilePath string, chunksGeneratorCR *operatorv1alpha1.ChunksGenerator) (*unstructured.ChunksFile, error) {
	logger := log.FromContext(ctx)
	logger.Info("chunking file", "file", convertedFilePath)

	// read the converted file from the filestore
	convertedFileRaw, err := r.fileStore.Retrieve(ctx, convertedFilePath)
	if err != nil {
		return nil, err
	}

	convertedFile := unstructured.ConvertedFile{}
	err = json.Unmarshal(convertedFileRaw, &convertedFile)
	if err != nil {
		return nil, err
	}

	var chunker unstructured.Chunker
	switch chunksGeneratorCR.Spec.ChunksGeneratorConfig.Strategy {
	case operatorv1alpha1.ChunkingStrategyRecursiveCharacter:
		chunker = &unstructured.RecursiveCharacterSplitter{
			LangchainClient: langchainClient,
			Config: &textsplitter.Options{
				Separators:    chunksGeneratorCR.Spec.ChunksGeneratorConfig.RecursiveCharacterSplitterConfig.Separators,
				ChunkSize:     chunksGeneratorCR.Spec.ChunksGeneratorConfig.RecursiveCharacterSplitterConfig.ChunkSize,
				ChunkOverlap:  chunksGeneratorCR.Spec.ChunksGeneratorConfig.RecursiveCharacterSplitterConfig.ChunkOverlap,
				KeepSeparator: chunksGeneratorCR.Spec.ChunksGeneratorConfig.RecursiveCharacterSplitterConfig.KeepSeparator,
			},
		}
	case operatorv1alpha1.ChunkingStrategyMarkdown:
		chunker = &unstructured.MarkdownSplitter{
			LangchainClient: langchainClient,
			Config: &textsplitter.Options{
				ChunkSize:            chunksGeneratorCR.Spec.ChunksGeneratorConfig.MarkdownSplitterConfig.ChunkSize,
				ChunkOverlap:         chunksGeneratorCR.Spec.ChunksGeneratorConfig.MarkdownSplitterConfig.ChunkOverlap,
				CodeBlocks:           chunksGeneratorCR.Spec.ChunksGeneratorConfig.MarkdownSplitterConfig.CodeBlocks,
				ReferenceLinks:       chunksGeneratorCR.Spec.ChunksGeneratorConfig.MarkdownSplitterConfig.ReferenceLinks,
				KeepHeadingHierarchy: chunksGeneratorCR.Spec.ChunksGeneratorConfig.MarkdownSplitterConfig.HeadingHierarchy,
				JoinTableRows:        chunksGeneratorCR.Spec.ChunksGeneratorConfig.MarkdownSplitterConfig.JoinTableRows,
			},
		}
	case operatorv1alpha1.ChunkingStrategyToken:
		chunker = &unstructured.TokenSplitter{
			LangchainClient: langchainClient,
			Config: &textsplitter.Options{
				ChunkSize:         chunksGeneratorCR.Spec.ChunksGeneratorConfig.TokenSplitterConfig.ChunkSize,
				ChunkOverlap:      chunksGeneratorCR.Spec.ChunksGeneratorConfig.TokenSplitterConfig.ChunkOverlap,
				ModelName:         chunksGeneratorCR.Spec.ChunksGeneratorConfig.TokenSplitterConfig.ModelName,
				EncodingName:      chunksGeneratorCR.Spec.ChunksGeneratorConfig.TokenSplitterConfig.EncodingName,
				AllowedSpecial:    chunksGeneratorCR.Spec.ChunksGeneratorConfig.TokenSplitterConfig.AllowedSpecial,
				DisallowedSpecial: chunksGeneratorCR.Spec.ChunksGeneratorConfig.TokenSplitterConfig.DisallowedSpecial,
			},
		}
	default:
		return nil, fmt.Errorf("invalid strategy: %s", chunksGeneratorCR.Spec.ChunksGeneratorConfig.Strategy)
	}

	chunks, err := chunker.Chunk(convertedFile.ConvertedDocument.Content.Markdown)
	if err != nil {
		return nil, err
	}

	return &unstructured.ChunksFile{
		ConvertedDocument: convertedFile.ConvertedDocument,
		ChunksDocument: &unstructured.ChunksDocument{
			Metadata: &unstructured.ChunksFileMetadata{
				ChunkingTool:          unstructured.LangchainChunkingTool,
				ChunksGeneratorConfig: chunksGeneratorCR.Spec.ChunksGeneratorConfig,
				ConvertedFileMetadata: convertedFile.ConvertedDocument.Metadata,
			},
			Chunks: &unstructured.Chunks{
				Text: chunks,
			},
		},
	}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ChunksGeneratorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	labelPredicate := controllerutils.ForceReconcilePredicate()
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.ChunksGenerator{}).
		WithEventFilter(predicate.Or(predicate.GenerationChangedPredicate{}, labelPredicate)).
		Complete(r)
}
func (r *ChunksGeneratorReconciler) handleError(ctx context.Context, chunksGeneratorCR *operatorv1alpha1.ChunksGenerator, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	key := client.ObjectKeyFromObject(chunksGeneratorCR)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.ChunksGenerator{}
		if getErr := r.Get(ctx, key, latest); getErr != nil {
			return getErr
		}
		latest.UpdateStatus("", reconcileErr)
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update ChunksGenerator CR status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, reconcileErr
}
