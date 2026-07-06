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
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

const (
	VectorEmbeddingsGeneratorControllerName = "VectorEmbeddingsGenerator"
)

type VectorEmbeddingsGeneratorReconciler struct {
	client.Client
	Scheme     *runtime.Scheme
	fileStore  *filestore.FileStore
	inputPath  string
	outputPath string
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=vectorembeddingsgenerators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=vectorembeddingsgenerators/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=vectorembeddingsgenerators/finalizers,verbs=update

func (r *VectorEmbeddingsGeneratorReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling", "controller", VectorEmbeddingsGeneratorControllerName)

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

	// get the vector embedding generation CR
	vectorEmbeddingsGeneratorCR := &operatorv1alpha1.VectorEmbeddingsGenerator{}
	if err := r.Get(ctx, req.NamespacedName, vectorEmbeddingsGeneratorCR); err != nil {
		logger.Error(err, "failed to get VectorEmbeddingsGenerator CR")
		return ctrl.Result{}, err
	}
	vectorEmbeddingsGeneratorCR = vectorEmbeddingsGeneratorCR.DeepCopy()
	vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.SetDefaults()

	// set status to waiting
	if err := controllerutils.StatusPatch(ctx, r.Client, vectorEmbeddingsGeneratorCR, func() {
		vectorEmbeddingsGeneratorCR.SetWaiting()
	}); err != nil {
		logger.Error(err, "failed to update VectorEmbeddingsGenerator CR status")
		return ctrl.Result{}, err
	}

	// create filestore client
	fs, err := filestore.New(ctx, cacheDirectory, dataStorageBucket)
	if err != nil {
		if IsAWSClientNotInitializedError(err) {
			logger.Info("ControllerConfig has not initialized AWS clients yet, will try again in a bit ...")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		logger.Error(err, "failed to create the filestore client")
		return r.handleError(ctx, vectorEmbeddingsGeneratorCR, err)
	}
	r.fileStore = fs

	pipelineName, err := controllerutils.ParentPipelineNameFromOwnerReference(vectorEmbeddingsGeneratorCR)
	if err != nil {
		return r.handleError(ctx, vectorEmbeddingsGeneratorCR, err)
	}
	r.inputPath = unstructured.StagePath(pipelineName, vectorEmbeddingsGeneratorCR.Spec.DependsOn[0].Name)
	r.outputPath = unstructured.StagePath(pipelineName, vectorEmbeddingsGeneratorCR.Spec.StageName)
	filePaths, err := r.fileStore.ListFilesInPath(ctx, r.inputPath)
	logger.Info("files in path", "count", len(filePaths))
	if err != nil {
		logger.Error(err, "failed to list files in path")
		return r.handleError(ctx, vectorEmbeddingsGeneratorCR, err)
	}

	embeddingErrors := []error{}
	var filesProcessed int64
	for _, chunksFilePath := range filePaths {
		logger.Info("processing chunked file for embedding", "file", chunksFilePath)
		processed, err := r.processChunkedFile(ctx, chunksFilePath, vectorEmbeddingsGeneratorCR)
		if err != nil {
			embeddingErrors = append(embeddingErrors, err)
			logger.Error(err, "failed to process chunked file", "file", chunksFilePath)
			continue
		}
		if processed {
			filesProcessed++
		}
	}

	if len(embeddingErrors) > 0 {
		logger.Error(embeddingErrors[0], "failed to process some chunked files")
		return r.handleError(ctx, vectorEmbeddingsGeneratorCR, errors.New("failed to process some chunked files"))
	}

	// all done, let's update the status to ready
	successMessage := fmt.Sprintf("successfully reconciled vector embeddings generator: %s", vectorEmbeddingsGeneratorCR.Name)
	if err := controllerutils.StatusPatch(ctx, r.Client, vectorEmbeddingsGeneratorCR, func() {
		vectorEmbeddingsGeneratorCR.Status.FilesProcessed += filesProcessed
		vectorEmbeddingsGeneratorCR.Status.AppliedConfig = vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.DeepCopy()
		vectorEmbeddingsGeneratorCR.UpdateStatus(successMessage, nil)
	}); err != nil {
		logger.Error(err, "failed to update VectorEmbeddingsGenerator CR status", "namespace", vectorEmbeddingsGeneratorCR.Namespace, "name", vectorEmbeddingsGeneratorCR.Name)
		return r.handleError(ctx, vectorEmbeddingsGeneratorCR, err)
	}
	logger.Info("successfully updated VectorEmbeddingsGenerator CR status", "status", vectorEmbeddingsGeneratorCR.Status)

	return ctrl.Result{}, nil
}

func (r *VectorEmbeddingsGeneratorReconciler) processChunkedFile(ctx context.Context, chunksFilePath string, vectorEmbeddingsGeneratorCR *operatorv1alpha1.VectorEmbeddingsGenerator) (bool, error) {
	logger := log.FromContext(ctx)
	logger.Info("processing chunked file", "chunksFilePath", chunksFilePath)

	needsEmbedding, err := r.needsEmbedding(ctx, chunksFilePath, vectorEmbeddingsGeneratorCR)
	if err != nil {
		logger.Error(err, "failed to check if file needs embedding")
		return false, err
	}
	if !needsEmbedding {
		logger.Info("file does not need embedding, skipping ...", "file", chunksFilePath)
		return false, nil
	}

	logger.Info("retrieving chunked file from filestore", "file", chunksFilePath)
	chunkedFileRaw, err := r.fileStore.Retrieve(ctx, chunksFilePath)
	if err != nil {
		logger.Error(err, "failed to retrieve chunked file")
		return false, err
	}

	chunkedFile := &unstructured.ChunksFile{}
	if err := json.Unmarshal(chunkedFileRaw, &chunkedFile); err != nil {
		logger.Error(err, "failed to unmarshal chunked file")
		return false, err
	}

	// Validate chunked file structure
	if chunkedFile.ConvertedDocument == nil || chunkedFile.ChunksDocument == nil {
		return false, errors.New("invalid chunks file structure: missing required fields")
	}
	if chunkedFile.ChunksDocument.Chunks == nil || len(chunkedFile.ChunksDocument.Chunks.Text) == 0 {
		logger.Info("chunks file has no text chunks, skipping", "file", chunksFilePath)
		return false, nil
	}

	embeddingFileMetadata := &unstructured.EmbeddingFileMetadata{
		ConvertedFileMetadata:   chunkedFile.ConvertedDocument.Metadata,
		ChunkFileMetadata:       chunkedFile.ChunksDocument.Metadata,
		ModelName:               vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.ModelName,
		NomicEmbedTextV15Config: vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.NomicEmbedTextV15Config,
	}

	texts := make([]string, len(chunkedFile.ChunksDocument.Chunks.Text))
	copy(texts, chunkedFile.ChunksDocument.Chunks.Text)

	var embeddingClient embedding.EmbeddingGenerator

	vegConfig := vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig
	switch vegConfig.ModelName {
	case "nomic-ai/nomic-embed-text-v1.5":
		embeddingClient = embedding.NewHTTPClient(&embedding.HTTPClientConfig{
			Endpoint:   embeddingEndpoint,
			APIKey:     embeddingAPIKey,
			AuthFormat: "Bearer",
			ModelName:  vegConfig.ModelName,
		})
	default:
		return false, fmt.Errorf("unsupported embedding model: %s", vegConfig.ModelName)
	}

	logger.Info("generating embeddings for chunks", "file", chunksFilePath, "chunkCount", len(texts))

	batchSize := vegConfig.BatchSize
	encodingFormat := vegConfig.NomicEmbedTextV15Config.EncodingFormat
	allEmbeddings := make([][]float64, 0, len(texts))

	for batchStart := 0; batchStart < len(texts); batchStart += batchSize {
		batchEnd := min(batchStart+batchSize, len(texts))
		batch := texts[batchStart:batchEnd]

		logger.Info("processing batch", "batchStart", batchStart, "batchEnd", batchEnd, "batchSize", len(batch))
		embeddingResult, err := embeddingClient.GenerateEmbeddings(ctx, batch, encodingFormat)
		if err != nil {
			if strings.Contains(err.Error(), "status 429") {
				logger.Error(err, "embedding API rate limited (429), will retry on next reconciliation", "file", chunksFilePath, "batchStart", batchStart)
			} else {
				logger.Error(err, "failed to generate embeddings for batch", "file", chunksFilePath, "batchStart", batchStart, "batchEnd", batchEnd)
			}
			return false, err
		}
		allEmbeddings = append(allEmbeddings, embeddingResult.Embeddings...)
		logger.Info("successfully processed batch", "batchStart", batchStart, "batchEnd", batchEnd, "embeddingsGenerated", len(embeddingResult.Embeddings))
	}

	if len(allEmbeddings) != len(texts) {
		err := fmt.Errorf("embedding count mismatch: expected %d, got %d", len(texts), len(allEmbeddings))
		logger.Error(err, "embedding count does not match input text count", "file", chunksFilePath)
		return false, err
	}

	logger.Info("successfully generated embeddings", "file", chunksFilePath, "embeddingCount", len(allEmbeddings))

	// rearrange the embeddings
	embeddings := make([]*unstructured.Embeddings, len(allEmbeddings))
	for i, embeddingVector := range allEmbeddings {
		embeddings[i] = &unstructured.Embeddings{
			Text:      texts[i],
			Embedding: embeddingVector,
		}
	}

	// Create the complete embeddings file structure
	embeddingsFile := &unstructured.EmbeddingsFile{
		ConvertedDocument: chunkedFile.ConvertedDocument,
		ChunksDocument:    chunkedFile.ChunksDocument,
		EmbeddingDocument: &unstructured.EmbeddingDocument{
			Metadata:   embeddingFileMetadata,
			Embeddings: embeddings,
		},
	}

	embeddingsFileBytes, err := json.Marshal(embeddingsFile)
	if err != nil {
		logger.Error(err, "failed to marshal embeddings file")
		return false, err
	}

	embeddingsFilePath := unstructured.RemapToOutputDir(chunksFilePath, r.inputPath, r.outputPath)
	logger.Info("storing embedded file", "embeddingsFilePath", embeddingsFilePath)
	if err := r.fileStore.Store(ctx, embeddingsFilePath, embeddingsFileBytes); err != nil {
		logger.Error(err, "failed to store embedded file")
		return false, err
	}

	logger.Info("successfully processed and stored embedded file", "file", chunksFilePath, "embeddingsFile", embeddingsFilePath)
	return true, nil
}

func (r *VectorEmbeddingsGeneratorReconciler) needsEmbedding(ctx context.Context, chunksFilePath string, vectorEmbeddingsGeneratorCR *operatorv1alpha1.VectorEmbeddingsGenerator) (bool, error) {
	logger := log.FromContext(ctx)
	logger.Info("checking if file needs embedding", "file", chunksFilePath)

	chunksFileExists, err := r.fileStore.Exists(ctx, chunksFilePath)
	if err != nil {
		return false, err
	}
	if !chunksFileExists {
		err := fmt.Errorf("chunked file %s does not exist", chunksFilePath)
		logger.Error(err, "chunked file does not exist", "file", chunksFilePath)
		return false, err
	}

	chunkedFileRaw, err := r.fileStore.Retrieve(ctx, chunksFilePath)
	if err != nil {
		return false, err
	}

	chunkedFile := &unstructured.ChunksFile{}
	if err := json.Unmarshal(chunkedFileRaw, &chunkedFile); err != nil {
		return false, err
	}

	embeddingsFilePath := unstructured.RemapToOutputDir(chunksFilePath, r.inputPath, r.outputPath)
	logger.Info("embeddings file path", "embeddingsFilePath", embeddingsFilePath)
	embeddingsFileExists, err := r.fileStore.Exists(ctx, embeddingsFilePath)
	if err != nil {
		return false, err
	}

	if embeddingsFileExists {
		embeddingsFileRaw, err := r.fileStore.Retrieve(ctx, embeddingsFilePath)
		if err != nil {
			return false, err
		}

		currentEmbeddedFile := &unstructured.EmbeddingsFile{}
		if err := json.Unmarshal(embeddingsFileRaw, &currentEmbeddedFile); err != nil {
			logger.Info("embeddings file exists but cannot be parsed, will re-embed", "file", chunksFilePath, "error", err)
			return true, nil
		}

		// Check if the embedded file structure is valid
		if currentEmbeddedFile.EmbeddingDocument == nil || currentEmbeddedFile.EmbeddingDocument.Metadata == nil {
			logger.Info("embeddings file exists but has invalid structure, will re-embed", "file", chunksFilePath)
			return true, nil
		}

		fileToEmbedMetadata := &unstructured.EmbeddingFileMetadata{
			ConvertedFileMetadata:   chunkedFile.ConvertedDocument.Metadata,
			ChunkFileMetadata:       chunkedFile.ChunksDocument.Metadata,
			ModelName:               vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.ModelName,
			NomicEmbedTextV15Config: vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.NomicEmbedTextV15Config,
		}

		if currentEmbeddedFile.EmbeddingDocument.Metadata.Equal(fileToEmbedMetadata) {
			logger.Info("embeddings file has the same configuration, no embedding needed", "file", chunksFilePath)
			return false, nil
		}

		logger.Info("embeddings file exists but with different configuration, will re-embed", "file", chunksFilePath)
	}

	logger.Info("file needs embedding", "file", chunksFilePath)
	return true, nil
}

func (r *VectorEmbeddingsGeneratorReconciler) handleError(ctx context.Context, vectorEmbeddingsGeneratorCR *operatorv1alpha1.VectorEmbeddingsGenerator, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	if updateErr := controllerutils.StatusPatch(ctx, r.Client, vectorEmbeddingsGeneratorCR, func() {
		vectorEmbeddingsGeneratorCR.UpdateStatus("", reconcileErr)
	}); updateErr != nil {
		logger.Error(updateErr, "failed to update VectorEmbeddingsGenerator CR status")
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{}, reconcileErr
}

func (r *VectorEmbeddingsGeneratorReconciler) findDependents(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &operatorv1alpha1.VectorEmbeddingsGeneratorList{}
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
func (r *VectorEmbeddingsGeneratorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.VectorEmbeddingsGenerator{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&operatorv1alpha1.SourceCrawler{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.DocumentProcessor{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.ChunksGenerator{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.DestinationSyncer{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Complete(r)
}
