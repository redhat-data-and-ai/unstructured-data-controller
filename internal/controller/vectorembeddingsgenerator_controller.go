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
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

const (
	VectorEmbeddingsGeneratorControllerName = "VectorEmbeddingsGenerator"
)

type VectorEmbeddingsGeneratorReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	fileStore *filestore.FileStore
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

	// set status to waiting
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.VectorEmbeddingsGenerator{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.SetWaiting()
		return r.Status().Update(ctx, latest)
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

	dataProductName := vectorEmbeddingsGeneratorCR.Spec.DataProduct
	filePaths, err := r.fileStore.ListFilesInPath(ctx, dataProductName)
	logger.Info("files in path", "files", filePaths)
	if err != nil {
		logger.Error(err, "failed to list files in path")
		return r.handleError(ctx, vectorEmbeddingsGeneratorCR, err)
	}

	// now that we have the list of files to process, we may remove the force reconcile label as we are ready to accept more events
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.VectorEmbeddingsGenerator{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		return controllerutils.RemoveForceReconcileLabel(ctx, r.Client, latest)
	}); err != nil {
		logger.Error(err, "failed to remove force reconcile label from VectorEmbeddingsGenerator CR")
		return r.handleError(ctx, vectorEmbeddingsGeneratorCR, err)
	}

	chunksFilePaths := unstructured.FilterChunksFilePaths(filePaths)
	logger.Info("chunks filepaths filtered successfully", "chunksFilePaths", chunksFilePaths)

	embeddingErrors := []error{}
	var embedded bool
	for _, chunksFilePath := range chunksFilePaths {
		logger.Info("processing chunked file for embedding", "file", chunksFilePath)
		fileEmbedded, err := r.processChunkedFile(ctx, chunksFilePath, vectorEmbeddingsGeneratorCR)
		if err != nil {
			embeddingErrors = append(embeddingErrors, err)
			logger.Error(err, "failed to process chunked file", "file", chunksFilePath)
			continue
		}
		if fileEmbedded {
			embedded = true
		}
	}

	if len(embeddingErrors) > 0 {
		logger.Error(embeddingErrors[0], "failed to process some chunked files")
		return r.handleError(ctx, vectorEmbeddingsGeneratorCR, errors.New("failed to process some chunked files"))
	}

	// Add force reconcile to unstructured data product if any of the file got embedded during this reconciliation
	if embedded {
		unstructuredDataProductKey := client.ObjectKey{Namespace: vectorEmbeddingsGeneratorCR.Namespace, Name: vectorEmbeddingsGeneratorCR.Spec.DataProduct}
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			unstructuredDataProduct := &operatorv1alpha1.UnstructuredDataProduct{}
			if err := r.Get(ctx, unstructuredDataProductKey, unstructuredDataProduct); err != nil {
				return err
			}
			return controllerutils.AddForceReconcileLabel(ctx, r.Client, unstructuredDataProduct)
		}); err != nil {
			logger.Error(err, "failed to add force reconcile label to UnstructuredDataProduct CR")
			return r.handleError(ctx, vectorEmbeddingsGeneratorCR, err)
		}
		logger.Info("successfully added force reconcile label to UnstructuredDataProduct CR")
	} else {
		logger.Info("no files were embedded, no need to add force reconcile label to UnstructuredDataProduct CR")
	}

	// all done, let's update the status to ready
	successMessage := fmt.Sprintf("successfully reconciled vector embeddings generator: %s", vectorEmbeddingsGeneratorCR.Name)
	vectorEmbeddingsGeneratorKey := client.ObjectKeyFromObject(vectorEmbeddingsGeneratorCR)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		res := &operatorv1alpha1.VectorEmbeddingsGenerator{}
		if err := r.Get(ctx, vectorEmbeddingsGeneratorKey, res); err != nil {
			return err
		}
		res.UpdateStatus(successMessage, nil)
		return r.Status().Update(ctx, res)
	}); err != nil {
		logger.Error(err, "failed to update VectorEmbeddingsGenerator CR status", "namespace", vectorEmbeddingsGeneratorKey.Namespace, "name", vectorEmbeddingsGeneratorKey.Name)
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

	switch vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.ModelName {
	case "nomic-ai/nomic-embed-text-v1.5":
		endpoint := string(unstructuredSecret.Data[modelMap[Model(vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.ModelName)].Endpoint])
		apiKey := string(unstructuredSecret.Data[modelMap[Model(vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.ModelName)].APIKey])
		embeddingClient = embedding.NewHTTPClient(&embedding.HTTPClientConfig{
			Endpoint:   endpoint,
			APIKey:     apiKey,
			AuthFormat: "Bearer",
			ModelName:  vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.ModelName,
		})
	default:
		return false, fmt.Errorf("unsupported embedding model: %s", vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.ModelName)
	}

	logger.Info("generating embeddings for chunks", "file", chunksFilePath, "chunkCount", len(texts))

	const batchSize = 10
	allEmbeddings := make([][]float64, 0, len(texts))

	for batchStart := 0; batchStart < len(texts); batchStart += batchSize {
		batchEnd := batchStart + batchSize
		if batchEnd > len(texts) {
			batchEnd = len(texts)
		}
		batch := texts[batchStart:batchEnd]

		logger.Info("processing batch", "batchStart", batchStart, "batchEnd", batchEnd, "batchSize", len(batch))
		encodingFormat := vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.NomicEmbedTextV15Config.EncodingFormat
		embeddingResult, err := embeddingClient.GenerateEmbeddings(ctx, batch, encodingFormat)
		// 429 status code indicates usage limit exceeded
		if err != nil && strings.Contains(err.Error(), "API returned status 429: Usage limit exceeded") {
			logger.Info("usage limit exceeded, will retry after 10 seconds", "batchStart", batchStart, "batchEnd", batchEnd)
			time.Sleep(5 * time.Second)
			batchStart -= batchSize
			continue
		} else if err != nil {
			logger.Error(err, "failed to generate embeddings for batch", "batchStart", batchStart, "batchEnd", batchEnd)
			return false, err
		}
		allEmbeddings = append(allEmbeddings, embeddingResult.Embeddings...)
		logger.Info("successfully processed batch", "batchStart", batchStart, "batchEnd", batchEnd, "embeddingsGenerated", len(embeddingResult.Embeddings))
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

	embeddingsFilePath := unstructured.GetVectorEmbeddingsFilePath(chunkedFile.ConvertedDocument.Metadata.RawFilePath)
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

	embeddingsFilePath := unstructured.GetVectorEmbeddingsFilePath(chunkedFile.ConvertedDocument.Metadata.RawFilePath)
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
	key := client.ObjectKeyFromObject(vectorEmbeddingsGeneratorCR)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.VectorEmbeddingsGenerator{}
		if getErr := r.Get(ctx, key, latest); getErr != nil {
			return getErr
		}
		latest.UpdateStatus("", reconcileErr)
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update VectorEmbeddingsGenerator CR status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, reconcileErr
}

// SetupWithManager sets up the controller with the Manager.
func (r *VectorEmbeddingsGeneratorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	labelPredicate := controllerutils.ForceReconcilePredicate()
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.VectorEmbeddingsGenerator{}).
		WithEventFilter(predicate.Or(predicate.GenerationChangedPredicate{}, labelPredicate)).
		Complete(r)
}
