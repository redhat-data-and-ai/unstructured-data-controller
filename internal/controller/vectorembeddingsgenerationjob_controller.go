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
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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
	VectorEmbeddingsGenerationJobControllerName = "VectorEmbeddingsGenerationJob"
)

type VectorEmbeddingsGenerationJobReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	fileStore *filestore.FileStore
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=vectorembeddingsgenerationjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=vectorembeddingsgenerationjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=vectorembeddingsgenerationjobs/finalizers,verbs=update

func (r *VectorEmbeddingsGenerationJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling VectorEmbeddingsGenerationJob")

	// get the vector embedding generation CR
	vectorEmbeddingsGenerationJobCR := &operatorv1alpha1.VectorEmbeddingsGenerationJob{}
	if err := r.Get(ctx, req.NamespacedName, vectorEmbeddingsGenerationJobCR); err != nil {
		logger.Error(err, "failed to get VectorEmbeddingsGenerationJob CR")
		return ctrl.Result{}, err
	}

	// set status to waiting
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.VectorEmbeddingsGenerationJob{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.SetWaiting()
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update VectorEmbeddingsGenerationJob CR status")
		return ctrl.Result{}, err
	}

	// read the kubernetes secret named unstructured-secret
	apiKeySecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      "unstructured-secret",
		Namespace: vectorEmbeddingsGenerationJobCR.Namespace,
	}, apiKeySecret); err != nil {
		logger.Error(err, "failed to get API key secret")
		return r.handleError(ctx, vectorEmbeddingsGenerationJobCR, err)
	}

	// create filestore client
	fs, err := filestore.New(ctx, cacheDirectory, dataStorageBucket)
	if err != nil {
		logger.Error(err, "failed to create the filestore client")
		return r.handleError(ctx, vectorEmbeddingsGenerationJobCR, err)
	}
	r.fileStore = fs

	dataProductName := vectorEmbeddingsGenerationJobCR.Spec.DataProduct
	filePaths, err := r.fileStore.ListFilesInPath(ctx, dataProductName)
	logger.Info("files in path", "files", filePaths)
	if err != nil {
		logger.Error(err, "failed to list files in path")
		return r.handleError(ctx, vectorEmbeddingsGenerationJobCR, err)
	}

	// now that we have the list of files to process, we may remove the force reconcile label as we are ready to accept more events
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.VectorEmbeddingsGenerationJob{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		return controllerutils.RemoveForceReconcileLabel(ctx, r.Client, latest)
	}); err != nil {
		logger.Error(err, "failed to remove force reconcile label from VectorEmbeddingsGenerationJob CR")
		return r.handleError(ctx, vectorEmbeddingsGenerationJobCR, err)
	}

	chunksFilePaths := unstructured.FilterChunksFilePaths(filePaths)
	logger.Info("chunks filepaths filtered successfully", "chunksFilePaths", chunksFilePaths)

	embeddingErrors := []error{}
	var embedded bool
	for _, chunksFilePath := range chunksFilePaths {
		logger.Info("processing chunked file for embedding", "file", chunksFilePath)
		fileEmbedded, err := r.processChunkedFile(ctx, chunksFilePath, vectorEmbeddingsGenerationJobCR, apiKeySecret)
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
		return r.handleError(ctx, vectorEmbeddingsGenerationJobCR, errors.New("failed to process some chunked files"))
	}

	// Add force reconcile to unstructured data product if any of the file got embedded during this reconciliation
	if embedded {
		unstructuredDataProductKey := client.ObjectKey{Namespace: vectorEmbeddingsGenerationJobCR.Namespace, Name: vectorEmbeddingsGenerationJobCR.Spec.DataProduct}
		if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
			unstructuredDataProduct := &operatorv1alpha1.UnstructuredDataProduct{}
			if err := r.Get(ctx, unstructuredDataProductKey, unstructuredDataProduct); err != nil {
				return err
			}
			return controllerutils.AddForceReconcileLabel(ctx, r.Client, unstructuredDataProduct)
		}); err != nil {
			logger.Error(err, "failed to add force reconcile label to UnstructuredDataProduct CR")
			return r.handleError(ctx, vectorEmbeddingsGenerationJobCR, err)
		}
		logger.Info("successfully added force reconcile label to UnstructuredDataProduct CR")
	} else {
		logger.Info("no files were embedded, no need to add force reconcile label to UnstructuredDataProduct CR")
	}

	// update the status to success
	successMessage := fmt.Sprintf("successfully reconciled vector embeddings generation job: %s", vectorEmbeddingsGenerationJobCR.Name)
	vectorEmbeddingsJobKey := client.ObjectKeyFromObject(vectorEmbeddingsGenerationJobCR)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		res := &operatorv1alpha1.VectorEmbeddingsGenerationJob{}
		if err := r.Get(ctx, vectorEmbeddingsJobKey, res); err != nil {
			return err
		}
		res.UpdateStatus(successMessage, nil)
		return r.Status().Update(ctx, res)
	}); err != nil {
		logger.Error(err, "failed to update VectorEmbeddingsGenerationJob CR status", "namespace", vectorEmbeddingsJobKey.Namespace, "name", vectorEmbeddingsJobKey.Name)
		return r.handleError(ctx, vectorEmbeddingsGenerationJobCR, err)
	}
	logger.Info("successfully updated VectorEmbeddingsGenerationJob CR status", "status", vectorEmbeddingsGenerationJobCR.Status)

	return ctrl.Result{}, nil
}

func (r *VectorEmbeddingsGenerationJobReconciler) processChunkedFile(ctx context.Context, chunksFilePath string, vectorEmbeddingsGenerationJobCR *operatorv1alpha1.VectorEmbeddingsGenerationJob, apiKeySecret *corev1.Secret) (bool, error) {
	logger := log.FromContext(ctx)
	logger.Info("processing chunked file", "chunksFilePath", chunksFilePath)

	needsEmbedding, err := r.needsEmbedding(ctx, chunksFilePath, vectorEmbeddingsGenerationJobCR)
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
		ConvertedFileMetadata:    chunkedFile.ConvertedDocument.Metadata,
		ChunkFileMetadata:        chunkedFile.ChunksDocument.Metadata,
		EmbeddingProvider:        vectorEmbeddingsGenerationJobCR.Spec.EmbeddingGeneratorConfig.Provider,
		EmbeddingGeneratorConfig: vectorEmbeddingsGenerationJobCR.Spec.EmbeddingGeneratorConfig,
	}

	texts := make([]string, len(chunkedFile.ChunksDocument.Chunks.Text))
	copy(texts, chunkedFile.ChunksDocument.Chunks.Text)

	var embeddingClient embedding.EmbeddingGenerator
	switch vectorEmbeddingsGenerationJobCR.Spec.EmbeddingGeneratorConfig.Provider {
	case embedding.SelfHostedModel:
		embeddingClient, err = embedding.NewSelfHostedModelClient(&vectorEmbeddingsGenerationJobCR.Spec.EmbeddingGeneratorConfig.SelfHostedModelConfig)
		if err != nil {
			logger.Error(err, "failed to create selfhosted model client")
			return false, err
		}
	case embedding.GeminiModel:
		embeddingClient, err = embedding.NewGeminiModelClient(&vectorEmbeddingsGenerationJobCR.Spec.EmbeddingGeneratorConfig.GeminiModelConfig)
		if err != nil {
			logger.Error(err, "failed to create gemini model client")
			return false, err
		}
	default:
		return false, fmt.Errorf("unsupported embedding provider: %s", vectorEmbeddingsGenerationJobCR.Spec.EmbeddingGeneratorConfig.Provider)
	}

	logger.Info("generating embeddings for chunks", "file", chunksFilePath, "chunkCount", len(texts))

	const batchSize = 10
	const delayBetweenBatches = 10 * time.Second
	allEmbeddings := make([][]float64, 0, len(texts))

	for i := 0; i < len(texts); i += batchSize {
		end := i + batchSize
		if end > len(texts) {
			end = len(texts)
		}
		batch := texts[i:end]

		logger.Info("processing batch", "batchStart", i, "batchEnd", end, "batchSize", len(batch))
		embeddingResult, err := embeddingClient.GenerateEmbeddings(ctx, batch, apiKeySecret)
		if err != nil {
			logger.Error(err, "failed to generate embeddings for batch", "batchStart", i, "batchEnd", end)
			return false, err
		}

		allEmbeddings = append(allEmbeddings, embeddingResult.Embeddings...)
		logger.Info("successfully processed batch", "batchStart", i, "batchEnd", end, "embeddingsGenerated", len(embeddingResult.Embeddings))

		if end < len(texts) {
			logger.Info("waiting for the next batch")
			time.Sleep(delayBetweenBatches)
		}
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

func (r *VectorEmbeddingsGenerationJobReconciler) needsEmbedding(ctx context.Context, chunksFilePath string, vectorEmbeddingsGenerationJobCR *operatorv1alpha1.VectorEmbeddingsGenerationJob) (bool, error) {
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
			ConvertedFileMetadata:    chunkedFile.ConvertedDocument.Metadata,
			ChunkFileMetadata:        chunkedFile.ChunksDocument.Metadata,
			EmbeddingProvider:        vectorEmbeddingsGenerationJobCR.Spec.EmbeddingGeneratorConfig.Provider,
			EmbeddingGeneratorConfig: vectorEmbeddingsGenerationJobCR.Spec.EmbeddingGeneratorConfig,
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

func (r *VectorEmbeddingsGenerationJobReconciler) handleError(ctx context.Context, vectorEmbeddingsGenerationJobCR *operatorv1alpha1.VectorEmbeddingsGenerationJob, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	key := client.ObjectKeyFromObject(vectorEmbeddingsGenerationJobCR)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.VectorEmbeddingsGenerationJob{}
		if getErr := r.Get(ctx, key, latest); getErr != nil {
			return getErr
		}
		latest.UpdateStatus("", reconcileErr)
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update VectorEmbeddingsGenerationJob CR status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, reconcileErr
}

// SetupWithManager sets up the controller with the Manager.
func (r *VectorEmbeddingsGenerationJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	labelPredicate := controllerutils.ForceReconcilePredicate()
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.VectorEmbeddingsGenerationJob{}).
		WithEventFilter(predicate.Or(predicate.GenerationChangedPredicate{}, labelPredicate)).
		Complete(r)
}
