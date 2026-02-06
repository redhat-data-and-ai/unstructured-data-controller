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

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

const (
	VectorEmbeddingsGenerationJobControllerName = "VectorEmbeddingsGenerationJob"
)

var (
	cacheDirectory    = "/Users/shikgupt/Unstructured-Data-Initiative/unstructured-data-controller/tmp/cache/"
	dataStorageBucket = "data-storage-bucket"
)

type VectorEmbeddingsGenerationJobReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	fileStore *filestore.FileStore
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,resources=vectorembeddingsgenerationjobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,resources=vectorembeddingsgenerationjobs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,resources=vectorembeddingsgenerationjobs/finalizers,verbs=update

func (r *VectorEmbeddingsGenerationJobReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("Reconciling VectorEmbeddingsGenerationJob")

	// get the vector embedding generation CR
	vectorEmbeddingsGenerationJobCR := &operatorv1alpha1.VectorEmbeddingsGenerationJob{}
	if err := r.Get(ctx, req.NamespacedName, vectorEmbeddingsGenerationJobCR); err != nil {
		logger.Error(err, "failed to get VectorEmbeddingsGenerationJob CR")
		return ctrl.Result{}, err
	}

	// read the kubernetes secret for the api key
	apiKeySecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      vectorEmbeddingsGenerationJobCR.Spec.ApiKey,
		Namespace: vectorEmbeddingsGenerationJobCR.Namespace,
	}, apiKeySecret); err != nil {
		logger.Error(err, "failed to get API key secret")
		return ctrl.Result{}, err
	}

	// read AWS secret
	awsSecret := &corev1.Secret{}
	if err := r.Get(ctx, types.NamespacedName{
		Name:      vectorEmbeddingsGenerationJobCR.Spec.AwsSecret,
		Namespace: vectorEmbeddingsGenerationJobCR.Namespace,
	}, awsSecret); err != nil {
		logger.Error(err, "failed to get AWS secret")
		return ctrl.Result{}, err
	}

	// extract AWS config from secret and initialize global S3 client
	awsConfig := &awsclienthandler.AWSConfig{
		Region:          string(awsSecret.Data["AWS_REGION"]),
		AccessKeyID:     string(awsSecret.Data["AWS_ACCESS_KEY_ID"]),
		SecretAccessKey: string(awsSecret.Data["AWS_SECRET_ACCESS_KEY"]),
		SessionToken:    string(awsSecret.Data["AWS_SESSION_TOKEN"]),
		Endpoint:        string(awsSecret.Data["AWS_ENDPOINT"]),
	}

	_, err := awsclienthandler.NewS3ClientFromConfig(ctx, awsConfig)
	if err != nil {
		logger.Error(err, "failed to initialize S3 client")
		return ctrl.Result{}, err
	}
	logger.Info("S3 client initialized successfully")

	if err := controllerutils.RemoveForceReconcileLabel(ctx, r.Client, vectorEmbeddingsGenerationJobCR); err != nil {
		logger.Error(err, "error removing the force-reconcile label from the VectorEmbeddingsGenerationJob CR")
		return ctrl.Result{}, err
	}

	vectorEmbeddingsGenerationJobCR.SetWaiting()
	if err := r.Update(ctx, vectorEmbeddingsGenerationJobCR); err != nil {
		logger.Error(err, "failed to update the VectorEmbeddingsGenerationJob CR")
		return ctrl.Result{}, err
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

	chunksFilePaths := unstructured.FilterChunksFilePaths(filePaths)
	logger.Info("chunks filepaths filtered successfully", "chunksFilePaths", chunksFilePaths)

	embeddingErrors := []error{}
	for _, chunksFilePath := range chunksFilePaths {
		logger.Info("processing chunked file for embedding", "file", chunksFilePath)
		if err := r.processChunkedFile(ctx, chunksFilePath, vectorEmbeddingsGenerationJobCR, apiKeySecret); err != nil {
			embeddingErrors = append(embeddingErrors, err)
			logger.Error(err, "failed to process chunked file", "file", chunksFilePath)
		}
	}

	if len(embeddingErrors) > 0 {
		logger.Error(embeddingErrors[0], "failed to process some chunked files")
		return ctrl.Result{}, fmt.Errorf("failed to process some chunked files")
	}

	return ctrl.Result{}, nil
}

func (r *VectorEmbeddingsGenerationJobReconciler) processChunkedFile(ctx context.Context, chunksFilePath string, vectorEmbeddingsGenerationJobCR *operatorv1alpha1.VectorEmbeddingsGenerationJob, apiKeySecret *corev1.Secret) error {
	logger := log.FromContext(ctx)
	logger.Info("processing chunked file", "chunksFilePath", chunksFilePath)

	needsEmbedding, err := r.needsEmbedding(ctx, chunksFilePath, vectorEmbeddingsGenerationJobCR)
	if err != nil {
		logger.Error(err, "failed to check if file needs embedding")
		return err
	}
	if !needsEmbedding {
		logger.Info("file does not need embedding, skipping ...", "file", chunksFilePath)
		return nil
	}

	logger.Info("retrieving chunked file from filestore", "file", chunksFilePath)
	chunkedFileRaw, err := r.fileStore.Retrieve(ctx, chunksFilePath)
	if err != nil {
		logger.Error(err, "failed to retrieve chunked file")
		return err
	}

	chunkedFile := &unstructured.ChunksFile{}
	if err := json.Unmarshal(chunkedFileRaw, &chunkedFile); err != nil {
		logger.Error(err, "failed to unmarshal chunked file")
		return err
	}

	// Validate chunked file structure
	if chunkedFile.ConvertedDocument == nil || chunkedFile.ChunksDocument == nil {
		return fmt.Errorf("invalid chunks file structure: missing required fields")
	}
	if chunkedFile.ChunksDocument.Chunks == nil || len(chunkedFile.ChunksDocument.Chunks.Text) == 0 {
		logger.Info("chunks file has no text chunks, skipping", "file", chunksFilePath)
		return nil
	}

	embeddingFileMetadata := &unstructured.EmbeddingFileMetadata{
		ConvertedFileMetadata:    &chunkedFile.ConvertedDocument.Metadata,
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
			return err
		}
	case embedding.GeminiModel:
		embeddingClient, err = embedding.NewGeminiModelClient(&vectorEmbeddingsGenerationJobCR.Spec.EmbeddingGeneratorConfig.GeminiModelConfig)
		if err != nil {
			logger.Error(err, "failed to create gemini model client")
			return err
		}
	default:
		return fmt.Errorf("unsupported embedding provider: %s", vectorEmbeddingsGenerationJobCR.Spec.EmbeddingGeneratorConfig.Provider)
	}

	logger.Info("generating embeddings for chunks", "file", chunksFilePath, "chunkCount", len(texts))
	embeddingResult, err := embeddingClient.GenerateEmbeddings(ctx, texts, apiKeySecret)
	if err != nil {
		logger.Error(err, "failed to generate embeddings")
		return err
	}

	logger.Info("successfully generated embeddings", "file", chunksFilePath, "embeddingCount", embeddingResult.Count)

	// rearrange the embeddings
	embeddings := make([]*unstructured.Embeddings, len(embeddingResult.Embeddings))
	for i, embeddingVector := range embeddingResult.Embeddings {
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
		return err
	}

	embeddingsFilePath := unstructured.GetVectorEmbeddingsFilePath(chunkedFile.ConvertedDocument.Metadata.RawFilePath)
	logger.Info("storing embedded file", "embeddingsFilePath", embeddingsFilePath)
	if err := r.fileStore.Store(ctx, embeddingsFilePath, embeddingsFileBytes); err != nil {
		logger.Error(err, "failed to store embedded file")
		return err
	}

	logger.Info("successfully processed and stored embedded file", "file", chunksFilePath, "embeddingsFile", embeddingsFilePath)
	return nil
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
			ConvertedFileMetadata:    &chunkedFile.ConvertedDocument.Metadata,
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
	vectorEmbeddingsGenerationJobCR.UpdateStatus("Error during reconciliation", err)
	if err := r.Update(ctx, vectorEmbeddingsGenerationJobCR); err != nil {
		logger.Error(err, "failed to update the VectorEmbeddingsGenerationJob CR status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, err
}

// SetupWithManager sets up the controller with the Manager.
func (r *VectorEmbeddingsGenerationJobReconciler) SetupWithManager(mgr ctrl.Manager) error {
	labelPredicate := controllerutils.ForceReconcilePredicate()
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.VectorEmbeddingsGenerationJob{}).
		WithEventFilter(predicate.Or(predicate.GenerationChangedPredicate{}, labelPredicate)).
		Complete(r)
}
