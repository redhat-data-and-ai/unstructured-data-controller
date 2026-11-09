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
	fs, err := filestore.New(ctx, dataStorageDirectory, dataStorageBucket)
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
	inputPath := unstructured.StagePath(pipelineName, vectorEmbeddingsGeneratorCR.Spec.DependsOn[0].Name)
	outputPath := unstructured.StagePath(pipelineName, vectorEmbeddingsGeneratorCR.Spec.StageName)
	filePaths, err := r.fileStore.ListFilesInPath(ctx, inputPath)
	logger.Info("files in path", "count", len(filePaths))
	if err != nil {
		logger.Error(err, "failed to list files in path")
		return r.handleError(ctx, vectorEmbeddingsGeneratorCR, err)
	}

	embeddingErrors := []error{}
	var filesProcessed int64
	for _, chunksFilePath := range filePaths {
		logger.Info("processing chunked file for embedding", "file", chunksFilePath)
		processed, err := r.processChunkedFile(ctx, chunksFilePath, vectorEmbeddingsGeneratorCR, inputPath, outputPath)
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
		vectorEmbeddingsGeneratorCR.UpdateStatus(successMessage, nil)
	}); err != nil {
		logger.Error(err, "failed to update VectorEmbeddingsGenerator CR status", "namespace", vectorEmbeddingsGeneratorCR.Namespace, "name", vectorEmbeddingsGeneratorCR.Name)
		return r.handleError(ctx, vectorEmbeddingsGeneratorCR, err)
	}
	logger.Info("successfully updated VectorEmbeddingsGenerator CR status", "status", vectorEmbeddingsGeneratorCR.Status)

	return ctrl.Result{}, nil
}

func (r *VectorEmbeddingsGeneratorReconciler) processChunkedFile(ctx context.Context, chunksFilePath string, vectorEmbeddingsGeneratorCR *operatorv1alpha1.VectorEmbeddingsGenerator, inputPath, outputPath string) (bool, error) {
	logger := log.FromContext(ctx)
	logger.Info("processing chunked file", "chunksFilePath", chunksFilePath)

	needsEmbedding, err := r.needsEmbedding(ctx, chunksFilePath, vectorEmbeddingsGeneratorCR, inputPath, outputPath)
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

	// Validate chunked file structure. Metadata is checked too, because
	// everything below reads the converted metadata through it.
	if chunkedFile.ChunksDocument == nil ||
		chunkedFile.ChunksDocument.Metadata == nil ||
		chunkedFile.ChunksDocument.Metadata.ConvertedFileMetadata == nil {
		return false, errors.New("invalid chunks file structure: missing required fields")
	}
	if chunkedFile.ChunksDocument.Chunks == nil || len(chunkedFile.ChunksDocument.Chunks.Text) == 0 {
		logger.Info("chunks file has no text chunks, skipping", "file", chunksFilePath)
		return false, nil
	}

	texts := make([]string, len(chunkedFile.ChunksDocument.Chunks.Text))
	copy(texts, chunkedFile.ChunksDocument.Chunks.Text)

	vegConfig := vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig
	modelName := vegConfig.ModelName

	creds, supported := embeddingModelCredentials[Model(modelName)]
	if !supported {
		return false, fmt.Errorf("unsupported embedding model: %s", modelName)
	}

	embeddingClient := embedding.NewHTTPClient(&embedding.HTTPClientConfig{
		Endpoint:   creds.Endpoint,
		AuthFormat: "Bearer",
		APIKey:     creds.APIKey,
		ModelName:  modelName,
	})

	embeddingFileMetadata := &unstructured.EmbeddingFileMetadata{
		ConvertedFileMetadata:   chunkedFile.ChunksDocument.Metadata.ConvertedFileMetadata,
		ChunkFileMetadata:       chunkedFile.ChunksDocument.Metadata,
		ModelName:               modelName,
		NomicEmbedTextV15Config: vegConfig.NomicEmbedTextV15Config,
		GeminiEmbedding2Config:  vegConfig.GeminiEmbedding2Config,
	}

	var encodingFormat string
	switch modelName {
	case "gemini-embedding-2":
		encodingFormat = vegConfig.GeminiEmbedding2Config.EncodingFormat
	case "nomic-ai/nomic-embed-text-v1.5":
		encodingFormat = vegConfig.NomicEmbedTextV15Config.EncodingFormat
	default:
		return false, fmt.Errorf("unsupported model for encoding format: %s", modelName)
	}

	logger.Info("generating embeddings for chunks", "file", chunksFilePath, "chunkCount", len(texts))

	batchSize := vegConfig.BatchSize
	// Indexed by chunk, not appended to: a skipped chunk has to leave a hole,
	// or every later vector inherits the wrong chunk's provenance.
	vectors := make([][]float64, len(texts))
	var skippedChunks []int

	for batchStart := 0; batchStart < len(texts); batchStart += batchSize {
		batchEnd := min(batchStart+batchSize, len(texts))

		logger.Info("processing batch", "batchStart", batchStart, "batchEnd", batchEnd)
		skipped, err := embedRange(ctx, embeddingClient, texts, batchStart, batchEnd, encodingFormat, vectors)
		if err != nil {
			return false, fmt.Errorf("embedding chunks [%d,%d) of %s: %w",
				batchStart, batchEnd, chunksFilePath, err)
		}
		skippedChunks = append(skippedChunks, skipped...)
	}

	// Every chunk refused is a fault in the request rather than in any chunk.
	// A 400 also answers a bad encoding format, an unknown model, or a
	// gateway that meets a missing credential that way, and none of those say
	// anything about an input. Skipping them all would store an empty file,
	// report success, and never try again, because needsEmbedding compares
	// only the metadata.
	if len(texts) > 0 && len(skippedChunks) == len(texts) {
		return false, fmt.Errorf("embedding API refused all %d chunks of %s, so the request is at fault rather than the chunks",
			len(texts), chunksFilePath)
	}

	if len(skippedChunks) > 0 {
		logger.Info("chunks the model refused, absent from the embeddings file",
			"file", chunksFilePath, "skipped", len(skippedChunks), "chunks", skippedChunks)
	}

	logger.Info("successfully generated embeddings", "file", chunksFilePath,
		"embeddingCount", len(texts)-len(skippedChunks))

	// Each vector carries its chunk's provenance. A field left off here
	// cannot be recovered later: the chunks document is nested in the same
	// file, but each entry is read on its own and cannot reach it.
	chunkedFilePath := chunkedFile.ChunksDocument.Metadata.ConvertedFileMetadata.RawFilePath
	embeddings := make([]*unstructured.Embeddings, 0, len(vectors)-len(skippedChunks))
	for i, embeddingVector := range vectors {
		if embeddingVector == nil {
			continue
		}
		chunk := chunkedFile.ChunksDocument.Chunks.At(i)
		embeddings = append(embeddings, &unstructured.Embeddings{
			Text:      texts[i],
			Embedding: embeddingVector,
			ChunkID:   unstructured.ChunkID{FilePath: chunkedFilePath, Index: i}.String(),
			RawText:   chunk.RawText,
			BlockIDs:  chunk.BlockIDs,
			Regions:   chunk.Regions,
		})
	}

	// Create the complete embeddings file structure
	embeddingsFile := &unstructured.EmbeddingsFile{
		ChunksDocument: chunkedFile.ChunksDocument,
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

	embeddingsFilePath := unstructured.RemapToOutputDir(chunksFilePath, inputPath, outputPath)
	logger.Info("storing embedded file", "embeddingsFilePath", embeddingsFilePath)
	if err := r.fileStore.Store(ctx, embeddingsFilePath, embeddingsFileBytes); err != nil {
		logger.Error(err, "failed to store embedded file")
		return false, err
	}

	logger.Info("successfully processed and stored embedded file", "file", chunksFilePath, "embeddingsFile", embeddingsFilePath)
	return true, nil
}

// embedRange fills vectors[start:end] from a single request.
//
// A terminal rejection is bisected rather than returned: the API rejects the
// whole request for one unembeddable input, and the rest of the range is still
// good. Only a rejection of the input itself is bisected; anything else, a bad
// credential above all, would otherwise skip every chunk in the file and store
// that as a success. Returns the chunk indices it had to give up on.
func embedRange(
	ctx context.Context, embeddingClient embedding.EmbeddingGenerator,
	texts []string, start, end int, encodingFormat string, vectors [][]float64,
) ([]int, error) {
	logger := log.FromContext(ctx)

	result, err := embeddingClient.GenerateEmbeddings(ctx, texts[start:end], encodingFormat)
	if err == nil {
		if len(result.Embeddings) != end-start {
			return nil, fmt.Errorf("embedding count mismatch for chunks [%d,%d): got %d",
				start, end, len(result.Embeddings))
		}
		// An empty vector in a 200 would be dropped by the caller's output
		// loop without being counted as skipped, so the file would lose a
		// chunk while the log still reported it embedded.
		for offset, vector := range result.Embeddings {
			if len(vector) == 0 {
				return nil, fmt.Errorf("embedding API returned no vector for chunk %d", start+offset)
			}
		}
		copy(vectors[start:end], result.Embeddings)
		return nil, nil
	}

	var apiErr *embedding.APIError
	if !errors.As(err, &apiErr) || !apiErr.InputRejected() {
		return nil, err
	}

	if end-start == 1 {
		logger.Info("embedding API rejected this chunk, skipping it",
			"chunk", start, "reason", err.Error())
		return []int{start}, nil
	}

	mid := start + (end-start)/2
	skippedLeft, err := embedRange(ctx, embeddingClient, texts, start, mid, encodingFormat, vectors)
	if err != nil {
		return nil, err
	}
	skippedRight, err := embedRange(ctx, embeddingClient, texts, mid, end, encodingFormat, vectors)
	if err != nil {
		return nil, err
	}
	return append(skippedLeft, skippedRight...), nil
}

func (r *VectorEmbeddingsGeneratorReconciler) needsEmbedding(ctx context.Context, chunksFilePath string, vectorEmbeddingsGeneratorCR *operatorv1alpha1.VectorEmbeddingsGenerator, inputPath, outputPath string) (bool, error) {
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

	embeddingsFilePath := unstructured.RemapToOutputDir(chunksFilePath, inputPath, outputPath)
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
			ConvertedFileMetadata:   chunkedFile.ChunksDocument.Metadata.ConvertedFileMetadata,
			ChunkFileMetadata:       chunkedFile.ChunksDocument.Metadata,
			ModelName:               vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.ModelName,
			NomicEmbedTextV15Config: vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.NomicEmbedTextV15Config,
			GeminiEmbedding2Config:  vectorEmbeddingsGeneratorCR.Spec.VectorEmbeddingsGeneratorConfig.GeminiEmbedding2Config,
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
