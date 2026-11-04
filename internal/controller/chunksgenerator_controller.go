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
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
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
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/langchain"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
	"github.com/tmc/langchaingo/textsplitter"
)

const (
	ChunksGeneratorControllerName = "ChunksGenerator"

	// maxDoclingChunkingAttempts bounds how often one file is sent to
	// docling's chunker before it is given up on.
	//
	// A bound is needed because the usual causes do not clear by themselves.
	// An unreachable tokenizer returns success with no chunks, and a document
	// the chunker cannot read fails the same way every time, so an unbounded
	// retry takes a slot on every pass and chunks nothing.
	maxDoclingChunkingAttempts = 3
)

// ChunksGeneratorReconciler reconciles a ChunksGenerator object
type ChunksGeneratorReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	fileStore *filestore.FileStore
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=chunksgenerators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=chunksgenerators/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=chunksgenerators/finalizers,verbs=update

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
	logger.Info("reconciling", "controller", ChunksGeneratorControllerName)

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
	chunksGeneratorCR = chunksGeneratorCR.DeepCopy()
	chunksGeneratorCR.Spec.ChunksGeneratorConfig.SetDefaults()

	// set status to waiting
	if err := controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
		chunksGeneratorCR.SetWaiting()
	}); err != nil {
		logger.Error(err, "failed to update ChunksGenerator CR status")
		return ctrl.Result{}, err
	}

	fs, err := filestore.New(ctx, dataStorageDirectory, dataStorageBucket)
	if err != nil {
		if IsAWSClientNotInitializedError(err) {
			logger.Info("ControllerConfig has not initialized AWS clients yet, will try again in a bit ...")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		logger.Error(err, "failed to create filestore")
		return r.handleError(ctx, chunksGeneratorCR, err)
	}
	r.fileStore = fs

	pipelineName, err := controllerutils.ParentPipelineNameFromOwnerReference(chunksGeneratorCR)
	if err != nil {
		return r.handleError(ctx, chunksGeneratorCR, err)
	}
	inputPath := unstructured.StagePath(pipelineName, chunksGeneratorCR.Spec.DependsOn[0].Name)
	outputPath := unstructured.StagePath(pipelineName, chunksGeneratorCR.Spec.StageName)
	filePaths, err := r.fileStore.ListFilesInPath(ctx, inputPath)
	if err != nil {
		logger.Error(err, "failed to list files in path")
		return r.handleError(ctx, chunksGeneratorCR, err)
	}

	chunkingErrors := []error{}
	skippedFiles := []string{}
	var filesProcessed int64

	// Collect finished chunking tasks before submitting any more, so a task
	// that has completed gives its slot back and frees capacity for this
	// pass. The slice is copied because reconcileChunkJob patches status and
	// so rewrites the one on the resource underneath the loop. Only the
	// remote chunker records jobs; the in process splitters leave this empty.
	// A completed task counts itself, inside the same patch that forgets the
	// job, rather than through the counter below. The counter is only added
	// up once every file has been dealt with, and this loop can leave tasks
	// still running, which returns early.
	for _, job := range slices.Clone(chunksGeneratorCR.Status.Jobs) {
		// A job with no task id is a failed attempt waiting to be sent
		// again, not a task to ask about. The file loop below resubmits it
		// and carries its attempt count.
		if job.TaskID == "" {
			continue
		}
		if err := r.reconcileChunkJob(ctx, job, chunksGeneratorCR, inputPath, outputPath); err != nil {
			chunkingErrors = append(chunkingErrors, err)
			logger.Error(err, "failed to reconcile chunking task", "file", job.FilePath, "taskID", job.TaskID)
		}
	}

	for _, convertedFilePath := range filePaths {
		logger.Info("processing converted file", "file", convertedFilePath)
		processed, err := r.processConvertedFile(ctx, convertedFilePath, chunksGeneratorCR, inputPath, outputPath)
		if err != nil {
			// Both chunking paths refuse work when their client is already at
			// capacity, rather than queueing behind it, so the file waits for
			// the next pass instead of failing the stage. The two clients
			// report it in their own words.
			if strings.Contains(err.Error(), langchain.SemaphoreAcquireError) ||
				strings.Contains(err.Error(), docling.SemaphoreAcquireError) {
				logger.Error(err, "failed to process converted file, semaphore acquire error, will try again later", "file", convertedFilePath)
				skippedFiles = append(skippedFiles, convertedFilePath)
				continue
			}
			if errors.Is(err, unstructured.ErrNotAConvertedFile) {
				// Not every object under the convert stage prefix is a
				// converted file. Retrying cannot help, so skip it rather
				// than failing the whole stage. Logged at error level, as
				// the semaphore case above is, because something put a file
				// here that does not belong and an operator should see it.
				logger.Error(err, "skipping file that is not a converted file", "file", convertedFilePath)
				continue
			}
			chunkingErrors = append(chunkingErrors, err)
			logger.Error(err, "failed to process converted file", "file", convertedFilePath)
			continue
		}
		if processed {
			filesProcessed++
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

	// Chunking tasks outlive the reconcile that submitted them, and nothing
	// else wakes this stage when one finishes. Without the requeue the files
	// behind them are never written.
	if len(chunksGeneratorCR.Status.Jobs) > 0 {
		logger.Info("some chunking tasks are still running, will requeue after a bit ...",
			"tasks", len(chunksGeneratorCR.Status.Jobs))
		return ctrl.Result{
			RequeueAfter: requeueAfter,
		}, nil
	}

	successMessage := fmt.Sprintf("successfully reconciled chunks generator: %s", chunksGeneratorCR.Name)
	if err := controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
		chunksGeneratorCR.Status.FilesProcessed += filesProcessed
		chunksGeneratorCR.UpdateStatus(successMessage, nil)
	}); err != nil {
		logger.Error(err, "failed to update ChunksGenerator CR status", "namespace", chunksGeneratorCR.Namespace, "name", chunksGeneratorCR.Name)
		return r.handleError(ctx, chunksGeneratorCR, err)
	}
	logger.Info("successfully updated ChunksGenerator CR status", "status", chunksGeneratorCR.Status)

	return ctrl.Result{}, nil
}

func (r *ChunksGeneratorReconciler) processConvertedFile(ctx context.Context, convertedFilePath string, chunksGeneratorCR *operatorv1alpha1.ChunksGenerator, inputPath, outputPath string) (bool, error) {
	logger := log.FromContext(ctx)
	logger.Info("processing converted file", "file", convertedFilePath)

	chunksFilePath := unstructured.RemapToOutputDir(convertedFilePath, inputPath, outputPath)

	// Given up on after repeated failures. Its chunks file does not exist, so
	// every check below says it needs chunking, and without this it is
	// resubmitted on every reconcile for as long as the resource lives.
	if chunksGeneratorCR.IsFilePermanentlyFailing(convertedFilePath) {
		logger.Info("skipping file that has permanently failed chunking", "file", convertedFilePath)
		return false, nil
	}

	// A file whose chunking task is already running needs nothing here. Its
	// artifact does not exist until the task finishes, so the checks below
	// would submit it again on every requeue, each submit taking another slot
	// against docling and orphaning the task id it overwrites.
	//
	// A job with no task id is a failed attempt waiting to be retried, and
	// falls through so the submit below picks it up and carries its attempt
	// count. The in process splitters finish within one reconcile and never
	// record a job at all.
	if job := chunksGeneratorCR.GetJobByFilePath(convertedFilePath); job != nil && job.TaskID != "" {
		logger.Info("chunking task already running for file, nothing to submit", "file", convertedFilePath, "taskID", job.TaskID)
		return false, nil
	}

	// figure out if the file is already chunked
	needsChunking, err := r.needsChunking(ctx, convertedFilePath, chunksGeneratorCR, inputPath, outputPath)
	if err != nil {
		logger.Error(err, "failed to check if file needs chunking")
		return false, err
	}
	if !needsChunking {
		logger.Info("file is already chunked, skipping ...")
		return false, nil
	}

	// Docling's chunker runs remotely and is submitted rather than called, so
	// this reconcile records a task and a later one collects its result.
	// There is no chunks file to write yet, and the file does not count as
	// processed until there is.
	if chunksGeneratorCR.Spec.ChunksGeneratorConfig.Strategy == operatorv1alpha1.ChunkingStrategyDoclingHybrid {
		return false, r.submitChunkJob(ctx, convertedFilePath, chunksGeneratorCR)
	}

	// chunk the file
	chunksFile, err := r.chunkFile(ctx, convertedFilePath, chunksGeneratorCR)
	if err != nil {
		logger.Error(err, "failed to chunk file")
		return false, err
	}

	// store the chunks in the filestore
	chunksFileBytes, err := json.Marshal(chunksFile)
	if err != nil {
		logger.Error(err, "failed to marshal chunks file")
		return false, err
	}
	if err := r.fileStore.Store(ctx, chunksFilePath, chunksFileBytes); err != nil {
		logger.Error(err, "failed to store chunks file")
		return false, err
	}

	return true, nil
}

// chunkingOptionsFrom maps the generator's hybrid chunker settings onto the
// ones the docling client sends.
//
// Chunker is left alone: the client sets it, because it has to agree with the
// endpoint the client chose by path.
func chunkingOptionsFrom(config operatorv1alpha1.HybridChunkerConfig) docling.ChunkingOptions {
	return docling.ChunkingOptions{
		Tokenizer: config.Tokenizer,
		MaxTokens: config.MaxTokens,
		// Both of these default to true and SetDefaults fills them in for
		// this strategy. A nil is read as true rather than as false so that a
		// config that somehow reached here undefaulted does not quietly
		// produce chunks with no text to display, and tables serialised as
		// "RowLabel, ColumnName = value" triplets.
		IncludeRawText:    config.IncludeRawText == nil || *config.IncludeRawText,
		UseMarkdownTables: config.UseMarkdownTables == nil || *config.UseMarkdownTables,
	}
}

// submitChunkJob hands a stored document to docling's chunker and records the
// task docling accepted.
//
// It writes no chunks file. The chunker is asynchronous, so the result is
// collected by a later reconcile, and the job recorded on status is what
// survives a controller restart in between.
func (r *ChunksGeneratorReconciler) submitChunkJob(ctx context.Context, convertedFilePath string, chunksGeneratorCR *operatorv1alpha1.ChunksGenerator) error {
	logger := log.FromContext(ctx)
	logger.Info("submitting file for chunking", "file", convertedFilePath)

	convertedFileRaw, err := r.fileStore.Retrieve(ctx, convertedFilePath)
	if err != nil {
		return err
	}

	convertedFile := unstructured.ConvertedFile{}
	if err := json.Unmarshal(convertedFileRaw, &convertedFile); err != nil {
		return err
	}
	if err := convertedFile.Validate(); err != nil {
		return err
	}

	// The stored document is sent, never the original file. Converting the
	// same file twice does not produce the same document, and a chunk names
	// the elements it was built from by their address within one document, so
	// addresses from one conversion resolve against another to the wrong
	// elements. It is also the difference between seconds and milliseconds,
	// since nothing is parsed again.
	doclingDocument := convertedFile.ConvertedDocument.Content.DoclingDocument
	if len(doclingDocument) == 0 {
		// Expected for anything converted before structured output was asked
		// for. Retrying cannot help while the converted file stays as it is,
		// so this names what is missing rather than reporting a chunking
		// failure.
		return fmt.Errorf("cannot chunk %s with docling: it holds no structured document, only markdown", convertedFilePath)
	}

	config := chunksGeneratorCR.Spec.ChunksGeneratorConfig
	response, err := doclingClient.ChunkDoclingDocument(ctx, convertedFilePath, doclingDocument, chunkingOptionsFrom(config.HybridChunkerConfig))
	if err != nil {
		return err
	}
	logger.Info("docling accepted the chunking task", "taskID", response.TaskID, "file", convertedFilePath)

	// A job left behind by a failed attempt carries the count forward. The
	// count is the only record of how many times this file has been tried,
	// so restarting it at one here would retry forever.
	attempt := 1
	if previous := chunksGeneratorCR.GetJobByFilePath(convertedFilePath); previous != nil {
		attempt = previous.Attempts + 1
	}

	// The config travels with the job rather than being read again when the
	// result arrives. The spec can change while the task runs, and the
	// metadata written then has to describe the chunking that actually ran,
	// or the next reconcile compares against a config this artifact was not
	// produced under.
	return controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
		chunksGeneratorCR.AddOrUpdateJob(operatorv1alpha1.ChunkJob{
			FilePath:              convertedFilePath,
			FileIdentifier:        convertedFile.ConvertedDocument.Metadata.FileIdentifier,
			ChunksGeneratorConfig: config,
			TaskID:                response.TaskID,
			Status:                response.TaskStatus,
			CreatedOn:             time.Now().Format(time.RFC3339),
			Attempts:              attempt,
		})
	})
}

// reconcileChunkJob asks docling where a submitted chunking task has got to,
// and acts on the answer.
//
// A task still running is left alone. One that has finished has its chunks
// written and its job forgotten. One that ended badly is retried until the
// attempt limit, after which the file is recorded as permanently failing.
//
// A job is never left on status carrying the id of a task that has ended.
// Polling an ended task releases its semaphore slot a second time, and a
// weighted semaphore keeps no record of who holds what, so the second release
// takes a slot from a task that is still running rather than failing.
func (r *ChunksGeneratorReconciler) reconcileChunkJob(ctx context.Context, job operatorv1alpha1.ChunkJob,
	chunksGeneratorCR *operatorv1alpha1.ChunksGenerator, inputPath, outputPath string) error {
	logger := log.FromContext(ctx)

	taskStatus, chunkResponse, err := doclingClient.GetChunks(ctx, job.TaskID)
	switch {
	case err != nil && !errors.Is(err, docling.ErrChunkTaskEnded):
		// The task is alive and still holds its slot, so the next pass polls
		// the same id rather than starting again.
		return fmt.Errorf("polling the chunking task for %s: %w", job.FilePath, err)

	case errors.Is(err, docling.ErrNoChunksReturned):
		// Deliberately not failed here. This is both how the chunker reports
		// being unable to load its tokenizer and what a document with no
		// content legitimately produces, and only the document can tell them
		// apart, so the write below decides.
		logger.Info("chunking task returned no chunks", "file", job.FilePath, "taskID", job.TaskID)

	case err != nil:
		return r.failChunkJob(ctx, job, chunksGeneratorCR, err)

	case taskStatus == docling.TaskStatusPending || taskStatus == docling.TaskStatusStarted:
		logger.Info("chunking task is still running", "file", job.FilePath, "taskID", job.TaskID, "status", taskStatus)
		return nil

	default:
		// The task finished and returned chunks. Written below.
	}

	chunks := []docling.Chunk{}
	if chunkResponse != nil {
		chunks = chunkResponse.Chunks
	}

	// Any failure from here on goes through failChunkJob rather than being
	// returned, even one as transient as a failed upload. The task has ended
	// and given its slot back, so leaving the job with its id would have the
	// next pass poll it again. Retrying means chunking the file afresh.
	if err := r.writeChunksFile(ctx, job, chunks, inputPath, outputPath); err != nil {
		return r.failChunkJob(ctx, job, chunksGeneratorCR, err)
	}
	logger.Info("stored chunks for file", "file", job.FilePath, "taskID", job.TaskID, "chunks", len(chunks))

	// Counted here, in the same patch that forgets the job, because the
	// reconcile that submitted this task finished long ago.
	return controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
		chunksGeneratorCR.DeleteJobByFilePath(job.FilePath)
		chunksGeneratorCR.Status.FilesProcessed++
	})
}

// failChunkJob gives a file another attempt, or gives up on it.
//
// Under the limit it keeps the job and clears the task id, which is what the
// submit path reads as "retry this and carry the count". The count lives
// nowhere else, so dropping the job here would restart it and retry forever.
func (r *ChunksGeneratorReconciler) failChunkJob(ctx context.Context, job operatorv1alpha1.ChunkJob,
	chunksGeneratorCR *operatorv1alpha1.ChunksGenerator, cause error) error {
	logger := log.FromContext(ctx)

	if job.Attempts >= maxDoclingChunkingAttempts {
		// Not returned as an error. The file is recorded on status, which is
		// where an operator looks, and failing the whole stage for one file
		// that will never succeed stops every other file from being written.
		logger.Error(cause, "giving up on chunking file, attempt limit reached",
			"file", job.FilePath, "attempts", job.Attempts)
		return controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
			chunksGeneratorCR.AddPermanentlyFailingFile(job.FilePath)
			chunksGeneratorCR.DeleteJobByFilePath(job.FilePath)
		})
	}

	retry := job
	retry.TaskID = ""
	retry.Status = ""
	if err := controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
		chunksGeneratorCR.AddOrUpdateJob(retry)
	}); err != nil {
		return err
	}

	return fmt.Errorf("chunking %s failed on attempt %d of %d: %w",
		job.FilePath, job.Attempts, maxDoclingChunkingAttempts, cause)
}

// writeChunksFile stores what docling's chunker produced for one job.
//
// The metadata comes from the job rather than from the spec. A task can
// outlive a change to the resource, and the artifact has to describe the
// chunking that actually ran or the next reconcile compares it against a
// configuration it was not produced under.
func (r *ChunksGeneratorReconciler) writeChunksFile(ctx context.Context, job operatorv1alpha1.ChunkJob,
	chunks []docling.Chunk, inputPath, outputPath string) error {
	convertedFileRaw, err := r.fileStore.Retrieve(ctx, job.FilePath)
	if err != nil {
		return err
	}

	convertedFile := unstructured.ConvertedFile{}
	if err := json.Unmarshal(convertedFileRaw, &convertedFile); err != nil {
		return err
	}
	if err := convertedFile.Validate(); err != nil {
		return err
	}

	// Docling numbers its chunks and this artifact is read by position, so
	// the two have to agree. Sorting costs nothing and the alternative is a
	// file whose text and provenance belong to different chunks, which
	// nothing downstream can detect.
	ordered := slices.Clone(chunks)
	slices.SortFunc(ordered, func(a, b docling.Chunk) int {
		return cmp.Compare(a.ChunkIndex, b.ChunkIndex)
	})

	// The contextualised text, with the heading trail, rather than the body
	// alone. It is what gets embedded, so a passage stays findable under the
	// section it came from.
	texts := make([]string, 0, len(ordered))
	for _, chunk := range ordered {
		texts = append(texts, chunk.Text)
	}

	// The same guard the in process path uses, and the reason no chunks is
	// not failed before reaching here. A document that genuinely held nothing
	// is allowed to produce nothing; one that held content and produced
	// nothing is the silent tokenizer failure.
	if err := unstructured.ValidateChunks(convertedFile.ConvertedDocument.Content.Markdown, texts); err != nil {
		return fmt.Errorf("chunking %s: %w", job.FilePath, err)
	}

	chunksFile := unstructured.ChunksFile{
		ConvertedDocument: convertedFile.ConvertedDocument,
		ChunksDocument: &unstructured.ChunksDocument{
			Metadata: &unstructured.ChunksFileMetadata{
				ChunkingTool:          unstructured.ChunkingToolForStrategy(job.ChunksGeneratorConfig.Strategy),
				ChunksGeneratorConfig: job.ChunksGeneratorConfig,
				ConvertedFileMetadata: convertedFile.ConvertedDocument.Metadata,
			},
			Chunks: &unstructured.Chunks{Text: texts},
		},
	}

	chunksFileBytes, err := json.Marshal(chunksFile)
	if err != nil {
		return err
	}

	return r.fileStore.Store(ctx, unstructured.RemapToOutputDir(job.FilePath, inputPath, outputPath), chunksFileBytes)
}

func (r *ChunksGeneratorReconciler) needsChunking(ctx context.Context, convertedFilePath string, chunksGeneratorCR *operatorv1alpha1.ChunksGenerator, inputPath, outputPath string) (bool, error) {
	logger := log.FromContext(ctx)
	logger.Info("checking if file needs chunking", "file", convertedFilePath)

	chunksFilePath := unstructured.RemapToOutputDir(convertedFilePath, inputPath, outputPath)

	// fetch the converted file from the filestore
	// this will also make sure that the converted file exists in the filestore
	convertedFileRaw, err := r.fileStore.Retrieve(ctx, convertedFilePath)
	if err != nil {
		return false, err
	}

	convertedFile := unstructured.ConvertedFile{}
	err = json.Unmarshal(convertedFileRaw, &convertedFile)
	if err != nil {
		return false, err
	}
	if err := convertedFile.Validate(); err != nil {
		return false, err
	}

	convertedFileMetadata := convertedFile.ConvertedDocument.Metadata

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
		ConvertedFileMetadata: convertedFileMetadata,
		// Derived, not assumed. Equal compares the tool, so hardcoding one
		// here while the writer records another re-chunks every file on every
		// reconcile.
		ChunkingTool:          unstructured.ChunkingToolForStrategy(chunksGeneratorCR.Spec.ChunksGeneratorConfig.Strategy),
		ChunksGeneratorConfig: chunksGeneratorCR.Spec.ChunksGeneratorConfig,
	}
	if !chunksFile.ChunksDocument.Metadata.Equal(&newChunksFileMetadata) {
		logger.Info("chunks file config has changed, re-chunking needed", "file", convertedFilePath)
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
	if err := convertedFile.Validate(); err != nil {
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

	content := convertedFile.ConvertedDocument.Content.Markdown
	chunks, err := chunker.Chunk(content)
	if err != nil {
		return nil, err
	}

	// A parser that returns a document and no usable text chunks to nothing
	// while every stage reports success, so the only place this can be caught
	// is here, before the empty artifact is written. Observed against a
	// vision-language parser on whole slide decks, which is why it is a hard
	// failure rather than a warning: a deck missing from retrieval looks
	// exactly like a deck nobody asked about.
	//
	// Unlike every other chunking failure, this one recurs identically on each
	// reconcile: the input and the config are unchanged and the stage has no
	// attempt limit.
	if err := unstructured.ValidateChunks(content, chunks); err != nil {
		return nil, fmt.Errorf("chunking %s: %w", convertedFilePath, err)
	}

	return &unstructured.ChunksFile{
		ConvertedDocument: convertedFile.ConvertedDocument,
		ChunksDocument: &unstructured.ChunksDocument{
			Metadata: &unstructured.ChunksFileMetadata{
				ChunkingTool:          unstructured.ChunkingToolForStrategy(chunksGeneratorCR.Spec.ChunksGeneratorConfig.Strategy),
				ChunksGeneratorConfig: chunksGeneratorCR.Spec.ChunksGeneratorConfig,
				ConvertedFileMetadata: convertedFile.ConvertedDocument.Metadata,
			},
			Chunks: &unstructured.Chunks{
				Text: chunks,
			},
		},
	}, nil
}

func (r *ChunksGeneratorReconciler) findDependents(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &operatorv1alpha1.ChunksGeneratorList{}
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
func (r *ChunksGeneratorReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.ChunksGenerator{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&operatorv1alpha1.SourceCrawler{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.DocumentProcessor{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.VectorEmbeddingsGenerator{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.DestinationSyncer{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Complete(r)
}
func (r *ChunksGeneratorReconciler) handleError(ctx context.Context, chunksGeneratorCR *operatorv1alpha1.ChunksGenerator, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	if updateErr := controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
		chunksGeneratorCR.UpdateStatus("", reconcileErr)
	}); updateErr != nil {
		logger.Error(updateErr, "failed to update ChunksGenerator CR status")
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{}, reconcileErr
}
