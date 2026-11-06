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

	"github.com/google/go-cmp/cmp"
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
	// docling's chunker before it is given up on. The usual causes do not
	// clear by themselves, so an unbounded retry takes a slot on every pass
	// and chunks nothing.
	maxDoclingChunkingAttempts = 3

	// maxChunkTaskAge is how long a task may stay in flight before a job that
	// cannot be collected is counted as a failed attempt rather than waited
	// on. Far longer than any real chunking run, so anything near it is gone
	// rather than slow.
	maxChunkTaskAge = 30 * time.Minute
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
		// Inside the patch, not before it: StatusPatch diffs against a copy
		// taken when it is called, so an earlier change produces an empty
		// patch. An edit to the spec gives given-up files another chance,
		// since chunking usually fails on configuration, not on the file.
		if chunksGeneratorCR.ResetPermanentlyFailingFiles() {
			logger.Info("spec changed, retrying files that chunking had given up on")
		}
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

	// The convert stage writes page images beside its documents, and only the
	// documents are chunkable.
	filePaths = unstructured.ConvertedFilePaths(filePaths)

	chunkingErrors := []error{}
	skippedFiles := []string{}
	var filesProcessed int64

	// Collect finished tasks before submitting more, so a completed one gives
	// its slot back and frees capacity for this pass. The slice is copied
	// because reconcileChunkJob patches status and so rewrites the one on the
	// resource underneath the loop.
	for _, job := range slices.Clone(chunksGeneratorCR.Status.Jobs) {
		// A job with no task id is a failed attempt waiting to be sent again,
		// not a task to ask about. The file loop below resubmits it and
		// carries its attempt count.
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
			// Both chunking paths refuse work at capacity rather than
			// queueing, so the file waits for the next pass instead of
			// failing the stage. The langchain match is on text because that
			// client offers nothing else.
			if errors.Is(err, docling.ErrSemaphoreUnavailable) ||
				strings.Contains(err.Error(), langchain.SemaphoreAcquireError) {
				logger.Error(err, "failed to process converted file, semaphore acquire error, will try again later", "file", convertedFilePath)
				skippedFiles = append(skippedFiles, convertedFilePath)
				continue
			}
			if errors.Is(err, unstructured.ErrNotAConvertedFile) {
				// Not every object under the convert stage prefix is a
				// converted file, and retrying cannot help. Logged at error
				// level because something put a file here that does not
				// belong.
				logger.Error(err, "skipping file that is not a converted file", "file", convertedFilePath)
				continue
			}
			if errors.Is(err, unstructured.ErrNoStructuredDocument) {
				// A file converted before the structured document was asked
				// for will not change on its own, and the attempt limit
				// cannot stop the retries because it only covers failures
				// after a task id exists. Skipped, not requeued: converting
				// the file again writes a new converted file, which wakes
				// this stage.
				logger.Error(err, "skipping file with no structured document to chunk", "file", convertedFilePath)
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

	// Recorded here rather than in the success patch below, because every
	// return between the two skips that patch and the count is lost for good:
	// the next pass finds the artifacts current and never counts them. A
	// collected task counts itself in the patch that forgets its job.
	if filesProcessed > 0 {
		if err := controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
			chunksGeneratorCR.Status.FilesProcessed += filesProcessed
		}); err != nil {
			logger.Error(err, "failed to record processed files on ChunksGenerator CR status")
			return r.handleError(ctx, chunksGeneratorCR, err)
		}
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

	// failChunkJob returns nil so one bad file does not stop the corpus, so
	// this is the only place giving up can be reported. Without it a mistyped
	// tokenizer settles into a green resource that has written nothing.
	if given := len(chunksGeneratorCR.Status.PermanentlyFailingFiles); given > 0 {
		err := fmt.Errorf("gave up chunking %d file(s) after %d attempts each; "+
			"see status.permanentlyFailingFiles, and editing the spec retries them",
			given, maxDoclingChunkingAttempts)
		return r.handleError(ctx, chunksGeneratorCR, err)
	}

	successMessage := fmt.Sprintf("successfully reconciled chunks generator: %s", chunksGeneratorCR.Name)
	if err := controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
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
	// would start it again on every requeue, each start taking another slot
	// and orphaning the task id it overwrites. A job with no task id is a
	// failed attempt and falls through to be resubmitted.
	if job := chunksGeneratorCR.GetJobByFilePath(convertedFilePath); job != nil && job.TaskID != "" {
		logger.Info("chunking task already running for file, nothing to start",
			"file", convertedFilePath, "taskID", job.TaskID)
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

	convertedFile, err := r.retrieveConvertedFile(ctx, convertedFilePath)
	if err != nil {
		return false, err
	}

	config := chunksGeneratorCR.Spec.ChunksGeneratorConfig
	chunker, err := chunkerFor(config)
	if err != nil {
		return false, err
	}

	result, err := chunker.Chunk(ctx, convertedFile.ConvertedDocument)
	if err != nil {
		logger.Error(err, "failed to chunk file")
		return false, err
	}

	// The chunker started work instead of finishing it, so there is nothing
	// to write yet: record the task and let a later reconcile collect it. The
	// file does not count as processed until then.
	if result.Pending() {
		return false, r.recordChunkJob(ctx, chunksGeneratorCR, convertedFile, convertedFilePath, config, result.TaskID)
	}

	if err := r.storeChunks(ctx, convertedFile, config, result.Chunks,
		chunksGeneratorCR.Spec.DependsOn[0].Name, convertedFilePath, chunksFilePath); err != nil {
		return false, err
	}

	return true, nil
}

// chunkerFor returns the chunker a generator's configuration selects. It is
// the only place the strategy is turned into behaviour; everything downstream
// asks the chunker what happened rather than asking the config what it is.
func chunkerFor(config operatorv1alpha1.ChunksGeneratorConfig) (unstructured.Chunker, error) {
	switch config.Strategy {
	case operatorv1alpha1.ChunkingStrategyRecursiveCharacter:
		return &unstructured.RecursiveCharacterSplitter{
			LangchainClient: langchainClient,
			Config: &textsplitter.Options{
				Separators:    config.RecursiveCharacterSplitterConfig.Separators,
				ChunkSize:     config.RecursiveCharacterSplitterConfig.ChunkSize,
				ChunkOverlap:  config.RecursiveCharacterSplitterConfig.ChunkOverlap,
				KeepSeparator: config.RecursiveCharacterSplitterConfig.KeepSeparator,
			},
		}, nil

	case operatorv1alpha1.ChunkingStrategyMarkdown:
		return &unstructured.MarkdownSplitter{
			LangchainClient: langchainClient,
			Config: &textsplitter.Options{
				ChunkSize:            config.MarkdownSplitterConfig.ChunkSize,
				ChunkOverlap:         config.MarkdownSplitterConfig.ChunkOverlap,
				CodeBlocks:           config.MarkdownSplitterConfig.CodeBlocks,
				ReferenceLinks:       config.MarkdownSplitterConfig.ReferenceLinks,
				KeepHeadingHierarchy: config.MarkdownSplitterConfig.HeadingHierarchy,
				JoinTableRows:        config.MarkdownSplitterConfig.JoinTableRows,
			},
		}, nil

	case operatorv1alpha1.ChunkingStrategyToken:
		return &unstructured.TokenSplitter{
			LangchainClient: langchainClient,
			Config: &textsplitter.Options{
				ChunkSize:         config.TokenSplitterConfig.ChunkSize,
				ChunkOverlap:      config.TokenSplitterConfig.ChunkOverlap,
				ModelName:         config.TokenSplitterConfig.ModelName,
				EncodingName:      config.TokenSplitterConfig.EncodingName,
				AllowedSpecial:    config.TokenSplitterConfig.AllowedSpecial,
				DisallowedSpecial: config.TokenSplitterConfig.DisallowedSpecial,
			},
		}, nil

	case operatorv1alpha1.ChunkingStrategyDoclingHybrid:
		return &unstructured.DoclingChunker{
			Client:  doclingClient,
			Options: chunkingOptionsFrom(config.HybridChunkerConfig),
		}, nil

	default:
		return nil, fmt.Errorf("invalid strategy: %s", config.Strategy)
	}
}

// retrieveConvertedFile reads a converted file and checks it is one.
func (r *ChunksGeneratorReconciler) retrieveConvertedFile(
	ctx context.Context, convertedFilePath string,
) (*unstructured.ConvertedFile, error) {
	raw, err := r.fileStore.Retrieve(ctx, convertedFilePath)
	if err != nil {
		return nil, fmt.Errorf("reading converted file %s: %w", convertedFilePath, err)
	}

	convertedFile := &unstructured.ConvertedFile{}
	if err := json.Unmarshal(raw, convertedFile); err != nil {
		return nil, fmt.Errorf("decoding converted file %s: %w", convertedFilePath, err)
	}
	if err := convertedFile.Validate(); err != nil {
		return nil, fmt.Errorf("checking converted file %s: %w", convertedFilePath, err)
	}

	return convertedFile, nil
}

// storeChunks writes the chunks artifact for a document. The configuration is
// passed in rather than read from the resource, because a task can outlive a
// change to the spec and the artifact must describe the chunking that ran.
func (r *ChunksGeneratorReconciler) storeChunks(
	ctx context.Context, convertedFile *unstructured.ConvertedFile,
	config operatorv1alpha1.ChunksGeneratorConfig, chunks []unstructured.Chunk,
	convertStage, convertedFilePath, chunksFilePath string,
) error {
	// Written here rather than by the chunker, which resolves geometry and
	// has no business knowing where files live. Left empty for a page that
	// rendered no image, which most input formats do not, so a reader can
	// tell "no image exists" from a path that would 404.
	for chunkIndex := range chunks {
		for regionIndex := range chunks[chunkIndex].Regions {
			region := &chunks[chunkIndex].Regions[regionIndex]
			if convertedFile.ConvertedDocument.Content.HasRenderedPage(region.PageNo) {
				region.Image = unstructured.PageImageDestinationKey(
					convertedFilePath, convertStage, region.PageNo)
			}
		}
	}

	// A parser that returns a document with no usable text chunks to nothing
	// while every stage reports success, so this is the only place to catch
	// it, before the empty artifact is written.
	content := convertedFile.ConvertedDocument.Content.Markdown
	if err := unstructured.ValidateChunks(content, chunks); err != nil {
		return fmt.Errorf("chunking %s: %w", chunksFilePath, err)
	}

	chunksFile := unstructured.ChunksFile{
		ChunksDocument: &unstructured.ChunksDocument{
			Metadata: &unstructured.ChunksFileMetadata{
				ChunkingTool:          unstructured.ChunkingToolForStrategy(config.Strategy),
				ChunksGeneratorConfig: config,
				ConvertedFileMetadata: convertedFile.ConvertedDocument.Metadata,
			},
			Chunks: unstructured.NewChunks(chunks),
		},
	}

	chunksFileBytes, err := json.Marshal(chunksFile)
	if err != nil {
		return err
	}

	return r.fileStore.Store(ctx, chunksFilePath, chunksFileBytes)
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
		// Both default to true via SetDefaults. Nil is read as true so an
		// undefaulted config does not quietly produce chunks with no text to
		// display and tables serialised as value triplets.
		IncludeRawText:    config.IncludeRawText == nil || *config.IncludeRawText,
		UseMarkdownTables: config.UseMarkdownTables == nil || *config.UseMarkdownTables,
	}
}

// recordChunkJob saves a task the chunker started, so a later reconcile can
// collect it. The configuration travels with the job because the spec can
// change while the task runs.
func (r *ChunksGeneratorReconciler) recordChunkJob(
	ctx context.Context, chunksGeneratorCR *operatorv1alpha1.ChunksGenerator,
	convertedFile *unstructured.ConvertedFile, convertedFilePath string,
	config operatorv1alpha1.ChunksGeneratorConfig, taskID string,
) error {
	log.FromContext(ctx).Info("chunking task accepted", "taskID", taskID, "file", convertedFilePath)

	// A job left behind by a failed attempt carries the count forward. The
	// count is the only record of how many times this file has been tried,
	// so restarting it at one here would retry forever.
	attempt := 1
	if previous := chunksGeneratorCR.GetJobByFilePath(convertedFilePath); previous != nil {
		attempt = previous.Attempts + 1
	}

	return controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
		chunksGeneratorCR.AddOrUpdateJob(operatorv1alpha1.ChunkJob{
			FilePath:              convertedFilePath,
			FileIdentifier:        convertedFile.ConvertedDocument.Metadata.FileIdentifier,
			ChunksGeneratorConfig: config,
			TaskID:                taskID,
			Status:                string(docling.TaskStatusPending),
			CreatedOn:             time.Now().Format(time.RFC3339),
			Attempts:              attempt,
		})
	})
}

// reconcileChunkJob collects a chunking task that a previous reconcile
// started, and acts on what comes back.
//
// A task still running is left alone, a finished one has its chunks written
// and its job forgotten, and one that ended badly is retried until the attempt
// limit. A job is never left carrying the id of a task that has ended:
// collecting it again would release a slot a second time, taking one from a
// task that is still running.
func (r *ChunksGeneratorReconciler) reconcileChunkJob(ctx context.Context, job operatorv1alpha1.ChunkJob,
	chunksGeneratorCR *operatorv1alpha1.ChunksGenerator, inputPath, outputPath string) error {
	logger := log.FromContext(ctx)

	chunker, err := chunkerFor(job.ChunksGeneratorConfig)
	if err != nil {
		return err
	}

	// Only a chunker whose work outlives the call can have produced a task,
	// so a job for one that cannot is a bug rather than a condition to
	// handle.
	collector, ok := chunker.(unstructured.AsyncChunker)
	if !ok {
		return fmt.Errorf("a chunking task is recorded for %s under strategy %s, which does not defer work",
			job.FilePath, job.ChunksGeneratorConfig.Strategy)
	}

	// The work may already be done: storing the chunks and forgetting the job
	// are two steps, so a failure in between leaves a job whose task has
	// ended and whose output is written. Polling it again would release a
	// second permit and count the file twice, so notice first.
	chunksFilePath := unstructured.RemapToOutputDir(job.FilePath, inputPath, outputPath)
	written, err := r.chunksAlreadyWritten(ctx, job, chunksFilePath)
	if err != nil {
		return err
	}
	if written {
		logger.Info("chunks for this task are already stored, forgetting the job",
			"file", job.FilePath, "taskID", job.TaskID)
		return controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
			chunksGeneratorCR.DeleteJobByFilePath(job.FilePath)
			chunksGeneratorCR.Status.FilesProcessed++
		})
	}

	// Handed over as a function, not read here: the chunker needs the
	// document only once the task has finished, and most polls find it still
	// running.
	var convertedFile *unstructured.ConvertedFile
	readDocument := func() (*unstructured.ConvertedDocument, error) {
		var readErr error
		convertedFile, readErr = r.retrieveConvertedFile(ctx, job.FilePath)
		if readErr != nil {
			return nil, readErr
		}
		return convertedFile.ConvertedDocument, nil
	}

	result, err := collector.Collect(ctx, job.TaskID, readDocument)
	switch {
	case err != nil && !errors.Is(err, docling.ErrChunkTaskEnded):
		// As far as we can tell the task is alive and still holds its slot,
		// so the next pass collects the same id rather than starting again.
		return r.failStuckChunkJob(ctx, job, chunksGeneratorCR,
			fmt.Errorf("collecting the chunking task for %s: %w", job.FilePath, err))

	case err != nil:
		return r.failChunkJob(ctx, job, chunksGeneratorCR, err)

	case result.Pending():
		logger.Info("chunking task is still running", "file", job.FilePath, "taskID", job.TaskID)
		return nil
	}

	// Any failure from here on goes through failChunkJob rather than being
	// returned, even a transient upload error: the task has ended, so leaving
	// the job with its id would have the next pass collect it again.
	if err := r.storeChunks(ctx, convertedFile, job.ChunksGeneratorConfig, result.Chunks,
		chunksGeneratorCR.Spec.DependsOn[0].Name, job.FilePath, chunksFilePath); err != nil {
		return r.failChunkJob(ctx, job, chunksGeneratorCR, err)
	}
	logger.Info("stored chunks for file", "file", job.FilePath, "taskID", job.TaskID, "chunks", len(result.Chunks))

	// Counted here, in the same patch that forgets the job, because the
	// reconcile that submitted this task finished long ago.
	return controllerutils.StatusPatch(ctx, r.Client, chunksGeneratorCR, func() {
		chunksGeneratorCR.DeleteJobByFilePath(job.FilePath)
		chunksGeneratorCR.Status.FilesProcessed++
	})
}

// chunksAlreadyWritten reports whether this job's chunks are already stored.
// It compares against the job's own configuration, not the spec's: an artifact
// from an earlier configuration is not this job's output, and taking it as
// such would leave the file chunked by something no longer asked for.
func (r *ChunksGeneratorReconciler) chunksAlreadyWritten(
	ctx context.Context, job operatorv1alpha1.ChunkJob, chunksFilePath string,
) (bool, error) {
	exists, err := r.fileStore.Exists(ctx, chunksFilePath)
	if err != nil || !exists {
		return false, err
	}

	raw, err := r.fileStore.Retrieve(ctx, chunksFilePath)
	if err != nil {
		return false, fmt.Errorf("reading chunks file %s: %w", chunksFilePath, err)
	}

	chunksFile := unstructured.ChunksFile{}
	if err := json.Unmarshal(raw, &chunksFile); err != nil {
		return false, fmt.Errorf("decoding chunks file %s: %w", chunksFilePath, err)
	}
	if chunksFile.ChunksDocument == nil || chunksFile.ChunksDocument.Metadata == nil {
		return false, nil
	}

	stored := chunksFile.ChunksDocument.Metadata
	return stored.ChunkingTool == unstructured.ChunkingToolForStrategy(job.ChunksGeneratorConfig.Strategy) &&
		cmp.Equal(stored.ChunksGeneratorConfig, job.ChunksGeneratorConfig), nil
}

// failStuckChunkJob counts an attempt against a job that cannot even be asked
// about, once it is too old to be waiting on anything real. A restarted
// service is enough to pin one forever otherwise: its task id polls as missing
// on every pass and its attempt count never moves.
//
// Giving up on the id may abandon a task that is still running, and its slot
// with it. The client is shared with conversion, so that slot is lost to both
// stages until the process restarts.
func (r *ChunksGeneratorReconciler) failStuckChunkJob(ctx context.Context, job operatorv1alpha1.ChunkJob,
	chunksGeneratorCR *operatorv1alpha1.ChunksGenerator, cause error) error {
	created, err := time.Parse(time.RFC3339, job.CreatedOn)
	// An unparseable timestamp is treated as old. It can only come from a
	// job this controller did not write, and leaving it pinned is worse.
	if err == nil && time.Since(created) < maxChunkTaskAge {
		return cause
	}

	return r.failChunkJob(ctx, job, chunksGeneratorCR,
		fmt.Errorf("chunking task unreachable for over %s: %w", maxChunkTaskAge, cause))
}

// failChunkJob gives a file another attempt, or gives up on it. Under the
// limit it keeps the job and clears the task id, which the submit path reads
// as "start this again and carry the count"; the count lives nowhere else, so
// dropping the job here would retry forever.
func (r *ChunksGeneratorReconciler) failChunkJob(ctx context.Context, job operatorv1alpha1.ChunkJob,
	chunksGeneratorCR *operatorv1alpha1.ChunksGenerator, cause error) error {
	logger := log.FromContext(ctx)

	if job.Attempts >= maxDoclingChunkingAttempts {
		// Not returned as an error: the file is recorded on status, and
		// failing the stage for one file that will never succeed stops every
		// other file from being written.
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
