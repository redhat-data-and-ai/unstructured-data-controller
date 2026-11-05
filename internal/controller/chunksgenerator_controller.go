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

	// maxChunkTaskAge is how long a task may stay in flight before a job
	// that cannot be collected is counted as a failed attempt rather than
	// waited on.
	//
	// Generous on purpose. Chunking a 143 page document measured under 20
	// seconds end to end, so anything near this is not slow, it is gone.
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
		// taken when it is called, so a change made earlier is already in
		// the base and produces an empty patch.
		//
		// An edit to the spec gives up-on files another chance. Chunking
		// usually fails for a whole corpus at once, on configuration rather
		// than on the file, so without this the fix for it recovers nothing.
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
			// capacity rather than queueing behind it, so the file waits for
			// the next pass instead of failing the stage. The langchain match
			// is on text because that client offers nothing else; the docling
			// one is a sentinel, so a file whose own path contains the phrase
			// cannot be mistaken for a busy client.
			if errors.Is(err, docling.ErrSemaphoreUnavailable) ||
				strings.Contains(err.Error(), langchain.SemaphoreAcquireError) {
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
			if errors.Is(err, unstructured.ErrNoStructuredDocument) {
				// A chunker that splits on structure cannot do anything with
				// a file converted before the structured document was asked
				// for, and the file will not change on its own. Failing the
				// stage would retry it on every reconcile forever, with no
				// attempt limit to stop it: the limit only covers failures
				// that happen after a task id exists, and this one happens
				// before the request is even sent. Skipped instead, so the
				// rest of the corpus is still chunked. Not requeued either:
				// nothing here changes it, and the file being converted
				// again writes a new converted file, which wakes this stage
				// on its own.
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
	// the files are chunked, so the next pass finds their artifacts current
	// and never counts them. A completed task counts itself inside the patch
	// that forgets its job, so it is not in this total.
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

	// Giving up on a file is not an error that reaches here: failChunkJob
	// records the file and returns nil so one bad file does not stop the
	// rest of the corpus. That makes this the only place the stage can
	// report it, and reporting success instead is the exact silent failure
	// this stage exists to avoid. A mistyped tokenizer returns success with
	// no chunks for every file in a corpus, which would otherwise settle
	// into a green resource that has written nothing.
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
	// against the service and orphaning the task id it overwrites.
	//
	// A job with no task id is a failed attempt waiting to be retried, and
	// falls through so the chunker below starts it again and carries its
	// attempt count. A chunker that finishes within the call never records a
	// job at all, so this only ever fires on the remote path.
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

	// The chunker started work instead of finishing it, which a chunker that
	// calls out over the network has to. There is nothing to write yet, so
	// the task is recorded and a later reconcile collects it. The file does
	// not count as processed until it has been.
	if result.Pending() {
		return false, r.recordChunkJob(ctx, chunksGeneratorCR, convertedFile, convertedFilePath, config, result.TaskID)
	}

	if err := r.storeChunks(ctx, convertedFile, config, result.Chunks, chunksFilePath); err != nil {
		return false, err
	}

	return true, nil
}

// chunkerFor returns the chunker a generator's configuration selects.
//
// This is the only place the strategy is turned into behaviour. Everything
// downstream asks the chunker what happened rather than asking the config
// what it is, which is what keeps a chunker that answers immediately and one
// that answers later on the same path.
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

// storeChunks writes the chunks artifact for a document.
//
// The configuration is passed in rather than read from the resource. A task
// can outlive a change to the spec, and the artifact has to describe the
// chunking that actually ran, or the next reconcile compares it against a
// configuration it was not produced under.
func (r *ChunksGeneratorReconciler) storeChunks(
	ctx context.Context, convertedFile *unstructured.ConvertedFile,
	config operatorv1alpha1.ChunksGeneratorConfig, chunks []unstructured.Chunk, chunksFilePath string,
) error {
	// A parser that returns a document and no usable text chunks to nothing
	// while every stage reports success, so the only place this can be caught
	// is here, before the empty artifact is written. Observed against a
	// vision-language parser on whole slide decks, which is why it is a hard
	// failure rather than a warning: a deck missing from retrieval looks
	// exactly like a deck nobody asked about.
	//
	// Unlike every other chunking failure, this one recurs identically on
	// each reconcile: the input and the config are unchanged.
	content := convertedFile.ConvertedDocument.Content.Markdown
	if err := unstructured.ValidateChunks(content, chunks); err != nil {
		return fmt.Errorf("chunking %s: %w", chunksFilePath, err)
	}

	chunksFile := unstructured.ChunksFile{
		ConvertedDocument: convertedFile.ConvertedDocument,
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
		// Both of these default to true and SetDefaults fills them in for
		// this strategy. A nil is read as true rather than as false so that a
		// config that somehow reached here undefaulted does not quietly
		// produce chunks with no text to display, and tables serialised as
		// "RowLabel, ColumnName = value" triplets.
		IncludeRawText:    config.IncludeRawText == nil || *config.IncludeRawText,
		UseMarkdownTables: config.UseMarkdownTables == nil || *config.UseMarkdownTables,
	}
}

// recordChunkJob saves a task the chunker started, so a later reconcile can
// collect it.
//
// The configuration travels with the job rather than being read again when
// the result arrives. The spec can change while the task runs, and the
// artifact written then has to describe the chunking that actually ran.
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
// A task still running is left alone. One that has finished has its chunks
// written and its job forgotten. One that ended badly is retried until the
// attempt limit, after which the file is recorded as permanently failing.
//
// A job is never left on status carrying the id of a task that has ended.
// Collecting an ended task releases its semaphore slot a second time, and a
// weighted semaphore keeps no record of who holds what, so the second
// release takes a slot from a task that is still running rather than
// failing.
func (r *ChunksGeneratorReconciler) reconcileChunkJob(ctx context.Context, job operatorv1alpha1.ChunkJob,
	chunksGeneratorCR *operatorv1alpha1.ChunksGenerator, inputPath, outputPath string) error {
	logger := log.FromContext(ctx)

	chunker, err := chunkerFor(job.ChunksGeneratorConfig)
	if err != nil {
		return err
	}

	// Only a chunker whose work outlives the call can have produced a task,
	// so a job for one that cannot is a bug rather than a condition to
	// handle. Reporting it beats stubbing a Collect onto chunkers that
	// finish before they return.
	collector, ok := chunker.(unstructured.AsyncChunker)
	if !ok {
		return fmt.Errorf("a chunking task is recorded for %s under strategy %s, which does not defer work",
			job.FilePath, job.ChunksGeneratorConfig.Strategy)
	}

	// Read before collecting, because the chunker resolves each chunk's
	// elements against the document and the reconcile that submitted the
	// task is long gone. The cost is one read per in flight task per pass,
	// and the number in flight is capped by the client's own concurrency
	// limit rather than by the size of the corpus.
	convertedFile, err := r.retrieveConvertedFile(ctx, job.FilePath)
	if err != nil {
		return r.failStuckChunkJob(ctx, job, chunksGeneratorCR, err)
	}

	result, err := collector.Collect(ctx, job.TaskID, convertedFile.ConvertedDocument)
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
	// returned, even one as transient as a failed upload. The task has ended
	// and given its slot back, so leaving the job with its id would have the
	// next pass collect it again. Retrying means chunking the file afresh.
	chunksFilePath := unstructured.RemapToOutputDir(job.FilePath, inputPath, outputPath)
	if err := r.storeChunks(ctx, convertedFile, job.ChunksGeneratorConfig, result.Chunks, chunksFilePath); err != nil {
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

// failStuckChunkJob counts an attempt against a job that cannot even be
// asked about, once it is too old to be waiting on anything real.
//
// Without it a job can be pinned forever. Nothing here touches the job, so
// its attempt count never moves and the file is skipped as already in
// flight, which is correct while the task is running and wrong once it is
// not. The service restarting is enough: the task id then polls as missing
// on every pass and the stage stays red with no way out.
//
// Giving up on the id may abandon a task that really is still running, and
// its slot with it. Note whose slot: the client is shared with conversion,
// so an abandoned permit is taken from both stages until the process
// restarts. It is still the better of two bad options, because the
// alternative is a file that is never chunked and a stage that never
// recovers, but it is a cost this stage imposes on another one. The real
// fix is for the client to account for permits per task, which also closes
// the double release that conversion already has.
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

// failChunkJob gives a file another attempt, or gives up on it.
//
// Under the limit it keeps the job and clears the task id, which is what the
// submit path reads as "start this again and carry the count". The count
// lives nowhere else, so dropping the job here would restart it and retry
// forever.
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
