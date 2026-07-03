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
	"errors"
	"fmt"
	"net/url"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/google"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

const (
	SourceCrawlerControllerName  = "SourceCrawler"
	defaultCrawlerResyncInterval = 2 * time.Minute
)

// SourceCrawlerReconciler reconciles a SourceCrawler object
type SourceCrawlerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=sourcecrawlers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=sourcecrawlers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=sourcecrawlers/finalizers,verbs=update
// +kubebuilder:rbac:groups="",namespace=unstructured-controller-namespace,resources=secrets,verbs=get;list;watch

func (r *SourceCrawlerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling", "controller", SourceCrawlerControllerName)

	isHealthy, err := IsConfigCRHealthy(ctx, r.Client, req.Namespace)
	if err != nil {
		logger.Error(err, "failed to check if ControllerConfig CR is healthy")
		return ctrl.Result{}, err
	}
	if !isHealthy {
		logger.Info("ControllerConfig CR is not ready yet, will try again in a bit ...")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	sourceCrawlerCR := &operatorv1alpha1.SourceCrawler{}
	if err := r.Get(ctx, req.NamespacedName, sourceCrawlerCR); err != nil {
		logger.Error(err, "failed to get SourceCrawler CR")
		return ctrl.Result{}, err
	}

	if err := controllerutils.StatusPatch(ctx, r.Client, sourceCrawlerCR, func() {
		sourceCrawlerCR.SetWaiting()
	}); err != nil {
		logger.Error(err, "failed to update SourceCrawler CR status")
		return ctrl.Result{}, err
	}

	fs, err := filestore.New(ctx, cacheDirectory, dataStorageBucket)
	if err != nil {
		if IsAWSClientNotInitializedError(err) {
			logger.Info("ControllerConfig has not initialized AWS clients yet, will try again in a bit ...")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		return ctrl.Result{}, r.handleError(ctx, sourceCrawlerCR, fmt.Errorf("failed to create filestore: %w", err))
	}
	parentPipeline, err := controllerutils.ParentPipelineNameFromOwnerReference(sourceCrawlerCR)
	if err != nil {
		return ctrl.Result{}, r.handleError(ctx, sourceCrawlerCR, err)
	}
	sourceCrawlerConfig := sourceCrawlerCR.Spec.SourceCrawlerConfig
	outputDir := unstructured.StagePath(parentPipeline, sourceCrawlerCR.Spec.StageName)

	var source unstructured.DataSource
	switch sourceCrawlerConfig.Type {
	case operatorv1alpha1.TypeS3:
		sourceAWSConfig, err := controllerutils.AWSConfigFromSecret(ctx, r.Client, sourceCrawlerCR.Spec.SecretRef, sourceCrawlerCR.Namespace)
		if err != nil {
			return ctrl.Result{}, r.handleError(ctx, sourceCrawlerCR, fmt.Errorf("failed to get source credentials: %w", err))
		}
		sourceS3Client, err := awsclienthandler.NewS3Client(ctx, sourceAWSConfig)
		if err != nil {
			return ctrl.Result{}, r.handleError(ctx, sourceCrawlerCR, fmt.Errorf("failed to create source S3 client: %w", err))
		}
		source = &unstructured.S3BucketSource{
			S3Client:  sourceS3Client,
			Bucket:    sourceCrawlerConfig.S3Config.Bucket,
			Prefix:    sourceCrawlerConfig.S3Config.Prefix,
			OutputDir: outputDir,
		}

	case operatorv1alpha1.TypeGoogleDrive:
		gdriveSource, err := r.buildGDriveSource(ctx, sourceCrawlerCR, sourceCrawlerConfig.GoogleDriveConfig, outputDir)
		if err != nil {
			return ctrl.Result{}, r.handleError(ctx, sourceCrawlerCR, err)
		}
		// Close releases HTTP connection pools after each reconcile. If TLS
		// re-establishment becomes a performance bottleneck, switch to caching
		// the Google client per pipeline (keyed by secret resourceVersion).
		defer gdriveSource.Close()
		source = gdriveSource

	default:
		return ctrl.Result{}, r.handleError(ctx, sourceCrawlerCR, fmt.Errorf("unsupported source type: %s", sourceCrawlerConfig.Type))
	}

	storedFiles, err := source.SyncFilesToFilestore(ctx, fs)
	if err != nil {
		return ctrl.Result{}, r.handleError(ctx, sourceCrawlerCR, fmt.Errorf("failed to store files to filestore: %w", err))
	}
	logger.Info("successfully stored files to filestore", "count", len(storedFiles))

	successMessage := fmt.Sprintf("successfully reconciled source crawler: %s", sourceCrawlerCR.Name)
	if err := controllerutils.StatusPatch(ctx, r.Client, sourceCrawlerCR, func() {
		sourceCrawlerCR.Status.FilesProcessed += int64(len(storedFiles))
		sourceCrawlerCR.UpdateStatus(successMessage, nil)
	}); err != nil {
		logger.Error(err, "failed to update SourceCrawler CR status")
		return ctrl.Result{}, r.handleError(ctx, sourceCrawlerCR, err)
	}

	// determine requeue strategy
	if sourceCrawlerConfig.Type == operatorv1alpha1.TypeS3 {
		sqsQueueURL := sourceCrawlerConfig.S3Config.SQSQueueURL
		if sqsQueueURL != "" {
			return handleSQSWakeUp(ctx, sqsQueueURL, sourceCrawlerConfig.S3Config.Bucket, sourceCrawlerConfig.S3Config.Prefix), nil
		}
	}
	return ctrl.Result{RequeueAfter: defaultCrawlerResyncInterval}, nil
}

func handleSQSWakeUp(ctx context.Context, queueURL, bucket, prefix string) ctrl.Result {
	logger := log.FromContext(ctx)

	sqsClient, err := awsclienthandler.GetSQSClient()
	if err != nil {
		logger.Error(err, "failed to initialize SQS client")
		return ctrl.Result{RequeueAfter: 10 * time.Second}
	}

	// DrainSQSQueue long-polls (up to 20s), so it blocks until messages
	// arrive or the timeout expires — no separate poll interval needed.
	hasMessages, err := awsclienthandler.DrainSQSQueue(ctx, sqsClient, queueURL, bucket, prefix)
	if err != nil {
		logger.Error(err, "failed to drain SQS queue")
		return ctrl.Result{Requeue: true}
	}

	if hasMessages {
		logger.Info("SQS messages received, requeuing immediately for state diff")
	}

	// requeue immediately — the long poll inside DrainSQSQueue is the wait
	return ctrl.Result{Requeue: true}
}

func (r *SourceCrawlerReconciler) buildGDriveSource(
	ctx context.Context,
	sourceCrawlerCR *operatorv1alpha1.SourceCrawler,
	gdriveConfig *operatorv1alpha1.GoogleDriveConfig,
	outputDir string,
) (*unstructured.GDriveSource, error) {
	if gdriveConfig == nil {
		return nil, errors.New("gdriveConfig is required when source type is gdrive")
	}

	ctrlConfig := GoogleDriveControllerCfg
	if ctrlConfig == nil {
		return nil, errors.New("googleDriveConfig is not set in ControllerConfig")
	}

	credentialsJSON, err := controllerutils.GDriveCredentialsFromSecret(
		ctx, r.Client, sourceCrawlerCR.Spec.SecretRef, sourceCrawlerCR.Namespace)
	if err != nil {
		return nil, fmt.Errorf("failed to get gdrive credentials: %w", err)
	}

	googleClient, err := google.NewClientFromJSON(ctx, credentialsJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to create google client: %w", err)
	}

	if LDAPClient == nil {
		return nil, errors.New("LDAP client not initialized in ControllerConfig")
	}
	if CacheClient == nil {
		return nil, errors.New("cache client not initialized in ControllerConfig")
	}

	gdriveClient, err := gdrive.NewClient(googleClient, LDAPClient, CacheClient)
	if err != nil {
		return nil, fmt.Errorf("failed to create gdrive client: %w", err)
	}

	maxRetries := ctrlConfig.MaxRetries
	if maxRetries == 0 {
		maxRetries = 3
	}
	concurrentFolders := ctrlConfig.ConcurrentFolders
	if concurrentFolders == 0 {
		concurrentFolders = 5
	}
	concurrentDownloads := ctrlConfig.ConcurrentDownloads
	if concurrentDownloads == 0 {
		concurrentDownloads = 10
	}

	folderIDs := make([]string, 0, len(gdriveConfig.Folders))
	for _, f := range gdriveConfig.Folders {
		id, err := extractGDriveFolderID(f.URL)
		if err != nil {
			return nil, fmt.Errorf("invalid Google Drive folder URL %q: %w", f.URL, err)
		}
		folderIDs = append(folderIDs, id)
	}
	skipFolderNames := make([]string, len(gdriveConfig.SkipFolders))
	for i, s := range gdriveConfig.SkipFolders {
		skipFolderNames[i] = s.Pattern
	}

	return &unstructured.GDriveSource{
		GDriveClient:        gdriveClient,
		FolderIDs:           folderIDs,
		SkipFolderNames:     skipFolderNames,
		MaxRetries:          maxRetries,
		ConcurrentFolders:   concurrentFolders,
		ConcurrentDownloads: concurrentDownloads,
		OutputDir:           outputDir,
	}, nil
}

// extractGDriveFolderID parses a Google Drive folder URL and returns the folder ID.
// Accepts URLs like "https://drive.google.com/drive/folders/<id>" or
// "https://drive.google.com/drive/u/0/folders/<id>" with optional query params.
// Also accepts a raw folder ID directly.
func extractGDriveFolderID(rawURL string) (string, error) {
	if !strings.Contains(rawURL, "/") {
		return rawURL, nil
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("failed to parse URL: %w", err)
	}
	parts := strings.Split(strings.TrimRight(u.Path, "/"), "/")
	for i, part := range parts {
		if part == "folders" && i+1 < len(parts) {
			return parts[i+1], nil
		}
	}
	return "", fmt.Errorf("could not extract folder ID from URL path: %s", u.Path)
}

func (r *SourceCrawlerReconciler) handleError(ctx context.Context, sourceCrawlerCR *operatorv1alpha1.SourceCrawler, err error) error {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	if updateErr := controllerutils.StatusPatch(ctx, r.Client, sourceCrawlerCR, func() {
		sourceCrawlerCR.UpdateStatus("", reconcileErr)
	}); updateErr != nil {
		logger.Error(updateErr, "failed to update SourceCrawler CR status")
		return updateErr
	}
	return reconcileErr
}

// findDependents maps a changed pipeline stage back to the SourceCrawlers that depend on it.
//
// Given a SourceCrawler CR like:
//
//	apiVersion: operator.dataverse.redhat.com/v1alpha1
//	kind: SourceCrawler
//	metadata:
//	  name: my-crawler
//	  ownerReferences:
//	    - name: my-pipeline        # ← ParentPipelineNameFromOwnerReference returns "my-pipeline"
//	spec:
//	  depends:
//	    - name: chunker            # ← dependency name
//	    - name: doc-processor
//
// If a DocumentProcessor named "my-pipeline-doc-processor" changes,
// this function matches it via: "my-pipeline" + "-" + "doc-processor" == "my-pipeline-doc-processor"
// and enqueues "my-crawler" for reconciliation.
func (r *SourceCrawlerReconciler) findDependents(ctx context.Context, obj client.Object) []reconcile.Request {
	// obj is the object that has changed, so it's not SourceCrawler
	list := &operatorv1alpha1.SourceCrawlerList{}
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

// findSecretDependents returns reconcile requests for SourceCrawlers that reference the changed Secret via SecretRef.
func (r *SourceCrawlerReconciler) findSecretDependents(ctx context.Context, obj client.Object) []reconcile.Request {
	list := &operatorv1alpha1.SourceCrawlerList{}
	if err := r.List(ctx, list, client.InNamespace(obj.GetNamespace())); err != nil {
		return nil
	}
	secretName := obj.GetName()
	var requests []reconcile.Request
	for _, item := range list.Items {
		if item.Spec.SecretRef == secretName {
			requests = append(requests, reconcile.Request{
				NamespacedName: types.NamespacedName{Name: item.Name, Namespace: item.Namespace},
			})
		}
	}
	return requests
}

// SetupWithManager registers watches on all downstream pipeline stages and secrets so that
// changes to any dependency trigger a reconcile of the owning SourceCrawler.
func (r *SourceCrawlerReconciler) SetupWithManager(mgr ctrl.Manager, maxConcurrentReconciles int) error {
	return ctrl.NewControllerManagedBy(mgr).
		WithOptions(controller.Options{MaxConcurrentReconciles: maxConcurrentReconciles}).
		For(&operatorv1alpha1.SourceCrawler{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Watches(&operatorv1alpha1.DocumentProcessor{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.ChunksGenerator{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.VectorEmbeddingsGenerator{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&operatorv1alpha1.DestinationSyncer{}, handler.EnqueueRequestsFromMapFunc(r.findDependents), builder.WithPredicates(controllerutils.FilesProcessedChangedPredicate{})).
		Watches(&corev1.Secret{}, handler.EnqueueRequestsFromMapFunc(r.findSecretDependents)).
		Named("sourcecrawler").
		Complete(r)
}
