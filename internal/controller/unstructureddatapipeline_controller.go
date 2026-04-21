/*
Copyright 2025.

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
	"fmt"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/util/retry"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

const (
	UnstructuredDataPipelineControllerName = "UnstructuredDataPipeline"
)

var (
	cacheDirectory    string
	dataStorageBucket string
)

// UnstructuredDataPipelineReconciler reconciles a UnstructuredDataPipeline object
type UnstructuredDataPipelineReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	fileStore *filestore.FileStore
	sf        *snowflake.Client
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddatapipelines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddatapipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddatapipelines/finalizers,verbs=update

func (r *UnstructuredDataPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling", "controller", UnstructuredDataPipelineControllerName)

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

	unstructuredDataPipelineCR := &operatorv1alpha1.UnstructuredDataPipeline{}
	if err := r.Get(ctx, req.NamespacedName, unstructuredDataPipelineCR); err != nil {
		logger.Error(err, "failed to get UnstructuredDataPipeline CR")
		return ctrl.Result{}, err
	}
	dataProductName := unstructuredDataPipelineCR.Name

	unstructuredDataPipelineKey := client.ObjectKeyFromObject(unstructuredDataPipelineCR)
	if err := controllerutils.RemoveForceReconcileLabelWithRetry(ctx, r.Client, unstructuredDataPipelineKey,
		func() client.Object { return &operatorv1alpha1.UnstructuredDataPipeline{} }); err != nil {
		logger.Error(err, "error removing the force-reconcile label from the UnstructuredDataPipeline CR")
		return ctrl.Result{}, err
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.UnstructuredDataPipeline{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.SetWaiting()
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update UnstructuredDataPipeline CR status")
		return ctrl.Result{}, err
	}

	// first, let's create (or update) the DocumentProcessor CR for this data product
	documentProcessorCR := &operatorv1alpha1.DocumentProcessor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataProductName,
			Namespace: unstructuredDataPipelineCR.Namespace,
		},
		Spec: operatorv1alpha1.DocumentProcessorSpec{
			DataProduct:             dataProductName,
			DocumentProcessorConfig: unstructuredDataPipelineCR.Spec.DocumentProcessorConfig,
		},
	}
	// result, err := kubecontrollerutil.CreateOrUpdate(ctx, r.Client, documentProcessorCR, func() error { return nil })
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, documentProcessorCR, func() error {
		documentProcessorCR.Spec = operatorv1alpha1.DocumentProcessorSpec{
			DataProduct:             dataProductName,
			DocumentProcessorConfig: unstructuredDataPipelineCR.Spec.DocumentProcessorConfig,
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "failed to create/update DocumentProcessor CR")
		return r.handleError(ctx, unstructuredDataPipelineCR, err)
	}
	logger.Info("DocumentProcessor CR created/updated", "result", result)

	// create ChunksGenerator CR for this data product here
	chunksGeneratorCR := &operatorv1alpha1.ChunksGenerator{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataProductName,
			Namespace: unstructuredDataPipelineCR.Namespace,
		},
		Spec: operatorv1alpha1.ChunksGeneratorSpec{
			DataProduct:           dataProductName,
			ChunksGeneratorConfig: unstructuredDataPipelineCR.Spec.ChunksGeneratorConfig,
		},
	}
	result, err = controllerutil.CreateOrUpdate(ctx, r.Client, chunksGeneratorCR, func() error {
		chunksGeneratorCR.Spec = operatorv1alpha1.ChunksGeneratorSpec{
			DataProduct:           dataProductName,
			ChunksGeneratorConfig: unstructuredDataPipelineCR.Spec.ChunksGeneratorConfig,
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "failed to create/update ChunksGenerator CR")
		return r.handleError(ctx, unstructuredDataPipelineCR, err)
	}
	logger.Info("ChunksGenerator CR created/updated", "result", result)

	// create vector embeddings generator cr
	vectorEmbeddingsGeneratorCR := &operatorv1alpha1.VectorEmbeddingsGenerator{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataProductName,
			Namespace: unstructuredDataPipelineCR.Namespace,
		},
		Spec: operatorv1alpha1.VectorEmbeddingsGeneratorSpec{
			DataProduct:                     dataProductName,
			VectorEmbeddingsGeneratorConfig: unstructuredDataPipelineCR.Spec.VectorEmbeddingsGeneratorConfig,
		},
	}

	result, err = controllerutil.CreateOrUpdate(ctx, r.Client, vectorEmbeddingsGeneratorCR, func() error {
		vectorEmbeddingsGeneratorCR.Spec = operatorv1alpha1.VectorEmbeddingsGeneratorSpec{
			DataProduct:                     dataProductName,
			VectorEmbeddingsGeneratorConfig: unstructuredDataPipelineCR.Spec.VectorEmbeddingsGeneratorConfig,
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "failed to create/update VectorEmbeddingsGenerator CR")
		return r.handleError(ctx, unstructuredDataPipelineCR, err)
	}
	logger.Info("VectorEmbeddingsGenerator CR created/updated", "result", result)

	var source unstructured.DataSource
	switch unstructuredDataPipelineCR.Spec.SourceConfig.Type {
	case operatorv1alpha1.TypeS3:
		// read all files from the ingestion bucket and store them in the filestore only if file not exists
		source = &unstructured.S3BucketSource{
			Bucket: unstructuredDataPipelineCR.Spec.SourceConfig.S3Config.Bucket,
			Prefix: unstructuredDataPipelineCR.Spec.SourceConfig.S3Config.Prefix,
		}
	default:
		return r.handleError(ctx, unstructuredDataPipelineCR, fmt.Errorf("unsupported source type: %s", unstructuredDataPipelineCR.Spec.SourceConfig.Type))
	}

	if !IsControllerConfigGlobalsSet(cacheDirectory, dataStorageBucket) {
		logger.Info("ControllerConfig has not set cacheDirectory/dataStorageBucket yet, will try again in a bit ...")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}
	fs, err := filestore.New(ctx, cacheDirectory, dataStorageBucket)
	if err != nil {
		if IsAWSClientNotInitializedError(err) {
			logger.Info("ControllerConfig has not initialized AWS clients yet, will try again in a bit ...")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		logger.Error(err, "failed to create filestore")
		return r.handleError(ctx, unstructuredDataPipelineCR, err)
	}

	r.fileStore = fs

	// cacheDirectory: /var/data/unstructured/
	// dataStorageBucket: data-storage-bucket

	// /var/data/unstructured/dataproduct/file1.pdf
	// /var/data/unstructured/dataproduct/file1.pdf-metadata.json
	// /var/data/unstructured/dataproduct/file1.pdf-converted.json
	// /var/data/unstructured/dataproduct/file1.pdf-chunks.json
	// /var/data/unstructured/dataproduct/file1.pdf-vector-embeddings.json

	// data-storage-bucket/dataproduct/file1.pdf
	// data-storage-bucket/dataproduct/file1.pdf-metadata.json
	// data-storage-bucket/dataproduct/file1.pdf-converted.json
	// data-storage-bucket/dataproduct/file1.pdf-chunks.json
	// data-storage-bucket/dataproduct/file1.pdf-vector-embeddings.json

	storedFiles, err := source.SyncFilesToFilestore(ctx, r.fileStore)
	if err != nil {
		logger.Error(err, "failed to store files to filestore")
		return r.handleError(ctx, unstructuredDataPipelineCR, err)
	}
	logger.Info("successfully stored files to filestore", "files", storedFiles)

	// add force reconcile label to the DocumentProcessor CR
	documentProcessorKey := client.ObjectKey{
		Namespace: unstructuredDataPipelineCR.Namespace,
		Name:      dataProductName,
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestDocumentProcessorCR := &operatorv1alpha1.DocumentProcessor{}
		if getErr := r.Get(ctx, documentProcessorKey, latestDocumentProcessorCR); getErr != nil {
			return getErr
		}
		return controllerutils.AddForceReconcileLabel(ctx, r.Client, latestDocumentProcessorCR)
	}); err != nil {
		logger.Error(err, "failed to add force reconcile label to DocumentProcessor CR")
		return r.handleError(ctx, unstructuredDataPipelineCR, err)
	}

	// Setup destination
	var destination unstructured.Destination
	switch unstructuredDataPipelineCR.Spec.DestinationConfig.Type {
	case operatorv1alpha1.DestinationTypeInternalStage:
		var err error
		destination, err = r.setupSnowflakeDestination(ctx, unstructuredDataPipelineCR)
		if err != nil {
			return r.handleError(ctx, unstructuredDataPipelineCR, err)
		}
	case operatorv1alpha1.TypeS3:
		var err error
		destination, err = setupS3Destination(unstructuredDataPipelineCR, dataProductName)
		if err != nil {
			if IsAWSClientNotInitializedError(err) {
				logger.Info("ControllerConfig has not initialized destination S3 client yet, will try again in a bit ...")
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
			return r.handleError(ctx, unstructuredDataPipelineCR, err)
		}
	default:
		err := fmt.Errorf("unsupported destination type: %s", unstructuredDataPipelineCR.Spec.DestinationConfig.Type)
		logger.Error(err, "unsupported destination type")
		return r.handleError(ctx, unstructuredDataPipelineCR, err)
	}

	// list all files in the filestore for the data product
	filePaths, err := r.fileStore.ListFilesInPath(ctx, dataProductName)
	if err != nil {
		logger.Error(err, "failed to list files in path")
		return r.handleError(ctx, unstructuredDataPipelineCR, err)
	}
	// extract the vector embeddings files that are to be ingested to destination
	filterEmbeddingsFiles := unstructured.FilterVectorEmbeddingsFilePaths(filePaths)
	logger.Info("files to ingest to destination", "files", filterEmbeddingsFiles)

	// ingest the embeddings files to destination
	if err = destination.SyncFilesToDestination(ctx, r.fileStore, filterEmbeddingsFiles); err != nil {
		logger.Error(err, "failed to ingest embeddings files to destination")
		return r.handleError(ctx, unstructuredDataPipelineCR, err)
	}
	logger.Info("successfully ingested embeddings files to destination")

	// all done, let's update the status to ready
	successMessage := fmt.Sprintf("successfully reconciled unstructured data product: %s", dataProductName)
	if err := controllerutils.StatusUpdateWithRetry(ctx, r.Client, unstructuredDataPipelineKey, func() client.Object { return &operatorv1alpha1.UnstructuredDataPipeline{} }, func(obj client.Object) {
		obj.(*operatorv1alpha1.UnstructuredDataPipeline).UpdateStatus(successMessage, nil)
	}); err != nil {
		logger.Error(err, "failed to update UnstructuredDataPipeline CR status", "namespace", unstructuredDataPipelineKey.Namespace, "name", unstructuredDataPipelineKey.Name)
		return r.handleError(ctx, unstructuredDataPipelineCR, err)
	}
	logger.Info("successfully updated UnstructuredDataPipeline CR status", "status", unstructuredDataPipelineCR.Status)

	ctrlResult := ctrl.Result{}
	if UnstructuredDataPipelineResyncInterval != nil {
		ctrlResult.RequeueAfter = time.Duration(*UnstructuredDataPipelineResyncInterval) * time.Minute
	}
	return ctrlResult, nil
}

// setupSnowflakeDestination returns a Snowflake internal stage destination for the given CR.
func (r *UnstructuredDataPipelineReconciler) setupSnowflakeDestination(ctx context.Context, unstructuredDataPipelineCR *operatorv1alpha1.UnstructuredDataPipeline) (unstructured.Destination, error) {
	logger := log.FromContext(ctx)
	sf, err := snowflake.GetClient(ctx)
	if err != nil {
		logger.Error(err, "failed to get snowflake client")
		return nil, err
	}
	r.sf = sf
	return &unstructured.SnowflakeInternalStage{
		Client:   sf,
		Role:     sf.GetRole(),
		Stage:    unstructuredDataPipelineCR.Spec.DestinationConfig.SnowflakeInternalStageConfig.Stage,
		Database: unstructuredDataPipelineCR.Spec.DestinationConfig.SnowflakeInternalStageConfig.Database,
		Schema:   unstructuredDataPipelineCR.Spec.DestinationConfig.SnowflakeInternalStageConfig.Schema,
	}, nil
}

// setupS3Destination returns an S3 destination for the given CR
func setupS3Destination(unstructuredDataPipelineCR *operatorv1alpha1.UnstructuredDataPipeline, dataProductName string) (unstructured.Destination, error) {
	destCfg := unstructuredDataPipelineCR.Spec.DestinationConfig.S3DestinationConfig
	destinationS3Client, err := awsclienthandler.GetDestinationS3Client()
	if err != nil {
		return nil, err
	}
	return &unstructured.S3Destination{
		S3Client:        destinationS3Client,
		Bucket:          destCfg.Bucket,
		Prefix:          destCfg.Prefix,
		DataProductName: dataProductName,
	}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *UnstructuredDataPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	labelPredicate := controllerutils.ForceReconcilePredicate()
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.UnstructuredDataPipeline{}).
		WithEventFilter(predicate.Or(predicate.GenerationChangedPredicate{}, labelPredicate)).
		Complete(r)
}

func (r *UnstructuredDataPipelineReconciler) handleError(ctx context.Context, unstructuredDataPipelineCR *operatorv1alpha1.UnstructuredDataPipeline, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	unstructuredDataPipelineKey := client.ObjectKeyFromObject(unstructuredDataPipelineCR)
	if updateErr := controllerutils.StatusUpdateWithRetry(ctx, r.Client, unstructuredDataPipelineKey, func() client.Object { return &operatorv1alpha1.UnstructuredDataPipeline{} }, func(obj client.Object) {
		obj.(*operatorv1alpha1.UnstructuredDataPipeline).UpdateStatus("", reconcileErr)
	}); updateErr != nil {
		logger.Error(updateErr, "failed to update UnstructuredDataPipeline CR status")
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{}, err
}
