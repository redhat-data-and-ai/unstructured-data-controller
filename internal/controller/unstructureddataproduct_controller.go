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
	"sigs.k8s.io/controller-runtime/pkg/builder"
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
	UnstructuredDataProductControllerName = "UnstructuredDataProduct"
)

var (
	cacheDirectory    string
	dataStorageBucket string
)

// UnstructuredDataProductReconciler reconciles a UnstructuredDataProduct object
type UnstructuredDataProductReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	fileStore *filestore.FileStore
	sf        *snowflake.Client
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddataproducts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddataproducts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddataproducts/finalizers,verbs=update

//nolint:gocyclo
func (r *UnstructuredDataProductReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling", "controller", UnstructuredDataProductControllerName)

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

	unstructuredDataProductCR := &operatorv1alpha1.UnstructuredDataProduct{}
	if err := r.Get(ctx, req.NamespacedName, unstructuredDataProductCR); err != nil {
		logger.Error(err, "failed to get UnstructuredDataProduct CR")
		return ctrl.Result{}, err
	}
	dataProductName := unstructuredDataProductCR.Name
	isNewFileDetected := false
	hasForceReconcileLabel := controllerutils.HasForceReconcileLabel(unstructuredDataProductCR)

	if !hasForceReconcileLabel {
		// Check if document processor is there for this data product
		documentProcessorKey := client.ObjectKey{
			Namespace: unstructuredDataProductCR.Namespace,
			Name:      dataProductName,
		}
		existingDocumentProcessorCR := &operatorv1alpha1.DocumentProcessor{}
		err = r.Get(ctx, documentProcessorKey, existingDocumentProcessorCR)
		if err == nil {
			logger.Info("DocumentProcessor CR already exists for this data product, now checking for the status", "documentProcessorCR", existingDocumentProcessorCR.Name)
			for _, condition := range existingDocumentProcessorCR.Status.Conditions {
				if condition.Type == operatorv1alpha1.DocumentProcessorCondition && condition.Status == metav1.ConditionUnknown {
					logger.Info("DocumentProcessor CR is in waiting state, this event is triggered because of Owns and it is setWaiting in the document processor reconciler, will do early exit", "documentProcessorCR", existingDocumentProcessorCR.Name)
					return ctrl.Result{}, nil
				}
			}
		} else if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "failed to get DocumentProcessor CR")
			return ctrl.Result{}, err
		}

		// check if chunks generator is there for this data product
		chunksGeneratorKey := client.ObjectKey{
			Namespace: unstructuredDataProductCR.Namespace,
			Name:      dataProductName,
		}
		existingChunksGeneratorCR := &operatorv1alpha1.ChunksGenerator{}
		err = r.Get(ctx, chunksGeneratorKey, existingChunksGeneratorCR)
		if err == nil {
			logger.Info("ChunksGenerator CR already exists for this data product, now checking for the status", "chunksGeneratorCR", existingChunksGeneratorCR.Name)
			for _, condition := range existingChunksGeneratorCR.Status.Conditions {
				if condition.Type == operatorv1alpha1.ChunksGeneratorCondition && condition.Status == metav1.ConditionUnknown {
					logger.Info("ChunksGenerator CR is in waiting state, this event is triggered because of Owns and it is setWaiting in the chunks generator reconciler, will do early exit", "chunksGeneratorCR", existingChunksGeneratorCR.Name)
					return ctrl.Result{}, nil
				}
			}
		} else if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "failed to get ChunksGenerator CR")
			return ctrl.Result{}, err
		}

		//check if vector embeddings generator is there for this data product
		vectorEmbeddingsGeneratorKey := client.ObjectKey{
			Namespace: unstructuredDataProductCR.Namespace,
			Name:      dataProductName,
		}
		existingVectorEmbeddingsGeneratorCR := &operatorv1alpha1.VectorEmbeddingsGenerator{}
		err = r.Get(ctx, vectorEmbeddingsGeneratorKey, existingVectorEmbeddingsGeneratorCR)
		if err == nil {
			logger.Info("VectorEmbeddingsGenerator CR already exists for this data product, now checking for the status", "vectorEmbeddingsGeneratorCR", existingVectorEmbeddingsGeneratorCR.Name)
			for _, condition := range existingVectorEmbeddingsGeneratorCR.Status.Conditions {
				if condition.Type == operatorv1alpha1.VectorEmbeddingGenerationConditionType && condition.Status == metav1.ConditionUnknown {
					logger.Info("VectorEmbeddingsGenerator CR is in waiting state, this event is triggered because of Owns and it is setWaiting in the vector embeddings generator reconciler, will do early exit", "vectorEmbeddingsGeneratorCR", existingVectorEmbeddingsGeneratorCR.Name)
					return ctrl.Result{}, nil
				}
			}
		} else if client.IgnoreNotFound(err) != nil {
			logger.Error(err, "failed to get VectorEmbeddingsGenerator CR")
			return ctrl.Result{}, err
		}
	} else {
		logger.Info("force-reconcile label present, skipping early-exit checks to process new files from SQS/manual trigger")
	}

	unstructuredDataProductKey := client.ObjectKeyFromObject(unstructuredDataProductCR)
	if err := controllerutils.RemoveForceReconcileLabelWithRetry(ctx, r.Client, unstructuredDataProductKey,
		func() client.Object { return &operatorv1alpha1.UnstructuredDataProduct{} }); err != nil {
		logger.Error(err, "error removing the force-reconcile label from the UnstructuredDataProduct CR")
		return ctrl.Result{}, err
	}

	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.UnstructuredDataProduct{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		latest.SetWaiting()
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update UnstructuredDataProduct CR status")
		return ctrl.Result{}, err
	}

	// first, let's create (or update) the DocumentProcessor CR for this data product
	documentProcessorCR := &operatorv1alpha1.DocumentProcessor{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataProductName,
			Namespace: unstructuredDataProductCR.Namespace,
		},
		Spec: operatorv1alpha1.DocumentProcessorSpec{
			DataProduct:             dataProductName,
			DocumentProcessorConfig: unstructuredDataProductCR.Spec.DocumentProcessorConfig,
		},
	}

	if err := ctrl.SetControllerReference(unstructuredDataProductCR, documentProcessorCR, r.Scheme); err != nil {
		logger.Error(err, "failed to set controller reference for DocumentProcessor CR")
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
	}
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, documentProcessorCR, func() error {
		documentProcessorCR.Spec = operatorv1alpha1.DocumentProcessorSpec{
			DataProduct:             dataProductName,
			DocumentProcessorConfig: unstructuredDataProductCR.Spec.DocumentProcessorConfig,
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "failed to create/update DocumentProcessor CR")
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
	}
	logger.Info("DocumentProcessor CR created/updated", "result", result)

	// create ChunksGenerator CR for this data product here
	chunksGeneratorCR := &operatorv1alpha1.ChunksGenerator{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataProductName,
			Namespace: unstructuredDataProductCR.Namespace,
		},
		Spec: operatorv1alpha1.ChunksGeneratorSpec{
			DataProduct:           dataProductName,
			ChunksGeneratorConfig: unstructuredDataProductCR.Spec.ChunksGeneratorConfig,
		},
	}
	if err := ctrl.SetControllerReference(unstructuredDataProductCR, chunksGeneratorCR, r.Scheme); err != nil {
		logger.Error(err, "failed to set controller reference for ChunksGenerator CR")
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
	}
	result, err = controllerutil.CreateOrUpdate(ctx, r.Client, chunksGeneratorCR, func() error {
		chunksGeneratorCR.Spec = operatorv1alpha1.ChunksGeneratorSpec{
			DataProduct:           dataProductName,
			ChunksGeneratorConfig: unstructuredDataProductCR.Spec.ChunksGeneratorConfig,
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "failed to create/update ChunksGenerator CR")
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
	}
	logger.Info("ChunksGenerator CR created/updated", "result", result)

	// create vector embeddings generator cr
	vectorEmbeddingsGeneratorCR := &operatorv1alpha1.VectorEmbeddingsGenerator{
		ObjectMeta: metav1.ObjectMeta{
			Name:      dataProductName,
			Namespace: unstructuredDataProductCR.Namespace,
		},
		Spec: operatorv1alpha1.VectorEmbeddingsGeneratorSpec{
			DataProduct:                     dataProductName,
			VectorEmbeddingsGeneratorConfig: unstructuredDataProductCR.Spec.VectorEmbeddingsGeneratorConfig,
		},
	}
	if err := ctrl.SetControllerReference(unstructuredDataProductCR, vectorEmbeddingsGeneratorCR, r.Scheme); err != nil {
		logger.Error(err, "failed to set controller reference for VectorEmbeddingsGenerator CR")
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
	}
	result, err = controllerutil.CreateOrUpdate(ctx, r.Client, vectorEmbeddingsGeneratorCR, func() error {
		vectorEmbeddingsGeneratorCR.Spec = operatorv1alpha1.VectorEmbeddingsGeneratorSpec{
			DataProduct:                     dataProductName,
			VectorEmbeddingsGeneratorConfig: unstructuredDataProductCR.Spec.VectorEmbeddingsGeneratorConfig,
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "failed to create/update VectorEmbeddingsGenerator CR")
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
	}
	logger.Info("VectorEmbeddingsGenerator CR created/updated", "result", result)

	var source unstructured.DataSource
	switch unstructuredDataProductCR.Spec.SourceConfig.Type {
	case operatorv1alpha1.TypeS3:
		// read all files from the ingestion bucket and store them in the filestore only if file not exists
		source = &unstructured.S3BucketSource{
			Bucket: unstructuredDataProductCR.Spec.SourceConfig.S3Config.Bucket,
			Prefix: unstructuredDataProductCR.Spec.SourceConfig.S3Config.Prefix,
		}
	default:
		return r.handleError(ctx, unstructuredDataProductCR, fmt.Errorf("unsupported source type: %s", unstructuredDataProductCR.Spec.SourceConfig.Type), isNewFileDetected)
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
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
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

	isNewFileDetected, storedFiles, err := source.SyncFilesToFilestore(ctx, r.fileStore)
	if err != nil {
		logger.Error(err, "failed to store files to filestore")
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
	}
	logger.Info("successfully stored files to filestore", "files", storedFiles, "isNewFileDetected", isNewFileDetected)

	// Setup destination
	var destination unstructured.Destination
	switch unstructuredDataProductCR.Spec.DestinationConfig.Type {
	case operatorv1alpha1.DestinationTypeInternalStage:
		destination, err = r.setupSnowflakeDestination(ctx, unstructuredDataProductCR)
		if err != nil {
			return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
		}
	case operatorv1alpha1.TypeS3:
		destination, err = setupS3Destination(unstructuredDataProductCR, dataProductName)
		if err != nil {
			if IsAWSClientNotInitializedError(err) {
				logger.Info("ControllerConfig has not initialized destination S3 client yet, will try again in a bit ...")
				return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
			}
			return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
		}
	default:
		err = fmt.Errorf("unsupported destination type: %s", unstructuredDataProductCR.Spec.DestinationConfig.Type)
		logger.Error(err, "unsupported destination type")
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
	}

	// list all files in the filestore for the data product
	filePaths, err := r.fileStore.ListFilesInPath(ctx, dataProductName)
	if err != nil {
		logger.Error(err, "failed to list files in path")
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
	}
	// extract the vector embeddings files that are to be ingested to destination
	filterEmbeddingsFiles := unstructured.FilterVectorEmbeddingsFilePaths(filePaths)
	logger.Info("files to ingest to destination", "files", filterEmbeddingsFiles)

	// ingest the embeddings files to destination
	if err = destination.SyncFilesToDestination(ctx, r.fileStore, filterEmbeddingsFiles); err != nil {
		logger.Error(err, "failed to ingest embeddings files to destination")
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
	}
	logger.Info("successfully ingested embeddings files to destination")

	// all done, let's update the status to ready
	successMessage := fmt.Sprintf("successfully reconciled unstructured data product: %s", dataProductName)
	if err = controllerutils.StatusUpdateWithRetry(ctx, r.Client, unstructuredDataProductKey, func() client.Object { return &operatorv1alpha1.UnstructuredDataProduct{} }, func(obj client.Object) {
		obj.(*operatorv1alpha1.UnstructuredDataProduct).UpdateStatus(successMessage, nil, isNewFileDetected)
	}); err != nil {
		logger.Error(err, "failed to update UnstructuredDataProduct CR status", "namespace", unstructuredDataProductKey.Namespace, "name", unstructuredDataProductKey.Name)
		return r.handleError(ctx, unstructuredDataProductCR, err, isNewFileDetected)
	}
	logger.Info("successfully updated UnstructuredDataProduct CR status", "status", unstructuredDataProductCR.Status)

	// requeue to check in every 5 minutes if there are new files in case
	// sqs dont put force-reconcile label on the CR
	return ctrl.Result{
		RequeueAfter: 5 * time.Minute,
	}, nil
}

// setupSnowflakeDestination returns a Snowflake internal stage destination for the given CR.
func (r *UnstructuredDataProductReconciler) setupSnowflakeDestination(ctx context.Context, unstructuredDataProductCR *operatorv1alpha1.UnstructuredDataProduct) (unstructured.Destination, error) {
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
		Stage:    unstructuredDataProductCR.Spec.DestinationConfig.SnowflakeInternalStageConfig.Stage,
		Database: unstructuredDataProductCR.Spec.DestinationConfig.SnowflakeInternalStageConfig.Database,
		Schema:   unstructuredDataProductCR.Spec.DestinationConfig.SnowflakeInternalStageConfig.Schema,
	}, nil
}

// setupS3Destination returns an S3 destination for the given CR
func setupS3Destination(unstructuredDataProductCR *operatorv1alpha1.UnstructuredDataProduct, dataProductName string) (unstructured.Destination, error) {
	destCfg := unstructuredDataProductCR.Spec.DestinationConfig.S3DestinationConfig
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
func (r *UnstructuredDataProductReconciler) SetupWithManager(mgr ctrl.Manager) error {
	labelPredicate := controllerutils.ForceReconcilePredicate()
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.UnstructuredDataProduct{}, builder.WithPredicates(predicate.Or(predicate.GenerationChangedPredicate{}, labelPredicate))).
		Owns(&operatorv1alpha1.DocumentProcessor{}).
		Owns(&operatorv1alpha1.ChunksGenerator{}).
		Owns(&operatorv1alpha1.VectorEmbeddingsGenerator{}).
		Complete(r)
}

func (r *UnstructuredDataProductReconciler) handleError(ctx context.Context, unstructuredDataProductCR *operatorv1alpha1.UnstructuredDataProduct, err error, isNewFileDetected bool) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	unstructuredDataProductKey := client.ObjectKeyFromObject(unstructuredDataProductCR)
	if updateErr := controllerutils.StatusUpdateWithRetry(ctx, r.Client, unstructuredDataProductKey, func() client.Object { return &operatorv1alpha1.UnstructuredDataProduct{} }, func(obj client.Object) {
		obj.(*operatorv1alpha1.UnstructuredDataProduct).UpdateStatus("", err, isNewFileDetected)
	}); updateErr != nil {
		logger.Error(updateErr, "failed to update UnstructuredDataProduct CR status")
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{}, err
}
