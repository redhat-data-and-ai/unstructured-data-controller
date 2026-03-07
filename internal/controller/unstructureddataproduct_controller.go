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

	"github.com/aws/aws-sdk-go-v2/service/s3"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
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

var (
	cacheDirectory    string
	dataStorageBucket string
)

const (
	UnstructuredDataProductControllerName = "UnstructuredDataProduct"
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
// +kubebuilder:rbac:groups="",namespace=unstructured-controller-namespace,resources=secrets,verbs=get

func (r *UnstructuredDataProductReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling UnstructuredDataProduct")

	unstructuredDataProductCR := &operatorv1alpha1.UnstructuredDataProduct{}
	if err := r.Get(ctx, req.NamespacedName, unstructuredDataProductCR); err != nil {
		logger.Error(err, "failed to get UnstructuredDataProduct CR")
		return ctrl.Result{}, err
	}
	dataProductName := unstructuredDataProductCR.Name

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
	// result, err := kubecontrollerutil.CreateOrUpdate(ctx, r.Client, documentProcessorCR, func() error { return nil })
	result, err := controllerutil.CreateOrUpdate(ctx, r.Client, documentProcessorCR, func() error {
		documentProcessorCR.Spec = operatorv1alpha1.DocumentProcessorSpec{
			DataProduct:             dataProductName,
			DocumentProcessorConfig: unstructuredDataProductCR.Spec.DocumentProcessorConfig,
		}
		return nil
	})
	if err != nil {
		logger.Error(err, "failed to create/update DocumentProcessor CR")
		return r.handleError(ctx, unstructuredDataProductCR, err)
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
	result, err = controllerutil.CreateOrUpdate(ctx, r.Client, chunksGeneratorCR, func() error { return nil })
	if err != nil {
		logger.Error(err, "failed to create/update ChunksGenerator CR")
		return r.handleError(ctx, unstructuredDataProductCR, err)
	}
	logger.Info("ChunksGenerator CR created/updated", "result", result)

	var source unstructured.DataSource
	switch unstructuredDataProductCR.Spec.SourceConfig.Type {
	case operatorv1alpha1.SourceTypeS3:
		// read all files from the ingestion bucket and store them in the filestore only if file not exists
		source = &unstructured.S3BucketSource{
			Bucket: unstructuredDataProductCR.Spec.SourceConfig.S3Config.Bucket,
			Prefix: unstructuredDataProductCR.Spec.SourceConfig.S3Config.Prefix,
		}
	default:
		return r.handleError(ctx, unstructuredDataProductCR, fmt.Errorf("unsupported source type: %s", unstructuredDataProductCR.Spec.SourceConfig.Type))
	}

	fs, err := filestore.New(ctx, cacheDirectory, dataStorageBucket)
	if err != nil {
		logger.Error(err, "failed to create filestore")
		return r.handleError(ctx, unstructuredDataProductCR, err)
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
		return r.handleError(ctx, unstructuredDataProductCR, err)
	}
	logger.Info("successfully stored files to filestore", "files", storedFiles)

	// // now we may remove the force reconcile label as we have read all the files from the source and we are ready to accept more events
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.UnstructuredDataProduct{}
		if err := r.Get(ctx, req.NamespacedName, latest); err != nil {
			return err
		}
		return controllerutils.RemoveForceReconcileLabel(ctx, r.Client, latest)
	}); err != nil {
		logger.Error(err, "failed to remove force reconcile label from UnstructuredDataProduct CR")
		return r.handleError(ctx, unstructuredDataProductCR, err)
	}

	// add force reconcile label to the DocumentProcessor CR
	documentProcessorKey := client.ObjectKey{
		Namespace: unstructuredDataProductCR.Namespace,
		Name:      dataProductName,
	}
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latestDocumentProcessorCR := &operatorv1alpha1.DocumentProcessor{}
		if err := r.Get(ctx, documentProcessorKey, latestDocumentProcessorCR); err != nil {
			return err
		}
		return controllerutils.AddForceReconcileLabel(ctx, r.Client, latestDocumentProcessorCR)
	}); err != nil {
		logger.Error(err, "failed to add force reconcile label to DocumentProcessor CR")
		return r.handleError(ctx, unstructuredDataProductCR, err)
	}

	// Setup destination
	var destination unstructured.Destination
	switch unstructuredDataProductCR.Spec.DestinationConfig.Type {
	case operatorv1alpha1.DestinationTypeInternalStage:
		var err error
		destination, err = r.setupSnowflakeDestination(ctx, unstructuredDataProductCR)
		if err != nil {
			return r.handleError(ctx, unstructuredDataProductCR, err)
		}
	case operatorv1alpha1.DestinationTypeS3:
		var err error
		destination, err = r.setupS3Destination(ctx, unstructuredDataProductCR, dataProductName)
		if err != nil {
			return r.handleError(ctx, unstructuredDataProductCR, err)
		}
	default:
		err := fmt.Errorf("unsupported destination type: %s", unstructuredDataProductCR.Spec.DestinationConfig.Type)
		logger.Error(err, "unsupported destination type")
		return r.handleError(ctx, unstructuredDataProductCR, err)
	}

	// list all files in the filestore for the data product
	filePaths, err := r.fileStore.ListFilesInPath(ctx, dataProductName)
	if err != nil {
		logger.Error(err, "failed to list files in path")
		return r.handleError(ctx, unstructuredDataProductCR, err)
	}
	// extract the chunked files that are to be ingested to destination
	filterChunksFiles := unstructured.FilterChunksFilePaths(filePaths)
	logger.Info("files to ingest to destination", "files", filterChunksFiles)

	// ingest the chunks files to destination
	if err := destination.SyncFilesToDestination(ctx, r.fileStore, filterChunksFiles); err != nil {
		logger.Error(err, "failed to ingest chunks files to destination")
		return r.handleError(ctx, unstructuredDataProductCR, err)
	}
	logger.Info("successfully ingested chunks files to destination")

	// all done, let's update the status to ready
	successMessage := fmt.Sprintf("successfully reconciled unstructured data product: %s", dataProductName)
	key := client.ObjectKeyFromObject(unstructuredDataProductCR)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		res := &operatorv1alpha1.UnstructuredDataProduct{}
		if err := r.Get(ctx, key, res); err != nil {
			return err
		}
		res.UpdateStatus(successMessage, nil)
		return r.Status().Update(ctx, res)
	}); err != nil {
		logger.Error(err, "failed to update UnstructuredDataProduct CR status", "namespace", key.Namespace, "name", key.Name)
		return r.handleError(ctx, unstructuredDataProductCR, err)
	}
	logger.Info("successfully updated UnstructuredDataProduct CR status", "status", unstructuredDataProductCR.Status)

	return ctrl.Result{}, nil
}

// setupSnowflakeDestination returns a Snowflake internal stage destination for the given CR.
func (r *UnstructuredDataProductReconciler) setupSnowflakeDestination(ctx context.Context, unstructuredDataProductCR *operatorv1alpha1.UnstructuredDataProduct) (unstructured.Destination, error) {
	logger := log.FromContext(ctx)
	sf, err := snowflake.GetClient()
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
func (r *UnstructuredDataProductReconciler) setupS3Destination(ctx context.Context, unstructuredDataProductCR *operatorv1alpha1.UnstructuredDataProduct, dataProductName string) (unstructured.Destination, error) {
	logger := log.FromContext(ctx)
	destCfg := unstructuredDataProductCR.Spec.DestinationConfig.S3DestinationConfig
	var s3Client *s3.Client
	if destCfg.S3SecretName != "" {
		awsSecret := &corev1.Secret{}
		if err := r.Get(ctx, types.NamespacedName{Name: destCfg.S3SecretName, Namespace: unstructuredDataProductCR.Namespace}, awsSecret); err != nil {
			logger.Error(err, "failed to get AWS secret for S3 destination", "secret", destCfg.S3SecretName)
			return nil, fmt.Errorf("failed to get AWS secret %s: %w", destCfg.S3SecretName, err)
		}
		awsConfig := awsclienthandler.AWSConfig{
			Region:          string(awsSecret.Data["AWS_REGION"]),
			AccessKeyID:     string(awsSecret.Data["AWS_ACCESS_KEY_ID"]),
			SecretAccessKey: string(awsSecret.Data["AWS_SECRET_ACCESS_KEY"]),
			SessionToken:    string(awsSecret.Data["AWS_SESSION_TOKEN"]),
			Endpoint:        string(awsSecret.Data["AWS_ENDPOINT"]),
		}
		var err error
		s3Client, err = awsclienthandler.NewS3ClientFromConfig(ctx, &awsConfig)
		if err != nil {
			logger.Error(err, "failed to create S3 client from secret")
			return nil, err
		}
	} else {
		var err error
		s3Client, err = awsclienthandler.GetS3Client()
		if err != nil {
			logger.Error(err, "failed to get S3 client for destination")
			return nil, err
		}
	}
	return &unstructured.S3Destination{
		S3Client:        s3Client,
		Bucket:          destCfg.Bucket,
		Prefix:          destCfg.Prefix,
		DataProductName: dataProductName,
	}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *UnstructuredDataProductReconciler) SetupWithManager(mgr ctrl.Manager) error {
	labelPredicate := controllerutils.ForceReconcilePredicate()
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.UnstructuredDataProduct{}).
		WithEventFilter(predicate.Or(predicate.GenerationChangedPredicate{}, labelPredicate)).
		Complete(r)
}

func (r *UnstructuredDataProductReconciler) handleError(ctx context.Context, unstructuredDataProductCR *operatorv1alpha1.UnstructuredDataProduct, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	key := client.ObjectKeyFromObject(unstructuredDataProductCR)
	if err := retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.UnstructuredDataProduct{}
		if getErr := r.Get(ctx, key, latest); getErr != nil {
			return getErr
		}
		latest.UpdateStatus("", reconcileErr)
		return r.Status().Update(ctx, latest)
	}); err != nil {
		logger.Error(err, "failed to update UnstructuredDataProduct CR status")
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, reconcileErr
}
