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
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/filestore"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
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
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddataproducts,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddataproducts/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddataproducts/finalizers,verbs=update

func (r *UnstructuredDataProductReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("reconciling UnstructuredDataProduct")

	unstructuredDataProductCR := &operatorv1alpha1.UnstructuredDataProduct{}
	if err := r.Get(ctx, req.NamespacedName, unstructuredDataProductCR); err != nil {
		logger.Error(err, "failed to get UnstructuredDataProduct CR")
		return ctrl.Result{}, err
	}
	dataProductName := unstructuredDataProductCR.Name

	// set status to waiting
	unstructuredDataProductCR.SetWaiting()
	if err := r.Status().Update(ctx, unstructuredDataProductCR); err != nil {
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

	// now we may remove the force reconcile label as we have read all the files from the source and we are ready to accept more events
	if err := controllerutils.RemoveForceReconcileLabel(ctx, r.Client, unstructuredDataProductCR); err != nil {
		logger.Error(err, "failed to remove force reconcile label from UnstructuredDataProduct CR")
		return r.handleError(ctx, unstructuredDataProductCR, err)
	}

	documentProcessorKey := client.ObjectKey{Namespace: unstructuredDataProductCR.Namespace, Name: dataProductName}
	if err := controllerutils.AddForceReconcileLabelWithRetry(ctx, r.Client, documentProcessorKey, func() client.Object { return &operatorv1alpha1.DocumentProcessor{} }); err != nil {
		logger.Error(err, "failed to add force reconcile label to DocumentProcessor CR")
		return r.handleError(ctx, unstructuredDataProductCR, err)
	}

	successMessage := fmt.Sprintf("successfully reconciled unstructured data product: %s", dataProductName)
	key := client.ObjectKeyFromObject(unstructuredDataProductCR)
	if err := controllerutils.StatusUpdateWithRetry(ctx, r.Client, key, func() client.Object { return &operatorv1alpha1.UnstructuredDataProduct{} }, func(obj client.Object) {
		obj.(*operatorv1alpha1.UnstructuredDataProduct).UpdateStatus(successMessage, nil)
	}); err != nil {
		logger.Error(err, "failed to update UnstructuredDataProduct CR status", "namespace", key.Namespace, "name", key.Name)
		return r.handleError(ctx, unstructuredDataProductCR, err)
	}
	logger.Info("successfully updated UnstructuredDataProduct CR status", "status", unstructuredDataProductCR.Status)

	return ctrl.Result{}, nil
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
	if updateErr := controllerutils.StatusUpdateWithRetry(ctx, r.Client, key, func() client.Object { return &operatorv1alpha1.UnstructuredDataProduct{} }, func(obj client.Object) {
		obj.(*operatorv1alpha1.UnstructuredDataProduct).UpdateStatus("", reconcileErr)
	}); updateErr != nil {
		logger.Error(updateErr, "failed to update UnstructuredDataProduct CR status")
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{}, reconcileErr
}
