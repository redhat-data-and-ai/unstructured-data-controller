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
	"slices"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
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
)

const (
	UnstructuredDataPipelineControllerName = "UnstructuredDataPipeline"
	PipelineLabel                          = "operator.dataverse.redhat.com/unstructured-data-pipeline"
	UDPFinalizer                           = "operator.dataverse.redhat.com/udp-finalizer"
)

var (
	cacheDirectory    string
	dataStorageBucket string
)

// UnstructuredDataPipelineReconciler reconciles a UnstructuredDataPipeline object
type UnstructuredDataPipelineReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddatapipelines,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddatapipelines/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=unstructureddatapipelines/finalizers,verbs=update
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=sourcecrawlers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=sourcecrawlers/status,verbs=get
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=documentprocessors,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=documentprocessors/status,verbs=get
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=chunksgenerators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=chunksgenerators/status,verbs=get
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=vectorembeddingsgenerators,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=vectorembeddingsgenerators/status,verbs=get
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=destinationsyncers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=destinationsyncers/status,verbs=get

func (r *UnstructuredDataPipelineReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	// Fetch the CR before the health check so that deletion is never blocked
	// by an unhealthy ControllerConfig.
	unstructuredDataPipelineCR := &operatorv1alpha1.UnstructuredDataPipeline{}
	if err := r.Get(ctx, req.NamespacedName, unstructuredDataPipelineCR); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}
	// DeepCopy to avoid mutating the shared informer cache
	unstructuredDataPipelineCR = unstructuredDataPipelineCR.DeepCopy()

	if !unstructuredDataPipelineCR.DeletionTimestamp.IsZero() {
		return r.handleDeletion(ctx, unstructuredDataPipelineCR)
	}

	logger.Info("reconciling", "controller", UnstructuredDataPipelineControllerName)

	isHealthy, err := IsConfigCRHealthy(ctx, r.Client, req.Namespace)
	if err != nil {
		logger.Error(err, "failed to check if ControllerConfig CR is healthy")
		return ctrl.Result{}, err
	}
	if !isHealthy {
		logger.Info("ControllerConfig CR is not ready yet, will try again in a bit ...")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	// Ensure the finalizer is registered before any other work.
	if !controllerutil.ContainsFinalizer(unstructuredDataPipelineCR, UDPFinalizer) {
		controllerutil.AddFinalizer(unstructuredDataPipelineCR, UDPFinalizer)
		if err := r.Update(ctx, unstructuredDataPipelineCR); err != nil {
			logger.Error(err, "failed to add finalizer")
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	stages := unstructuredDataPipelineCR.Spec.Stages
	if err := operatorv1alpha1.ValidateStages(stages); err != nil {
		logger.Error(err, "invalid pipeline stages")
		if _, updateErr := r.handleError(ctx, unstructuredDataPipelineCR, fmt.Errorf("invalid pipeline stages: %w", err)); updateErr != nil {
			logger.Error(updateErr, "failed to update status for validation error")
		}
		return ctrl.Result{}, nil
	}

	for _, stage := range stages {
		if err := r.ensureChildCR(ctx, stage, unstructuredDataPipelineCR); err != nil {
			logger.Error(err, "failed to ensure child CR", "stage", stage.Name)
			return r.handleError(ctx, unstructuredDataPipelineCR, err)
		}
	}

	if err := r.deleteOrphanedStageCRs(ctx, stages, unstructuredDataPipelineCR); err != nil {
		logger.Error(err, "failed to delete orphaned stage CRs")
		return ctrl.Result{}, err
	}

	if err := controllerutils.StatusPatch(ctx, r.Client, unstructuredDataPipelineCR, func() {
		unstructuredDataPipelineCR.UpdateStatus("all stage CRs created", nil)
	}); err != nil {
		logger.Error(err, "failed to update pipeline status")
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *UnstructuredDataPipelineReconciler) markStageCreated(ctx context.Context, pipeline *operatorv1alpha1.UnstructuredDataPipeline, stageName string) error {
	key := client.ObjectKeyFromObject(pipeline)
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.UnstructuredDataPipeline{}
		if err := r.Get(ctx, key, latest); err != nil {
			return err
		}
		found := false
		for i := range latest.Status.Stages {
			if latest.Status.Stages[i].Name == stageName {
				latest.Status.Stages[i].Created = true
				found = true
				break
			}
		}
		if !found {
			latest.Status.Stages = append(latest.Status.Stages, operatorv1alpha1.StageCreationStatus{
				Name:    stageName,
				Created: true,
			})
		}
		return r.Status().Update(ctx, latest)
	})
}

func (r *UnstructuredDataPipelineReconciler) deleteOrphanedStageCRs(ctx context.Context, stages []operatorv1alpha1.PipelineStage, pipeline *operatorv1alpha1.UnstructuredDataPipeline) error {
	logger := log.FromContext(ctx)
	listOpts := []client.ListOption{
		client.InNamespace(pipeline.Namespace),
		client.MatchingLabels{PipelineLabel: pipeline.Name},
	}

	// 1. expected stage CRs grouped by type; {SourceCrawler: ["pipeline-crawl"], DocumentProcessor: ["pipeline-convert"] ...}
	expectedStages := map[operatorv1alpha1.StageType][]string{}
	for _, stage := range stages {
		expectedStages[stage.Type] = append(expectedStages[stage.Type], childCRName(pipeline, stage.Name))
	}

	// 2. list all current stage CRs for this pipeline, grouped by type
	currentStages := map[operatorv1alpha1.StageType][]string{}
	for _, stg := range operatorv1alpha1.ListStages() {
		if err := r.List(ctx, stg.ObjectList, listOpts...); err != nil {
			return err
		}
		if err := meta.EachListItem(stg.ObjectList, func(obj runtime.Object) error {
			accessor, err := meta.Accessor(obj)
			if err != nil {
				return err
			}
			currentStages[stg.Type] = append(currentStages[stg.Type], accessor.GetName())
			return nil
		}); err != nil {
			return err
		}
	}

	// 3. for each type, find names not in expected — those are orphans
	// 4. delete orphans
	for stageType, currentNames := range currentStages {
		for _, name := range currentNames {
			if !slices.Contains(expectedStages[stageType], name) {
				logger.Info("deleting orphaned stage CR", "name", name)
				for _, reg := range operatorv1alpha1.ListStages() {
					if reg.Type == stageType {
						reg.Object.SetName(name)
						reg.Object.SetNamespace(pipeline.Namespace)
						if err := r.Delete(ctx, reg.Object); err != nil {
							return fmt.Errorf("failed to delete orphaned CR %s: %w", name, err)
						}
						break
					}
				}
			}
		}
	}

	// sync status.stages to match current spec
	activeStages := map[string]bool{}
	for _, stage := range stages {
		activeStages[stage.Name] = true
	}
	return retry.RetryOnConflict(retry.DefaultRetry, func() error {
		latest := &operatorv1alpha1.UnstructuredDataPipeline{}
		if err := r.Get(ctx, client.ObjectKeyFromObject(pipeline), latest); err != nil {
			return err
		}
		filtered := make([]operatorv1alpha1.StageCreationStatus, 0, len(latest.Status.Stages))
		for _, s := range latest.Status.Stages {
			if activeStages[s.Name] {
				filtered = append(filtered, s)
			}
		}
		latest.Status.Stages = filtered
		return r.Status().Update(ctx, latest)
	})
}

func childCRName(unstructuredDataPipelineCR *operatorv1alpha1.UnstructuredDataPipeline, stageName string) string {
	return unstructuredDataPipelineCR.Name + "-" + stageName
}

func (r *UnstructuredDataPipelineReconciler) ensureChildCR(ctx context.Context, stage operatorv1alpha1.PipelineStage, unstructuredDataPipelineCR *operatorv1alpha1.UnstructuredDataPipeline) error {
	logger := log.FromContext(ctx)
	crName := childCRName(unstructuredDataPipelineCR, stage.Name)
	deps := stage.DependsOn

	switch stage.Type {
	case operatorv1alpha1.StageTypeSourceCrawler:
		cr := &operatorv1alpha1.SourceCrawler{
			ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: unstructuredDataPipelineCR.Namespace},
		}
		result, err := controllerutil.CreateOrUpdate(ctx, r.Client, cr, func() error {
			if cr.Labels == nil {
				cr.Labels = make(map[string]string)
			}
			cr.Labels[PipelineLabel] = unstructuredDataPipelineCR.Name
			cr.Spec = operatorv1alpha1.SourceCrawlerSpec{
				StageName:           stage.Name,
				SecretRef:           unstructuredDataPipelineCR.Spec.SecretRef,
				DependsOn:           deps,
				SourceCrawlerConfig: *stage.SourceCrawlerConfig,
			}
			return controllerutil.SetControllerReference(unstructuredDataPipelineCR, cr, r.Scheme)
		})
		if err != nil {
			return fmt.Errorf("failed to create/update SourceCrawler CR: %w", err)
		}
		logger.Info("SourceCrawler CR created/updated", "name", crName, "result", result)

	case operatorv1alpha1.StageTypeDocumentProcessor:
		cr := &operatorv1alpha1.DocumentProcessor{
			ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: unstructuredDataPipelineCR.Namespace},
		}
		result, err := controllerutil.CreateOrUpdate(ctx, r.Client, cr, func() error {
			if cr.Labels == nil {
				cr.Labels = make(map[string]string)
			}
			cr.Labels[PipelineLabel] = unstructuredDataPipelineCR.Name
			dpConfig := operatorv1alpha1.DocumentProcessorConfig{}
			if stage.DocumentProcessorConfig != nil {
				dpConfig = *stage.DocumentProcessorConfig
			}
			cr.Spec = operatorv1alpha1.DocumentProcessorSpec{
				StageName:               stage.Name,
				DependsOn:               deps,
				DocumentProcessorConfig: dpConfig,
			}
			return controllerutil.SetControllerReference(unstructuredDataPipelineCR, cr, r.Scheme)
		})
		if err != nil {
			return fmt.Errorf("failed to create/update DocumentProcessor CR: %w", err)
		}
		logger.Info("DocumentProcessor CR created/updated", "name", crName, "result", result)

	case operatorv1alpha1.StageTypeChunksGenerator:
		cr := &operatorv1alpha1.ChunksGenerator{
			ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: unstructuredDataPipelineCR.Namespace},
		}
		result, err := controllerutil.CreateOrUpdate(ctx, r.Client, cr, func() error {
			if cr.Labels == nil {
				cr.Labels = make(map[string]string)
			}
			cr.Labels[PipelineLabel] = unstructuredDataPipelineCR.Name
			cgConfig := operatorv1alpha1.ChunksGeneratorConfig{}
			if stage.ChunksGeneratorConfig != nil {
				cgConfig = *stage.ChunksGeneratorConfig
			}
			cr.Spec = operatorv1alpha1.ChunksGeneratorSpec{
				StageName:             stage.Name,
				DependsOn:             deps,
				ChunksGeneratorConfig: cgConfig,
			}
			return controllerutil.SetControllerReference(unstructuredDataPipelineCR, cr, r.Scheme)
		})
		if err != nil {
			return fmt.Errorf("failed to create/update ChunksGenerator CR: %w", err)
		}
		logger.Info("ChunksGenerator CR created/updated", "name", crName, "result", result)

	case operatorv1alpha1.StageTypeVectorEmbeddingsGenerator:
		cr := &operatorv1alpha1.VectorEmbeddingsGenerator{
			ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: unstructuredDataPipelineCR.Namespace},
		}
		result, err := controllerutil.CreateOrUpdate(ctx, r.Client, cr, func() error {
			if cr.Labels == nil {
				cr.Labels = make(map[string]string)
			}
			cr.Labels[PipelineLabel] = unstructuredDataPipelineCR.Name
			vegConfig := operatorv1alpha1.VectorEmbeddingsGeneratorConfig{}
			if stage.VectorEmbeddingsGeneratorConfig != nil {
				vegConfig = *stage.VectorEmbeddingsGeneratorConfig
			}
			cr.Spec = operatorv1alpha1.VectorEmbeddingsGeneratorSpec{
				StageName:                       stage.Name,
				DependsOn:                       deps,
				VectorEmbeddingsGeneratorConfig: vegConfig,
			}
			return controllerutil.SetControllerReference(unstructuredDataPipelineCR, cr, r.Scheme)
		})
		if err != nil {
			return fmt.Errorf("failed to create/update VectorEmbeddingsGenerator CR: %w", err)
		}
		logger.Info("VectorEmbeddingsGenerator CR created/updated", "name", crName, "result", result)

	case operatorv1alpha1.StageTypeDestinationSyncer:
		cr := &operatorv1alpha1.DestinationSyncer{
			ObjectMeta: metav1.ObjectMeta{Name: crName, Namespace: unstructuredDataPipelineCR.Namespace},
		}
		result, err := controllerutil.CreateOrUpdate(ctx, r.Client, cr, func() error {
			if cr.Labels == nil {
				cr.Labels = make(map[string]string)
			}
			cr.Labels[PipelineLabel] = unstructuredDataPipelineCR.Name
			cr.Spec = operatorv1alpha1.DestinationSyncerSpec{
				StageName:               stage.Name,
				SecretRef:               unstructuredDataPipelineCR.Spec.SecretRef,
				DependsOn:               deps,
				DestinationSyncerConfig: *stage.DestinationSyncerConfig,
			}
			return controllerutil.SetControllerReference(unstructuredDataPipelineCR, cr, r.Scheme)
		})
		if err != nil {
			return fmt.Errorf("failed to create/update DestinationSyncer CR: %w", err)
		}
		logger.Info("DestinationSyncer CR created/updated", "name", crName, "result", result)

	default:
		return fmt.Errorf("unknown stage type: %s", stage.Type)
	}

	return r.markStageCreated(ctx, unstructuredDataPipelineCR, stage.Name)
}

func (r *UnstructuredDataPipelineReconciler) handleDeletion(ctx context.Context, pipeline *operatorv1alpha1.UnstructuredDataPipeline) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	if !controllerutil.ContainsFinalizer(pipeline, UDPFinalizer) {
		return ctrl.Result{}, nil
	}

	if err := r.deleteAllStageCRs(ctx, pipeline); err != nil {
		logger.Error(err, "failed to delete child stage CRs")
		return r.handleError(ctx, pipeline, err)
	}

	allGone, err := r.allStageCRsDeleted(ctx, pipeline)
	if err != nil {
		logger.Error(err, "failed to check if child stage CRs are deleted")
		return ctrl.Result{}, err
	}
	if !allGone {
		logger.Info("child stage CRs still terminating, requeueing")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	patch := client.MergeFrom(pipeline.DeepCopy())
	controllerutil.RemoveFinalizer(pipeline, UDPFinalizer)
	if err := r.Patch(ctx, pipeline, patch); err != nil {
		if client.IgnoreNotFound(err) == nil {
			return ctrl.Result{}, nil
		}
		logger.Error(err, "failed to remove finalizer")
		return ctrl.Result{}, err
	}
	logger.Info("finalizer removed, pipeline deletion complete", "pipeline", pipeline.Name)
	return ctrl.Result{}, nil
}

func (r *UnstructuredDataPipelineReconciler) deleteAllStageCRs(ctx context.Context, pipeline *operatorv1alpha1.UnstructuredDataPipeline) error {
	listOpts := []client.ListOption{
		client.InNamespace(pipeline.Namespace),
		client.MatchingLabels{PipelineLabel: pipeline.Name},
	}
	for _, stg := range operatorv1alpha1.ListStages() {
		if err := r.List(ctx, stg.ObjectList, listOpts...); err != nil {
			return err
		}
		if err := meta.EachListItem(stg.ObjectList, func(obj runtime.Object) error {
			o, ok := obj.(client.Object)
			if !ok {
				return fmt.Errorf("unexpected type %T in stage list", obj)
			}
			if o.GetDeletionTimestamp().IsZero() {
				return r.Delete(ctx, o)
			}
			return nil
		}); err != nil {
			return err
		}
	}
	return nil
}

func (r *UnstructuredDataPipelineReconciler) allStageCRsDeleted(ctx context.Context, pipeline *operatorv1alpha1.UnstructuredDataPipeline) (bool, error) {
	listOpts := []client.ListOption{
		client.InNamespace(pipeline.Namespace),
		client.MatchingLabels{PipelineLabel: pipeline.Name},
	}
	for _, stg := range operatorv1alpha1.ListStages() {
		if err := r.List(ctx, stg.ObjectList, listOpts...); err != nil {
			return false, err
		}
		count := 0
		_ = meta.EachListItem(stg.ObjectList, func(_ runtime.Object) error { count++; return nil })
		if count > 0 {
			return false, nil
		}
	}
	return true, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *UnstructuredDataPipelineReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.UnstructuredDataPipeline{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&operatorv1alpha1.SourceCrawler{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&operatorv1alpha1.DocumentProcessor{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&operatorv1alpha1.ChunksGenerator{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&operatorv1alpha1.VectorEmbeddingsGenerator{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Owns(&operatorv1alpha1.DestinationSyncer{}, builder.WithPredicates(predicate.GenerationChangedPredicate{})).
		Complete(r)
}

func (r *UnstructuredDataPipelineReconciler) handleError(ctx context.Context, unstructuredDataPipelineCR *operatorv1alpha1.UnstructuredDataPipeline, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Error(err, "encountered error")
	reconcileErr := err
	if updateErr := controllerutils.StatusPatch(ctx, r.Client, unstructuredDataPipelineCR, func() {
		unstructuredDataPipelineCR.UpdateStatus("", reconcileErr)
	}); updateErr != nil {
		logger.Error(updateErr, "failed to update UnstructuredDataPipeline CR status")
		return ctrl.Result{}, updateErr
	}
	return ctrl.Result{}, reconcileErr
}
