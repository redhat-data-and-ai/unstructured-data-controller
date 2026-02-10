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
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

var (
	ingestionBucket string
)

// ControllerConfigReconciler reconciles a ControllerConfig object
type ControllerConfigReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=controllerconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=controllerconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=controllerconfigs/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the ControllerConfig object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.21.0/pkg/reconcile
func (r *ControllerConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info(fmt.Sprintf("Reconciling controller config %s", req.NamespacedName))

	configList := operatorv1alpha1.ControllerConfigList{}
	if err := r.List(ctx, &configList); err != nil {
		return ctrl.Result{}, err
	}

	if len(configList.Items) == 0 {
		logger.Info("no ControllerConfig found, nothing to reconcile")
		return ctrl.Result{}, nil
	}

	config := configList.Items[0]

	// Skip reconciliation if we've already processed this generation successfully
	if config.Status.LastAppliedGeneration == config.Generation && config.IsHealthy() {
		logger.Info("config already reconciled for current generation, skipping")
		return ctrl.Result{}, nil
	}

	if config.Spec.AWSSecret != "" {
		// generate AWS clients now
		awsSecret := &corev1.Secret{}
		if err := r.Get(ctx,
			types.NamespacedName{Name: config.Spec.AWSSecret, Namespace: req.Namespace}, awsSecret); err != nil {
			logger.Error(err, fmt.Sprintf("error fetching AWS secret %s, retrying in 10 seconds ", config.Spec.AWSSecret))
			return ctrl.Result{
				Requeue:      true,
				RequeueAfter: 10 * time.Second,
			}, nil
		}
		awsConfig := awsclienthandler.AWSConfig{
			Region:          string(awsSecret.Data["AWS_REGION"]),
			AccessKeyID:     string(awsSecret.Data["AWS_ACCESS_KEY_ID"]),
			SecretAccessKey: string(awsSecret.Data["AWS_SECRET_ACCESS_KEY"]),
			SessionToken:    string(awsSecret.Data["AWS_SESSION_TOKEN"]),
			Endpoint:        string(awsSecret.Data["AWS_ENDPOINT"]),
		}
		// create SQS client
		_, err := awsclienthandler.NewSQSClientFromConfig(ctx, &awsConfig)
		if err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("SQS client created ...")

		// create S3 client
		_, err = awsclienthandler.NewS3ClientFromConfig(ctx, &awsConfig)
		if err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("S3 client created ...")

		// create presign client
		_, err = awsclienthandler.NewPresignClient(ctx)
		if err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("Presign client created ...")

		// TODO: add in the controller where used
		// // create IAM client
		// _, err = awsclienthandler.NewIAMClientFromConfig(ctx, &awsConfig)
		// if err != nil {
		// 	return ctrl.Result{}, err
		// }
		// logger.Info("IAM client created ...")

		// // create KMS client
		// _, err = awsclienthandler.NewKMSClientFromConfig(ctx, &awsConfig)
		// if err != nil {
		// 	return ctrl.Result{}, err
		// }
		// logger.Info("KMS client created ...")
	}

	ingestionBucket = config.Spec.UnstructuredDataProcessingConfig.IngestionBucket
	dataStorageBucket = config.Spec.UnstructuredDataProcessingConfig.DataStorageBucket
	cacheDirectory = config.Spec.UnstructuredDataProcessingConfig.CacheDirectory

	logger.Info(fmt.Sprintf("Ingestion bucket: %s, Data storage bucket: %s, Cache directory: %s",
		ingestionBucket, dataStorageBucket, cacheDirectory))

	// update the status of the Config CR to indicate that it is healthy
	config.UpdateStatus(nil)
	if err := r.Status().Update(ctx, &config); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *ControllerConfigReconciler) SetupWithManager(mgr ctrl.Manager) error {
	labelPredicate := controllerutils.ForceReconcilePredicate()
	return ctrl.NewControllerManagedBy(mgr).
		For(&operatorv1alpha1.ControllerConfig{}).
		WithEventFilter(predicate.Or(predicate.GenerationChangedPredicate{}, labelPredicate)).
		Named("controllerconfig").
		Complete(r)
}
