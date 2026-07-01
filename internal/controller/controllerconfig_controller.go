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

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	pkgcache "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/cache"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/cache/inmemory"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/ldap"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/langchain"
)

var (
	doclingClient                          *docling.Client
	langchainClient                        *langchain.Client
	embeddingEndpoint                      string
	embeddingAPIKey                        string
	UnstructuredDataPipelineResyncInterval *int
	LDAPClient                             ldap.Client
	CacheClient                            pkgcache.Cache
	GoogleDriveControllerCfg               *operatorv1alpha1.GoogleDriveControllerConfig
)

// ControllerConfigReconciler reconciles a ControllerConfig object
type ControllerConfigReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=controllerconfigs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=controllerconfigs/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=operator.dataverse.redhat.com,namespace=unstructured-controller-namespace,resources=controllerconfigs/finalizers,verbs=update

func (r *ControllerConfigReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info(fmt.Sprintf("Reconciling controller config %s", req.NamespacedName))

	config := operatorv1alpha1.ControllerConfig{}
	if err := r.Get(ctx, req.NamespacedName, &config); err != nil {
		return ctrl.Result{}, err
	}

	dataStorageBucket = config.Spec.DataStorageBucket
	cacheDirectory = config.Spec.CacheDirectory

	// fetch operator-level secret for filestore + docling credentials
	secret := &corev1.Secret{}
	if config.Spec.SecretRef != "" {
		if err := r.Get(ctx,
			types.NamespacedName{Name: config.Spec.SecretRef, Namespace: req.Namespace}, secret); err != nil {
			logger.Error(err, fmt.Sprintf("error fetching secret %s, retrying in 10 seconds", config.Spec.SecretRef))
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
	}

	// initialize docling client
	doclingConfig := &docling.ClientConfig{
		URL:                   config.Spec.DoclingServeURL,
		MaxConcurrentRequests: int64(config.Spec.MaxConcurrentDoclingTasks),
	}
	if doclingKey := string(secret.Data["DOCLING_USER_KEY"]); doclingKey != "" {
		doclingConfig.Key = doclingKey
	}
	doclingClient = docling.NewClientFromURL(doclingConfig)

	// initialize langchain client
	langchainClient = langchain.NewClient(langchain.ClientConfig{
		MaxConcurrentRequests: int64(config.Spec.MaxConcurrentLangchainTasks),
	})

	logger.Info(fmt.Sprintf("Data storage bucket: %s, Cache directory: %s", dataStorageBucket, cacheDirectory))

	// initialize filestore S3 client
	fileStoreAwsConfig := awsclienthandler.AWSConfig{
		Region:          string(secret.Data["FILE_STORE_AWS_REGION"]),
		AccessKeyID:     string(secret.Data["FILE_STORE_AWS_ACCESS_KEY_ID"]),
		SecretAccessKey: string(secret.Data["FILE_STORE_AWS_SECRET_ACCESS_KEY"]),
		SessionToken:    string(secret.Data["FILE_STORE_AWS_SESSION_TOKEN"]),
		Endpoint:        string(secret.Data["FILE_STORE_AWS_ENDPOINT"]),
	}
	if err := awsclienthandler.NewFileStoreS3ClientFromConfig(ctx, &fileStoreAwsConfig); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("File store S3 client created ...")

	// embedding model credentials
	embeddingEndpoint = string(secret.Data["EMBEDDING_ENDPOINT"])
	embeddingAPIKey = string(secret.Data["EMBEDDING_API_KEY"])

	// initialize LDAP client and cache if configured
	if config.Spec.LDAPConfig.Server != "" {
		ldapCfg := config.Spec.LDAPConfig
		lc, err := ldap.InitLDAP(ldap.Config{
			Server:           ldapCfg.Server,
			GroupDN:          ldapCfg.GroupDN,
			UserDN:           ldapCfg.UserDN,
			BaseUserDN:       ldapCfg.BaseUserDN,
			UserSearchFilter: ldapCfg.UserSearchFilter,
			EmailAttribute:   ldapCfg.EmailAttribute,
			Attributes:       ldapCfg.Attributes,
		})
		if err != nil {
			logger.Error(err, "failed to initialize LDAP client, will retry")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		LDAPClient = lc

		cc, err := pkgcache.New(&pkgcache.Config{
			Driver: pkgcache.DriverMemory,
			InMemory: &inmemory.Config{
				DefaultExpiration: -1,
				CleanupInterval:   -1,
			},
		})
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to create cache client: %w", err)
		}
		CacheClient = cc
		logger.Info("LDAP client and cache initialized")
	}

	GoogleDriveControllerCfg = config.Spec.GoogleDriveConfig

	if config.Spec.UnstructuredDataPipelineResyncInterval != nil {
		UnstructuredDataPipelineResyncInterval = config.Spec.UnstructuredDataPipelineResyncInterval
		logger.Info("setting unstructured data pipeline resync interval", "minutes", *UnstructuredDataPipelineResyncInterval)
	}

	if config.Status.LastAppliedGeneration == config.Generation && config.IsHealthy() {
		logger.Info("config already reconciled for current generation, skipping status update")
		return ctrl.Result{}, nil
	}

	config.UpdateStatus(nil)
	if err := r.Status().Update(ctx, &config); err != nil {
		return ctrl.Result{}, err
	}

	logger.Info("successfully updated controllerConfig CR status", "status", config.Status)
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
