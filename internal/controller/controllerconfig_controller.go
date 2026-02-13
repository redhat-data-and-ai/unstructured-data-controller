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
	"crypto/rsa"
	"fmt"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/keyutil"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/langchain"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
	"github.com/snowflakedb/gosnowflake"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

var (
	ingestionBucket string

	doclingClient   *docling.Client
	langchainClient *langchain.Client
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

		// Get the snowflake client config from the ControllerConfig CR
		snowflakeConfig := config.Spec.SnowflakeConfig
		snowflakeClientConfig, result, err := r.createSnowflakeConfig(ctx, req.Namespace, snowflakeConfig)
		if err != nil {
			return result, err
		}

		if snowflakeClientConfig == nil {
			logger.Info("waiting for snowflake secret to be created, will retry in 10 seconds...")
			return ctrl.Result{
				Requeue:      true,
				RequeueAfter: 10 * time.Second,
			}, nil
		}

		logger.Info("creating snowflake client and validating if snowflake connection is healthy for " + snowflakeConfig.Name)
		_, err = r.createAndValidateSfClient(ctx, *snowflakeClientConfig)
		if err != nil {
			logger.Error(err, "failed to ping snowflake for "+snowflakeConfig.Name+". re-attempting to ping snowflake in 10 seconds")
			config.UpdateStatus(err)
			if err := r.Status().Update(ctx, &config); err != nil {
				return ctrl.Result{}, err
			}
			logger.Info("re-attempting to ping snowflake in 10 seconds")
			return ctrl.Result{
				Requeue:      true,
				RequeueAfter: 10 * time.Second,
			}, nil
		}

		logger.Info("snowflake connection is healthy for " + snowflakeConfig.Name)
		logger.Info("Snowflake client created and connection validated ...")
	}

	ingestionBucket = config.Spec.UnstructuredDataProcessingConfig.IngestionBucket
	dataStorageBucket = config.Spec.UnstructuredDataProcessingConfig.DataStorageBucket
	cacheDirectory = config.Spec.UnstructuredDataProcessingConfig.CacheDirectory

	logger.Info(fmt.Sprintf("Ingestion bucket: %s, Data storage bucket: %s, Cache directory: %s",
		ingestionBucket, dataStorageBucket, cacheDirectory))

	doclingServeURL := config.Spec.UnstructuredDataProcessingConfig.DoclingServeURL

	// initialize the docling client
	doclingClient = docling.NewClientFromURL(
		&docling.ClientConfig{
			URL:                   doclingServeURL,
			MaxConcurrentRequests: int64(config.Spec.UnstructuredDataProcessingConfig.MaxConcurrentDoclingTasks),
		},
	)

	// initialze langchain client
	langchainClient = langchain.NewClient(
		langchain.ClientConfig{
			MaxConcurrentRequests: int64(config.Spec.UnstructuredDataProcessingConfig.MaxConcurrentLangchainTasks),
		},
	)

	cacheDirectory = config.Spec.UnstructuredDataProcessingConfig.CacheDirectory
	dataStorageBucket = config.Spec.UnstructuredDataProcessingConfig.DataStorageBucket

	// update the status of the Config CR to indicate that it is healthy
	config.UpdateStatus(nil)
	if err := r.Status().Update(ctx, &config); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (r *ControllerConfigReconciler) createSnowflakeConfig(ctx context.Context,
	namespace string, cfg operatorv1alpha1.SnowflakeConfig) (*gosnowflake.Config, ctrl.Result, error) {
	logger := log.FromContext(ctx)
	logger.Info("getting snowflake config from CR")
	snowflakeClientConfig := gosnowflake.Config{
		Account:          cfg.Account,
		User:             cfg.User,
		Role:             cfg.Role,
		Region:           cfg.Region,
		Warehouse:        cfg.Warehouse,
		KeepSessionAlive: true,
		Authenticator:    gosnowflake.AuthTypeJwt,
	}

	if cfg.PrivateKeySecret != "" {
		privateKey, err := r.fetchPrivateKeyFromSecret(ctx, cfg.PrivateKeySecret, namespace)
		if err != nil {
			if strings.Contains(err.Error(), "secret not found") {
				logger.Info(fmt.Sprintf("waiting for secret %s to be created, will retry in 10 seconds...", cfg.PrivateKeySecret))
				return nil, ctrl.Result{
					Requeue:      true,
					RequeueAfter: 10 * time.Second,
				}, nil
			}
			// For other errors, log as ERROR
			logger.Error(err, fmt.Sprintf("failed to fetch private key from secret for %s", cfg.PrivateKeySecret))
			return nil, ctrl.Result{}, err
		}
		snowflakeClientConfig.PrivateKey = privateKey
	}
	return &snowflakeClientConfig, ctrl.Result{}, nil
}

func (r *ControllerConfigReconciler) fetchPrivateKeyFromSecret(ctx context.Context, privateKeyName, namespace string) (*rsa.PrivateKey, error) {
	logger := log.FromContext(ctx)
	logger.Info(fmt.Sprintf("fetching private key from secret %s in namespace %s", privateKeyName, namespace))
	secret := &corev1.Secret{}
	if err := r.Get(ctx,
		types.NamespacedName{Name: privateKeyName, Namespace: namespace}, secret); err != nil {
		return nil, fmt.Errorf("secret not found for %s: %w", privateKeyName, err)
	}
	privateKeyData := secret.Data["privateKey"]
	if len(privateKeyData) == 0 {
		return nil, fmt.Errorf("secret:%s has empty 'privateKey'", privateKeyName)
	}
	privateKeyInterface, err := keyutil.ParsePrivateKeyPEM(privateKeyData)
	if err != nil {
		return nil, fmt.Errorf("failed to parse private key from secret %s: %w", privateKeyName, err)
	}
	privateKey, ok := privateKeyInterface.(*rsa.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("private key in secret %s is not an RSA private key", privateKeyName)
	}
	return privateKey, nil
}

func (*ControllerConfigReconciler) createAndValidateSfClient(ctx context.Context, snowflakeClientConfig gosnowflake.Config) (*snowflake.Client, error) {
	logger := log.FromContext(ctx)
	logger.Info("creating snowflake client")
	sfClient, err := snowflake.NewClient(ctx, &snowflake.ClientConfig{
		Config: snowflakeClientConfig,
	})
	if err != nil {
		return nil, err
	}
	if err := sfClient.Ping(ctx); err != nil {
		return nil, err
	}
	return sfClient, nil
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
