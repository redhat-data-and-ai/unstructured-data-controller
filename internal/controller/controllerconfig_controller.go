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
	"encoding/base64"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/util/keyutil"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/predicate"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/internal/controller/controllerutils"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/langchain"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
	"github.com/snowflakedb/gosnowflake"
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

	// Always set globals from config so they are populated on every Reconcile (including after process restart).
	ingestionBucket = config.Spec.UnstructuredDataProcessingConfig.IngestionBucket
	dataStorageBucket = config.Spec.UnstructuredDataProcessingConfig.DataStorageBucket
	cacheDirectory = config.Spec.UnstructuredDataProcessingConfig.CacheDirectory

	unstructuredSecret := &corev1.Secret{}
	if config.Spec.UnstructuredSecret != "" {
		if err := r.Get(ctx,
			types.NamespacedName{Name: config.Spec.UnstructuredSecret, Namespace: req.Namespace}, unstructuredSecret); err != nil {
			logger.Error(err, fmt.Sprintf("error fetching AWS secret %s, retrying in 10 seconds ", config.Spec.UnstructuredSecret))
			return ctrl.Result{
				Requeue:      true,
				RequeueAfter: 10 * time.Second,
			}, nil
		}
	}

	doclingServeURL := config.Spec.UnstructuredDataProcessingConfig.DoclingServeURL

	doclingConfig := &docling.ClientConfig{
		URL:                   doclingServeURL,
		MaxConcurrentRequests: int64(config.Spec.UnstructuredDataProcessingConfig.MaxConcurrentDoclingTasks),
	}

	doclingKey := string(unstructuredSecret.Data["DOCLING_USER_KEY"])
	if doclingKey != "" {
		doclingConfig.Key = doclingKey
	}

	// initialize the docling client
	doclingClient = docling.NewClientFromURL(doclingConfig)

	langchainClient = langchain.NewClient(
		langchain.ClientConfig{
			MaxConcurrentRequests: int64(config.Spec.UnstructuredDataProcessingConfig.MaxConcurrentLangchainTasks),
		},
	)

	logger.Info(fmt.Sprintf("Ingestion bucket: %s, Data storage bucket: %s, Cache directory: %s",
		ingestionBucket, dataStorageBucket, cacheDirectory))

	awsEndpoint := string(unstructuredSecret.Data["AWS_ENDPOINT"])
	if awsEndpoint != "" {
		// generate AWS clients now
		awsConfig := awsclienthandler.AWSConfig{
			Region:          string(unstructuredSecret.Data["AWS_REGION"]),
			AccessKeyID:     string(unstructuredSecret.Data["AWS_ACCESS_KEY_ID"]),
			SecretAccessKey: string(unstructuredSecret.Data["AWS_SECRET_ACCESS_KEY"]),
			SessionToken:    string(unstructuredSecret.Data["AWS_SESSION_TOKEN"]),
			Endpoint:        awsEndpoint,
		}
		if _, err := awsclienthandler.NewSQSClientFromConfig(ctx, &awsConfig); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("SQS client created ...")
		if _, err := awsclienthandler.NewS3ClientFromConfig(ctx, &awsConfig); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("S3 client created ...")
		if _, err := awsclienthandler.NewPresignClient(ctx); err != nil {
			return ctrl.Result{}, err
		}
		logger.Info("Presign client created ...")
	}

	snowflakePrivateKey := unstructuredSecret.Data["SNOWFLAKE_PRIVATE_KEY"]
	snowflakeConfig := config.Spec.SnowflakeConfig
	if len(snowflakePrivateKey) != 0 && snowflakeConfig != nil {
		encodedString, err := base64.StdEncoding.DecodeString(string(snowflakePrivateKey))
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to decode snowflake private key from secret %s: %w", unstructuredSecret.Name, err)
		}
		privateKeyInterface, err := keyutil.ParsePrivateKeyPEM(encodedString)
		if err != nil {
			return ctrl.Result{}, fmt.Errorf("failed to parse snowflake private key from secret %s: %w", unstructuredSecret.Name, err)
		}
		privateKey, ok := privateKeyInterface.(*rsa.PrivateKey)
		if !ok {
			return ctrl.Result{}, fmt.Errorf("snowflake private key in secret %s is not an RSA private key", unstructuredSecret.Name)
		}

		// Get the snowflake client config from the ControllerConfig CR
		snowflakeConfig := config.Spec.SnowflakeConfig
		snowflakeClientConfig := gosnowflake.Config{
			Account:          snowflakeConfig.Account,
			User:             snowflakeConfig.User,
			Role:             snowflakeConfig.Role,
			Region:           snowflakeConfig.Region,
			Warehouse:        snowflakeConfig.Warehouse,
			KeepSessionAlive: true,
			Authenticator:    gosnowflake.AuthTypeJwt,
			PrivateKey:       privateKey,
		}

		logger.Info("creating snowflake client and validating if snowflake connection is healthy for " + snowflakeConfig.Name)
		err = r.createAndValidateSfClient(ctx, snowflakeClientConfig)
		if err != nil {
			logger.Error(err, "failed to ping snowflake for "+snowflakeConfig.Name+". re-attempting to ping snowflake in 10 seconds")
			config.UpdateStatus(err)
			if updateErr := r.Status().Update(ctx, &config); updateErr != nil {
				return ctrl.Result{}, updateErr
			}
			return ctrl.Result{}, err
		}
	}

	// Skip only status update if we've already processed this generation successfully
	if config.Status.LastAppliedGeneration == config.Generation && config.IsHealthy() {
		logger.Info("config already reconciled for current generation, skipping status update")
		return ctrl.Result{}, nil
	}

	// update the status of the Config CR to indicate that it is healthy
	config.UpdateStatus(nil)
	if err := r.Status().Update(ctx, &config); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{}, nil
}

func (*ControllerConfigReconciler) createAndValidateSfClient(ctx context.Context, snowflakeClientConfig gosnowflake.Config) error {
	logger := log.FromContext(ctx)
	logger.Info("creating snowflake client")
	sfClient, err := snowflake.NewClient(ctx, &snowflake.ClientConfig{
		Config: snowflakeClientConfig,
	})
	if err != nil {
		return err
	}
	return sfClient.Ping(ctx)
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
