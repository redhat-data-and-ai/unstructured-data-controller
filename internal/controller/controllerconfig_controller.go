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
	"strings"
	"sync/atomic"
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
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/google"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/ldap"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/langchain"
)

type Model string

type ModelCredentials struct {
	Endpoint string
	APIKey   string
}

var modelMap = map[Model]ModelCredentials{
	Model("nomic-ai/nomic-embed-text-v1.5"): {
		Endpoint: "NOMIC_ENDPOINT",
		APIKey:   "NOMIC_API_KEY",
	},
	Model("gemini-embedding-2"): {
		Endpoint: "GEMINI_ENDPOINT",
		APIKey:   "GEMINI_API_KEY",
	},
}

var (
	doclingClient             *docling.Client
	langchainClient           *langchain.Client
	embeddingModelCredentials = map[Model]ModelCredentials{}
	vlmAPIKey                 string
	vlmAPIURL                 string
	vlmModelID                string
	// Published as one whole value rather than field by field: the document
	// processor reads it from another goroutine, and a piecemeal update could
	// hand it a credential from one revision paired with an endpoint from
	// another.
	vlmPipelineCfg                         atomic.Pointer[vlmPipelineSettings]
	UnstructuredDataPipelineResyncInterval *int
	LDAPClient                             ldap.Client
	CacheClient                            pkgcache.Cache
	GoogleDriveControllerCfg               *operatorv1alpha1.GoogleDriveControllerConfig
	GlobalGoogleClient                     google.GoogleClient
	controllerConfigSecretRef              string
	controllerConfigNamespace              string
)

// vlmPipelineAPIKeySecretKey is the Secret entry holding the credential for
// the model the vlm pipeline parses with. It must match
// config/samples/unstructured-secret.yaml: a key the controller never finds
// reads as "this endpoint needs no credential" and the request goes out
// unauthenticated.
const vlmPipelineAPIKeySecretKey = "VLM_PIPELINE_API_KEY"

// vlmPipelineSettings is where the vlm pipeline's model is reached. Which
// model it is, and what to ask it for, are spec fields on the
// DocumentProcessor instead, because those change what the parse returns and
// so have to reach the conversion cache key.
//
// apiKey has no CRD field at all: DoclingConfig is copied verbatim into
// ConvertedFileMetadata and shipped downstream, so a credential on a CR would
// be persisted into every converted artifact.
type vlmPipelineSettings struct {
	apiKey string
	apiURL string
}

// configured reports whether the model can be reached. Only the endpoint is
// required: a missing credential fails loudly at the gateway, whereas a
// missing endpoint has docling run its own bundled model and answer 200.
func (s *vlmPipelineSettings) configured() bool {
	return s != nil && s.apiURL != ""
}

// stringValue reads an optional API field, which is nil when unset.
func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return strings.TrimSpace(*value)
}

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
	dataStorageDirectory = config.Spec.DataStorageDirectory
	controllerConfigSecretRef = config.Spec.SecretRef
	controllerConfigNamespace = req.Namespace

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
	httpTimeout := 60 * time.Second
	if config.Spec.DoclingHTTPTimeout != nil {
		httpTimeout = time.Duration(*config.Spec.DoclingHTTPTimeout) * time.Second
	}
	doclingConfig := &docling.ClientConfig{
		URL:                   config.Spec.DoclingServeURL,
		MaxConcurrentRequests: int64(config.Spec.MaxConcurrentDoclingTasks),
		HTTPTimeout:           httpTimeout,
	}
	if doclingKey := string(secret.Data["DOCLING_USER_KEY"]); doclingKey != "" {
		doclingConfig.Key = doclingKey
	}
	doclingClient = docling.NewClientFromURL(doclingConfig)

	// initialize langchain client
	langchainClient = langchain.NewClient(langchain.ClientConfig{
		MaxConcurrentRequests: int64(config.Spec.MaxConcurrentLangchainTasks),
	})

	logger.Info(fmt.Sprintf("Data storage bucket: %s, Data storage directory: %s", dataStorageBucket, dataStorageDirectory))

	// initialize filestore S3 client
	fileStoreAwsConfig := awsclienthandler.AWSConfig{
		Region:          config.Spec.DataStorageBucketRegion,
		AccessKeyID:     string(secret.Data["FILE_STORE_AWS_ACCESS_KEY_ID"]),
		SecretAccessKey: string(secret.Data["FILE_STORE_AWS_SECRET_ACCESS_KEY"]),
		SessionToken:    string(secret.Data["FILE_STORE_AWS_SESSION_TOKEN"]),
		Endpoint:        config.Spec.DataStorageBucketEndpoint,
	}
	if err := awsclienthandler.NewFileStoreS3ClientFromConfig(ctx, &fileStoreAwsConfig); err != nil {
		return ctrl.Result{}, err
	}
	logger.Info("File store S3 client created ...")

	// embedding model credentials — endpoints from spec, API keys from secret
	endpointFromSpec := map[Model]string{
		Model("nomic-ai/nomic-embed-text-v1.5"): config.Spec.NomicEndpoint,
		Model("gemini-embedding-2"):             config.Spec.GeminiEndpoint,
	}
	for model, secretKeys := range modelMap {
		embeddingModelCredentials[model] = ModelCredentials{
			Endpoint: endpointFromSpec[model],
			APIKey:   string(secret.Data[secretKeys.APIKey]),
		}
	}

	// VLM credentials for picture description
	vlmAPIKey = strings.TrimSpace(string(secret.Data["VLM_API_KEY"]))
	vlmAPIURL = strings.TrimSpace(config.Spec.VLMAPIURL)
	vlmModelID = strings.TrimSpace(config.Spec.VLMModelID)

	// Where the vlm pipeline's model is served. Stored as one complete value so
	// no reader can observe a half-applied update.
	vlmPipelineCfg.Store(&vlmPipelineSettings{
		apiKey: strings.TrimSpace(string(secret.Data[vlmPipelineAPIKeySecretKey])),
		apiURL: stringValue(config.Spec.VLMPipelineAPIURL),
	})

	// initialize LDAP client and cache if configured
	if config.Spec.LDAPConfig != nil && config.Spec.LDAPConfig.Server != "" {
		ldapCfg := *config.Spec.LDAPConfig

		// fetch LDAP password from secret if provided
		ldapPassword := string(secret.Data["LDAP_PASSWORD"])

		lc, err := ldap.InitLDAP(ldap.Config{
			Server:           ldapCfg.Server,
			GroupDN:          ldapCfg.GroupDN,
			UserDN:           ldapCfg.UserDN,
			BaseUserDN:       ldapCfg.BaseUserDN,
			UserSearchFilter: ldapCfg.UserSearchFilter,
			EmailAttribute:   ldapCfg.EmailAttribute,
			Attributes:       ldapCfg.Attributes,
			BindUserName:     ldapCfg.BindUserName,
			BindPassword:     ldapPassword,
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

	if credJSON, ok := secret.Data["GROUPS_READER_GOOGLE_SERVICE_ACCOUNT_JSON"]; ok {
		globalClient, err := google.NewClientFromJSON(ctx, credJSON)
		if err != nil {
			logger.Error(err, "failed to create global Google client for group resolution")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}
		GlobalGoogleClient = globalClient
		logger.Info("Global Google client initialized for group membership resolution")
	}

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
