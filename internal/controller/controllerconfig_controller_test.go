package controller

import (
	"context"
	"testing"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/stretchr/testify/assert"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

// covering the following scenerios
// 1. calling reconcile with no-secrets (should return an error, should set the global variables like dataingestionBucket, dataStorageBucket and cacheDirectory)

func TestControllerConfigReconcileWithoutSecret(t *testing.T) {
	schema := runtime.NewScheme()
	assert.NoError(t, operatorv1alpha1.AddToScheme(schema))
	assert.NoError(t, corev1.AddToScheme(schema))

	controllerConfig := &operatorv1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-config",
			Namespace:  "default",
			Generation: 1,
		},
		Spec: operatorv1alpha1.ControllerConfigSpec{
			UnstructuredDataProcessingConfig: operatorv1alpha1.UnstructuredDataProcessingConfigSpec{
				IngestionBucket:             "data-ingestion-bucket",
				DataStorageBucket:           "data-storage-bucket",
				CacheDirectory:              "tmp/cache/",
				DoclingServeURL:             "http://docling:8080",
				MaxConcurrentDoclingTasks:   5,
				MaxConcurrentLangchainTasks: 3,
			},
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(schema).WithObjects(controllerConfig).WithStatusSubresource(controllerConfig).Build()

	reconciler := &ControllerConfigReconciler{
		Client: fakeClient,
		Scheme: schema,
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-config",
			Namespace: "default",
		},
	}

	ingestionBucket = ""
	dataStorageBucket = ""
	cacheDirectory = ""

	result, err := reconciler.Reconcile(context.Background(), req)
	assert.Error(t, err, "Expected error because there is no unstructured secret found")
	assert.Equal(t, "data-ingestion-bucket", ingestionBucket, "Ingestion bucket should be set")
	assert.Equal(t, "data-storage-bucket", dataStorageBucket, "Storage bucket should be set")
	assert.Equal(t, "tmp/cache/", cacheDirectory, "Cache directory should be set")

	t.Logf("Result: %+v", result)
}

func TestControllerConfigReconcileWithSecrets(t *testing.T) {
	schema := runtime.NewScheme()
	assert.NoError(t, operatorv1alpha1.AddToScheme(schema))
	assert.NoError(t, corev1.AddToScheme(schema))

	controllerConfig := &operatorv1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-config",
			Namespace:  "default",
			Generation: 1,
		},
		Spec: operatorv1alpha1.ControllerConfigSpec{
			UnstructuredSecret: "unstructured-secret",
			UnstructuredDataProcessingConfig: operatorv1alpha1.UnstructuredDataProcessingConfigSpec{
				IngestionBucket:             "data-ingestion-bucket",
				DataStorageBucket:           "data-storage-bucket",
				CacheDirectory:              "tmp/cache/",
				DoclingServeURL:             "http://docling:8080",
				MaxConcurrentDoclingTasks:   5,
				MaxConcurrentLangchainTasks: 3,
			},
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unstructured-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"SOURCE_AWS_REGION":                 []byte("us-east-1"),
			"SOURCE_AWS_ACCESS_KEY_ID":          []byte("fake"),
			"SOURCE_AWS_SECRET_ACCESS_KEY":      []byte("fake"),
			"SOURCE_AWS_ENDPOINT":               []byte(""),
			"DESTINATION_AWS_REGION":            []byte("us-east-1"),
			"DESTINATION_AWS_ACCESS_KEY_ID":     []byte("fake"),
			"DESTINATION_AWS_SECRET_ACCESS_KEY": []byte("fake"),
			"DESTINATION_AWS_ENDPOINT":          []byte(""),
			"FILE_STORE_AWS_REGION":             []byte("us-east-1"),
			"FILE_STORE_AWS_ACCESS_KEY_ID":      []byte("fake"),
			"FILE_STORE_AWS_SECRET_ACCESS_KEY":  []byte("fake"),
			"FILE_STORE_AWS_ENDPOINT":           []byte(""),
			"SNOWFLAKE_PRIVATE_KEY":             []byte("fake-base64-encoded-key"),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(schema).WithObjects(controllerConfig, secret).WithStatusSubresource(controllerConfig).Build()

	reconciler := &ControllerConfigReconciler{
		Client: fakeClient,
		Scheme: schema,
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-config",
			Namespace: "default",
		},
	}

	// fetch the secrets to verify their values
	fetchedSecret := &corev1.Secret{}
	err := fakeClient.Get(context.Background(), types.NamespacedName{Name: "unstructured-secret", Namespace: "default"}, fetchedSecret)
	assert.NoError(t, err, "Expected no error")
	assert.Equal(t, "us-east-1", string(fetchedSecret.Data["SOURCE_AWS_REGION"]), "Source AWS region should be set")
	assert.Equal(t, "fake", string(fetchedSecret.Data["SOURCE_AWS_ACCESS_KEY_ID"]), "Source AWS access key ID should be set")
	assert.Equal(t, "fake", string(fetchedSecret.Data["SOURCE_AWS_SECRET_ACCESS_KEY"]), "Source AWS secret access key should be set")
	assert.Equal(t, "", string(fetchedSecret.Data["SOURCE_AWS_ENDPOINT"]), "Source AWS endpoint should be set")
	assert.Equal(t, "us-east-1", string(fetchedSecret.Data["DESTINATION_AWS_REGION"]), "Destination AWS region should be set")
	assert.Equal(t, "fake", string(fetchedSecret.Data["DESTINATION_AWS_ACCESS_KEY_ID"]), "Destination AWS access key ID should be set")
	assert.Equal(t, "fake", string(fetchedSecret.Data["DESTINATION_AWS_SECRET_ACCESS_KEY"]), "Destination AWS secret access key should be set")
	assert.Equal(t, "", string(fetchedSecret.Data["DESTINATION_AWS_ENDPOINT"]), "Destination AWS endpoint should be set")
	assert.Equal(t, "us-east-1", string(fetchedSecret.Data["FILE_STORE_AWS_REGION"]), "File store AWS region should be set")
	assert.Equal(t, "fake", string(fetchedSecret.Data["FILE_STORE_AWS_ACCESS_KEY_ID"]), "File store AWS access key ID should be set")
	assert.Equal(t, "fake", string(fetchedSecret.Data["FILE_STORE_AWS_SECRET_ACCESS_KEY"]), "File store AWS secret access key should be set")
	assert.Equal(t, "", string(fetchedSecret.Data["FILE_STORE_AWS_ENDPOINT"]), "File store AWS endpoint should be set")
	assert.Equal(t, "fake-base64-encoded-key", string(fetchedSecret.Data["SNOWFLAKE_PRIVATE_KEY"]), "Snowflake private key should be set")

	result, err := reconciler.Reconcile(context.Background(), req)
	assert.NoError(t, err, "Expected no error")
	t.Logf("Result: %+v", result)

	updated := getControllerConfig(t, fakeClient, req.NamespacedName)
	assert.True(t, updated.IsHealthy())
	assert.Equal(t, int64(1), updated.Status.LastAppliedGeneration)
	cond := configReadyCondition(updated)
	assert.NotNil(t, cond)
	assert.Equal(t, metav1.ConditionTrue, cond.Status)
	assert.Equal(t, "SuccessfullyReconciled", cond.Reason)
	assert.Equal(t, "Config is healthy", cond.Message)
}

func TestControllerConfigReconcileWithInvalidSSnowflakeRSAKey(t *testing.T) {
	schema := runtime.NewScheme()
	assert.NoError(t, operatorv1alpha1.AddToScheme(schema))
	assert.NoError(t, corev1.AddToScheme(schema))

	controllerConfig := &operatorv1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:       "test-config",
			Namespace:  "default",
			Generation: 1,
		},
		Spec: operatorv1alpha1.ControllerConfigSpec{
			UnstructuredSecret: "unstructured-secret",
			UnstructuredDataProcessingConfig: operatorv1alpha1.UnstructuredDataProcessingConfigSpec{
				IngestionBucket:             "data-ingestion-bucket",
				DataStorageBucket:           "data-storage-bucket",
				CacheDirectory:              "tmp/cache/",
				DoclingServeURL:             "http://docling:8080",
				MaxConcurrentDoclingTasks:   5,
				MaxConcurrentLangchainTasks: 3,
			},
			SnowflakeConfig: &operatorv1alpha1.SnowflakeConfig{
				Account:   "test-account",
				User:      "test-user",
				Role:      "test-role",
				Region:    "us-east-1",
				Warehouse: "test-warehouse",
			},
		},
	}

	secret := &corev1.Secret{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "unstructured-secret",
			Namespace: "default",
		},
		Data: map[string][]byte{
			"SOURCE_AWS_REGION":                 []byte("us-east-1"),
			"SOURCE_AWS_ACCESS_KEY_ID":          []byte("fake"),
			"SOURCE_AWS_SECRET_ACCESS_KEY":      []byte("fake"),
			"SOURCE_AWS_ENDPOINT":               []byte(""),
			"DESTINATION_AWS_REGION":            []byte("us-east-1"),
			"DESTINATION_AWS_ACCESS_KEY_ID":     []byte("fake"),
			"DESTINATION_AWS_SECRET_ACCESS_KEY": []byte("fake"),
			"DESTINATION_AWS_ENDPOINT":          []byte(""),
			"FILE_STORE_AWS_REGION":             []byte("us-east-1"),
			"FILE_STORE_AWS_ACCESS_KEY_ID":      []byte("fake"),
			"FILE_STORE_AWS_SECRET_ACCESS_KEY":  []byte("fake"),
			"FILE_STORE_AWS_ENDPOINT":           []byte(""),
			"SNOWFLAKE_PRIVATE_KEY":             []byte("fake-base64-encoded-key"),
		},
	}

	fakeClient := fake.NewClientBuilder().WithScheme(schema).WithObjects(controllerConfig, secret).WithStatusSubresource(controllerConfig).Build()

	reconciler := &ControllerConfigReconciler{
		Client: fakeClient,
		Scheme: schema,
	}

	req := ctrl.Request{
		NamespacedName: types.NamespacedName{
			Name:      "test-config",
			Namespace: "default",
		},
	}

	result, err := reconciler.Reconcile(context.Background(), req)
	assert.Error(t, err)
	assert.ErrorContains(t, err, "failed to decode snowflake private key from secret unstructured-secret")
	t.Logf("Result: %+v", result)

	updated := getControllerConfig(t, fakeClient, req.NamespacedName)
	assert.False(t, updated.IsHealthy())
	assert.Equal(t, int64(0), updated.Status.LastAppliedGeneration)
	assert.Nil(t, configReadyCondition(updated), "ConfigReady condition should not be written on early failure")
}

// helper functions
func getControllerConfig(t *testing.T, c client.Client, nn types.NamespacedName) *operatorv1alpha1.ControllerConfig {
	t.Helper()
	cfg := &operatorv1alpha1.ControllerConfig{}
	assert.NoError(t, c.Get(context.Background(), nn, cfg))
	return cfg
}
func configReadyCondition(cfg *operatorv1alpha1.ControllerConfig) *metav1.Condition {
	for i := range cfg.Status.Conditions {
		if cfg.Status.Conditions[i].Type == operatorv1alpha1.ConfigCondition {
			return &cfg.Status.Conditions[i]
		}
	}
	return nil
}
