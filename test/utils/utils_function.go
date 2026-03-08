package utils

import (
	"os"

	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// DefaultE2ENamespace is the namespace used by e2e tests (must match test/e2e/main_test.go testNamespace).
const DefaultE2ENamespace = "unstructured-controller-namespace"

func GetControllerConfigResource() *v1alpha1.ControllerConfig {
	account := os.Getenv("SNOWFLAKE_ACCOUNT")
	if account == "" {
		account = "account-identifier"
	}

	user := os.Getenv("SNOWFLAKE_USER")
	if user == "" {
		user = "username"
	}

	role := os.Getenv("SNOWFLAKE_ROLE")
	if role == "" {
		role = "TESTING_ROLE"
	}

	warehouse := os.Getenv("SNOWFLAKE_WAREHOUSE")
	if warehouse == "" {
		warehouse = "DEFAULT"
	}

	return &v1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "controllerconfig",
			Namespace: DefaultE2ENamespace,
		},
		Spec: v1alpha1.ControllerConfigSpec{
			SnowflakeConfig: v1alpha1.SnowflakeConfig{
				Name:             "e2e",
				Account:          account,
				User:             user,
				Role:             role,
				Region:           "us-west-2",
				Warehouse:        warehouse,
				PrivateKeySecret: "private-key",
			},
			UnstructuredDataProcessingConfig: v1alpha1.UnstructuredDataProcessingConfigSpec{
				DoclingServeURL:             "http://docling-serve:5001",
				IngestionBucket:             "unstructured-bucket",
				DataStorageBucket:           "data-storage-bucket",
				CacheDirectory:              "/data/cache/",
				MaxConcurrentDoclingTasks:   5,
				MaxConcurrentLangchainTasks: 10,
			},
			AWSSecret: "aws-secret",
		},
	}
}
