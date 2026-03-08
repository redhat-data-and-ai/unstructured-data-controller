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
			SnowflakeConfig: &v1alpha1.SnowflakeConfig{
				Name:      "e2e",
				Account:   account,
				User:      user,
				Role:      role,
				Region:    "us-west-2",
				Warehouse: warehouse,
			},
			UnstructuredDataProcessingConfig: v1alpha1.UnstructuredDataProcessingConfigSpec{
				DoclingServeURL:             "http://docling-serve:5001",
				IngestionBucket:             "unstructured-bucket",
				DataStorageBucket:           "data-storage-bucket",
				CacheDirectory:              "/data/cache/",
				MaxConcurrentDoclingTasks:   3,
				MaxConcurrentLangchainTasks: 3,
			},
			UnstructuredSecret: "unstructured-secret",
		},
	}
}

// create an UnstructuredDataProduct CR for e2e tests
func GetUnstructuredDataProductResource(name, namespace string) v1alpha1.UnstructuredDataProduct {
	if name == "" {
		name = "testingschema"
	}
	if namespace == "" {
		namespace = DefaultE2ENamespace
	}
	return v1alpha1.UnstructuredDataProduct{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "unstructured-data-controller",
				"app.kubernetes.io/managed-by": "kustomize",
			},
		},
		Spec: v1alpha1.UnstructuredDataProductSpec{
			SourceConfig: v1alpha1.SourceConfig{
				Type: v1alpha1.TypeS3,
				S3Config: v1alpha1.S3Config{
					Bucket: "unstructured-bucket",
					Prefix: "testingschema",
				},
			},
			DestinationConfig: v1alpha1.DestinationConfig{
				Type: v1alpha1.DestinationTypeInternalStage,
				SnowflakeInternalStageConfig: v1alpha1.SnowflakeInternalStageConfig{
					Database: "testing_db",
					Schema:   "testingschema",
					Stage:    "testingschema_internal_stg",
				},
			},
			DocumentProcessorConfig: v1alpha1.DocumentProcessorConfig{
				Type: "docling",
				DoclingConfig: v1alpha1.DoclingConfig{
					FromFormats:     []string{"pdf", "docx", "doc", "txt", "html", "md", "csv", "xlsx"},
					ToFormats:       []string{"md"},
					ImageExportMode: "copy",
					DoOCR:           false,
					ForceOCR:        false,
					OCREngine:       "tesseract",
					OCRLang:         []string{"en"},
					PDFBackend:      "pypdf",
					TableMode:       "none",
					AbortOnError:    true,
				},
			},
			ChunksGeneratorConfig: v1alpha1.ChunksGeneratorConfig{
				Strategy: v1alpha1.ChunkingStrategyMarkdown,
				MarkdownSplitterConfig: v1alpha1.MarkdownSplitterConfig{
					ChunkSize:        1000,
					ChunkOverlap:     200,
					CodeBlocks:       true,
					ReferenceLinks:   true,
					HeadingHierarchy: true,
					JoinTableRows:    true,
				},
			},
		},
	}
}
