package utils

import (
	"context"
	"fmt"
	"math/rand"

	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/e2e-framework/pkg/utils"
)

// DefaultE2ENamespace is the namespace used by e2e tests (must match test/e2e/main_test.go testNamespace).
const DefaultE2ENamespace = "unstructured-controller-namespace"

func GetControllerConfigResource() *v1alpha1.ControllerConfig {
	return &v1alpha1.ControllerConfig{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "controllerconfig",
			Namespace: DefaultE2ENamespace,
		},
		Spec: v1alpha1.ControllerConfigSpec{
			SecretRef:                   "operator-secret",
			DoclingServeURL:             "http://docling-serve:5001",
			DataStorageBucket:           "data-storage-bucket",
			CacheDirectory:              "/data/cache/",
			MaxConcurrentDoclingTasks:   3,
			MaxConcurrentLangchainTasks: 3,
		},
	}
}

// GetUnstructuredDataPipelineResourceWithStage creates an UnstructuredDataPipeline CR for e2e tests
func GetUnstructuredDataPipelineResourceWithStage(name, namespace string) v1alpha1.UnstructuredDataPipeline {
	if name == "" {
		name = "unstructured"
	}
	if namespace == "" {
		namespace = DefaultE2ENamespace
	}
	return v1alpha1.UnstructuredDataPipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "unstructured-data-controller",
				"app.kubernetes.io/managed-by": "kustomize",
			},
		},
		Spec: v1alpha1.UnstructuredDataPipelineSpec{
			Description: "e2e test pipeline",
			Stages: []v1alpha1.PipelineStage{
				{
					Name: "crawl",
					Type: v1alpha1.StageTypeSourceCrawler,
					SourceCrawlerConfig: &v1alpha1.SourceCrawlerConfig{
						Type: v1alpha1.TypeS3,
						S3Config: v1alpha1.S3Config{
							Bucket: "unstructured-bucket",
							Prefix: "unstructured",
						},
					},
				},
				{
					Name:      "convert",
					Type:      v1alpha1.StageTypeDocumentProcessor,
					DependsOn: []v1alpha1.StageDependency{{Name: "crawl"}},
					DocumentProcessorConfig: &v1alpha1.DocumentProcessorConfig{
						Type: "docling",
						DoclingConfig: v1alpha1.DoclingConfig{
							FromFormats:     []string{"pdf", "docx", "html", "md", "csv", "xlsx"},
							ToFormats:       []string{"md"},
							ImageExportMode: "embedded",
							OCRPreset:       "auto",
							OCRLang:         []string{"en"},
							PDFBackend:      "docling_parse",
							Pipeline:        "standard",
							TableMode:       "fast",
						},
					},
				},
				{
					Name:      "chunk",
					Type:      v1alpha1.StageTypeChunksGenerator,
					DependsOn: []v1alpha1.StageDependency{{Name: "convert"}},
					ChunksGeneratorConfig: &v1alpha1.ChunksGeneratorConfig{
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
				{
					Name:      "embed",
					Type:      v1alpha1.StageTypeVectorEmbeddingsGenerator,
					DependsOn: []v1alpha1.StageDependency{{Name: "chunk"}},
					VectorEmbeddingsGeneratorConfig: &v1alpha1.VectorEmbeddingsGeneratorConfig{
						ModelName: "nomic-ai/nomic-embed-text-v1.5",
					},
				},
				{
					Name:      "sync",
					Type:      v1alpha1.StageTypeDestinationSyncer,
					DependsOn: []v1alpha1.StageDependency{{Name: "embed"}},
					DestinationSyncerConfig: &v1alpha1.DestinationSyncerConfig{
						Type: v1alpha1.TypeS3,
						S3DestinationConfig: v1alpha1.S3Config{
							Bucket: "output-bucket",
						},
					},
				},
			},
		},
	}
}

// GetControllerConfigResourceWithGDrive creates a ControllerConfig with GoogleDriveConfig for e2e tests.
func GetControllerConfigResourceWithGDrive() *v1alpha1.ControllerConfig {
	cfg := GetControllerConfigResource()
	cfg.Spec.GoogleDriveConfig = &v1alpha1.GoogleDriveControllerConfig{
		MaxRetries:          3,
		ConcurrentFolders:   5,
		ConcurrentDownloads: 10,
	}
	return cfg
}

// GetGDrivePipelineResource creates a Google Drive UnstructuredDataPipeline CR for e2e tests.
func GetGDrivePipelineResource(name, namespace, folderURL string) v1alpha1.UnstructuredDataPipeline {
	if name == "" {
		name = "gdrive-pipeline"
	}
	if namespace == "" {
		namespace = DefaultE2ENamespace
	}
	return v1alpha1.UnstructuredDataPipeline{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"app.kubernetes.io/name":       "unstructured-data-controller",
				"app.kubernetes.io/managed-by": "kustomize",
			},
		},
		Spec: v1alpha1.UnstructuredDataPipelineSpec{
			Description: "e2e test pipeline for Google Drive",
			Stages: []v1alpha1.PipelineStage{
				{
					Name: "crawl",
					Type: v1alpha1.StageTypeSourceCrawler,
					SourceCrawlerConfig: &v1alpha1.SourceCrawlerConfig{
						Type: v1alpha1.TypeGoogleDrive,
						GoogleDriveConfig: &v1alpha1.GoogleDriveConfig{
							Folders: []v1alpha1.GoogleDriveFolders{{URL: folderURL}},
						},
					},
				},
				{
					Name:      "convert",
					Type:      v1alpha1.StageTypeDocumentProcessor,
					DependsOn: []v1alpha1.StageDependency{{Name: "crawl"}},
					DocumentProcessorConfig: &v1alpha1.DocumentProcessorConfig{
						Type: "docling",
						DoclingConfig: v1alpha1.DoclingConfig{
							FromFormats:     []string{"pdf", "docx", "html", "md", "csv", "xlsx"},
							ToFormats:       []string{"md"},
							ImageExportMode: "embedded",
							OCRPreset:       "auto",
							OCRLang:         []string{"en"},
							PDFBackend:      "docling_parse",
							Pipeline:        "standard",
							TableMode:       "fast",
						},
					},
				},
				{
					Name:      "chunk",
					Type:      v1alpha1.StageTypeChunksGenerator,
					DependsOn: []v1alpha1.StageDependency{{Name: "convert"}},
					ChunksGeneratorConfig: &v1alpha1.ChunksGeneratorConfig{
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
				{
					Name:      "embed",
					Type:      v1alpha1.StageTypeVectorEmbeddingsGenerator,
					DependsOn: []v1alpha1.StageDependency{{Name: "chunk"}},
					VectorEmbeddingsGeneratorConfig: &v1alpha1.VectorEmbeddingsGeneratorConfig{
						ModelName: "nomic-ai/nomic-embed-text-v1.5",
					},
				},
				{
					Name:      "sync",
					Type:      v1alpha1.StageTypeDestinationSyncer,
					DependsOn: []v1alpha1.StageDependency{{Name: "embed"}},
					DestinationSyncerConfig: &v1alpha1.DestinationSyncerConfig{
						Type: v1alpha1.TypeS3,
						S3DestinationConfig: v1alpha1.S3Config{
							Bucket: "gdrive-output-bucket",
						},
					},
				},
			},
		},
	}
}

// RandomStringGenerator will return a random string of provided length
func RandomStringGenerator(length int) string {
	charset := "abcdefghijklmnopqrstuvwxyz0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.Intn(len(charset))]
	}
	return string(b)
}

// WaitForResourceReady waits for a resource to be ready with a default 10m timeout.
func WaitForResourceReady(ctx context.Context, condition, crdName, resourceName, namespace string) error {
	return WaitForResourceReadyWithTimeout(ctx, condition, crdName, resourceName, namespace, "10m")
}

// WaitForResourceReadyWithTimeout waits for a resource to be ready with a custom timeout.
func WaitForResourceReadyWithTimeout(
	ctx context.Context, condition, crdName, resourceName, namespace, timeout string,
) error {
	cmd := fmt.Sprintf("kubectl wait --for=condition=%s %s %s -n %s --timeout=%s",
		condition, crdName, resourceName, namespace, timeout)
	p := utils.RunCommandContext(ctx, cmd)
	if p.Err() != nil {
		return p.Err()
	}
	return nil
}
