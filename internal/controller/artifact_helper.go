package controller

import "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"

// GetProcessorForArtifact returns the artifact processor for the given artifact name
func GetProcessorForArtifact(artifactName string) unstructured.ArtifactProcessor {
	switch artifactName {
	case "documentProcessorConfig":
		return DocumentProcessor{}
	case "chunksGeneratorConfig":
		return ChunksGenerator{}
	case "vectorEmbeddingsGeneratorConfig":
		return VectorEmbeddings{}
	default:
		return nil
	}
}
