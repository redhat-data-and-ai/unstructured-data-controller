/*
Copyright 2025.

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

package unstructured

import (
	"slices"
	"strings"
)

const (
	MetadataFileSuffix         = "-metadata.json"
	ConvertedFileSuffix        = "-converted.json"
	ChunksFileSuffix           = "-chunks.json"
	VectorEmbeddingsFileSuffix = "-vector-embeddings.json"
)

type RawFileMetadata struct {
	FilePath string `json:"filePath,omitempty"`
	UID      string `json:"uid,omitempty"`
}

func GetMetadataFilePath(rawFilePath string) string {
	return rawFilePath + MetadataFileSuffix
}

func GetConvertedFilePath(rawFilePath string) string {
	return rawFilePath + ConvertedFileSuffix
}

func GetChunksFilePath(rawFilePath string) string {
	return rawFilePath + ChunksFileSuffix
}

func GetVectorEmbeddingsFilePath(rawFilePath string) string {
	return rawFilePath + VectorEmbeddingsFileSuffix
}

func GetRawFilePathFromConvertedFilePath(convertedFilePath string) string {
	return strings.TrimSuffix(convertedFilePath, ConvertedFileSuffix)
}

func FilterConvertedFilePaths(filePaths []string) []string {
	convertedFilePaths := []string{}

	for _, filePath := range filePaths {
		if strings.HasSuffix(filePath, ConvertedFileSuffix) {
			convertedFilePaths = append(convertedFilePaths, filePath)
		}
	}
	return convertedFilePaths
}

func FilterChunksFilePaths(filePaths []string) []string {
	chunksFilePaths := []string{}
	for _, filePath := range filePaths {
		if strings.HasSuffix(filePath, ChunksFileSuffix) {
			chunksFilePaths = append(chunksFilePaths, filePath)
		}
	}
	return chunksFilePaths
}

func FilterRawFilePaths(filePaths []string) []string {
	rawFilePaths := []string{}
	for _, filePath := range filePaths {
		if strings.HasSuffix(filePath, MetadataFileSuffix) {
			// this means that the metadata file exists ... now let's check if the raw file exists as well in the filePaths list
			rawFilePath := strings.TrimSuffix(filePath, MetadataFileSuffix)
			if slices.Contains(filePaths, rawFilePath) {
				rawFilePaths = append(rawFilePaths, rawFilePath)
			}
		}
	}
	return rawFilePaths
}
