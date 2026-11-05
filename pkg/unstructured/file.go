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

package unstructured

import (
	"path"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

// StagePath returns the S3 prefix for a stage's output directory.
// e.g. StagePath("my-pipeline", "crawl") => "pipelines/my-pipeline/stages/crawl/"
func StagePath(pipelineName, stageName string) string {
	return path.Join("pipelines", pipelineName, "stages", stageName) + "/"
}

// RemapToOutputDir maps a file from an input stage directory to the output stage directory.
// e.g. ("stages/crawl/f.pdf", "stages/crawl/", "stages/convert/") => "stages/convert/f.pdf"
func RemapToOutputDir(filePath, inputDir, outputDir string) string {
	rel := strings.TrimPrefix(filePath, inputDir)
	return filepath.Join(outputDir, rel)
}

type RawFileMetadata struct {
	FilePath string `json:"filePath,omitempty"`
	UID      string `json:"uid,omitempty"`
}

func MetadataPath(rawFilePath string) string {
	return rawFilePath + ".json"
}

func PermissionsPath(rawFilePath string) string {
	return rawFilePath + ".permissions.json"
}

// pageImageSuffix separates a page image from the raw file it renders. It
// ends in .png so IsPageImagePath can recognise one without parsing.
const pageImageSuffix = ".page-"

// PageImagePath names the stored render of one page, counted from one.
//
// Derived from the raw file path rather than stored anywhere, so a citation
// can find the image from the file path and page number it already has.
func PageImagePath(rawFilePath string, pageNo int) string {
	return rawFilePath + pageImageSuffix + strconv.Itoa(pageNo) + ".png"
}

// IsPageImagePath reports whether a path is one of those renders.
func IsPageImagePath(filePath string) bool {
	base := strings.TrimSuffix(filePath, ".png")
	if base == filePath {
		return false
	}
	index := strings.LastIndex(base, pageImageSuffix)
	if index < 0 {
		return false
	}
	pageNo, err := strconv.Atoi(base[index+len(pageImageSuffix):])
	return err == nil && pageNo > 0
}

// WithoutPageImages returns the paths that are not page renders.
func WithoutPageImages(filePaths []string) []string {
	kept := make([]string, 0, len(filePaths))
	for _, filePath := range filePaths {
		if !IsPageImagePath(filePath) {
			kept = append(kept, filePath)
		}
	}
	return kept
}

func FilterRawFilePaths(filePaths []string) []string {
	rawFilePaths := []string{}
	for _, filePath := range filePaths {
		if rawFilePath, ok := strings.CutSuffix(filePath, ".json"); ok {
			if slices.Contains(filePaths, rawFilePath) {
				rawFilePaths = append(rawFilePaths, rawFilePath)
			}
		}
	}
	return rawFilePaths
}
