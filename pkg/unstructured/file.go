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

// convertedFileSuffix is what the document processor appends to a raw file
// path to name the document it converted.
const convertedFileSuffix = ".json"

// ConvertedFilePath names the document processor's output for one raw file.
func ConvertedFilePath(rawFilePath string) string {
	return rawFilePath + convertedFileSuffix
}

// ConvertedFilePaths returns only the converted documents in a listing of
// the convert stage's output.
//
// A whitelist rather than an exclusion of what else is in there today: the
// stage already writes page images beside these, and anything added later
// is ignored without this having to learn about it.
func ConvertedFilePaths(filePaths []string) []string {
	converted := make([]string, 0, len(filePaths))
	for _, filePath := range filePaths {
		if strings.HasSuffix(filePath, convertedFileSuffix) {
			converted = append(converted, filePath)
		}
	}
	return converted
}

// pageImageSuffix separates a page image from the raw file it renders.
const pageImageSuffix = ".page-"

// PageImagePath names the stored render of one page, counted from one.
//
// Derived from the raw file path rather than stored anywhere, so a citation
// can find the image from the file path and page number it already has.
func PageImagePath(rawFilePath string, pageNo int) string {
	return rawFilePath + pageImageSuffix + strconv.Itoa(pageNo) + ".png"
}

// DestinationKey returns where a file lands under the destination prefix.
//
// The syncer keys by the stage directory and drops everything above it, so
// this reproduces that without needing the pipeline name. The stage name is
// part of the match because a pipeline name may itself end in "stages".
// Returns "" for a path that is not under that stage.
func DestinationKey(filePath, stageName string) string {
	index := strings.Index(filePath, "stages/"+stageName+"/")
	if index < 0 {
		return ""
	}
	return filePath[index:]
}

// PageImageDestinationKey locates one page's render in the destination, given
// the converted file it belongs to and the stage that wrote it.
func PageImageDestinationKey(convertedFilePath, stageName string, pageNo int) string {
	rawFilePath := strings.TrimSuffix(convertedFilePath, convertedFileSuffix)
	return DestinationKey(PageImagePath(rawFilePath, pageNo), stageName)
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
