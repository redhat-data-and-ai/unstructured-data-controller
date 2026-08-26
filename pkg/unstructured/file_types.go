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
	"strings"
)

// SupportedFileExtensions mirrors Docling-compatible document extensions, excluding image.
// Supported: docx, doc, pptx, html, pdf, asciidoc, md/txt, csv, xlsx.
var SupportedFileExtensions = map[string]bool{
	".docx":     true,
	".doc":      true, // legacy Word (GDrive exports as PDF; S3 needs LibreOffice/Docling support)
	".pptx":     true,
	".html":     true,
	".htm":      true,
	".pdf":      true,
	".adoc":     true,
	".asciidoc": true,
	".md":       true,
	".markdown": true,
	".txt":      true, // Docling treats as MD
	".text":     true, // Docling treats as MD
	".csv":      true,
	".xlsx":     true,
}

func IsSupportedFileType(fileName string) bool {
	return SupportedFileExtensions[strings.ToLower(path.Ext(fileName))]
}

func FileExtension(fileName string) string {
	ext := strings.ToLower(path.Ext(fileName))
	if ext == "" {
		return extensionNone
	}
	return ext
}
