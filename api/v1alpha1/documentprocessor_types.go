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

package v1alpha1

import (
	"slices"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// sample spec:
//
//	spec:
//	  stageName: convert
//	  dependsOn:
//	    - name: crawl                        # upstream stage name
//	  config:
//	    type: docling
//	    doclingConfig:
//	      from_formats: [docx, pptx, html, image, pdf, asciidoc, md, csv, xlsx]
//	      do_ocr: true
//	      ocr_preset: auto
//	      pdf_backend: docling_parse
//	      pipeline: standard
//	      table_mode: accurate
//	      table_cell_matching: true
//	      do_table_structure: true
//	      include_images: true
//	      images_scale: "2.0"
//	status:
//	  conditions:
//	    - type: DocumentProcessorReady
//	      status: "True"
//	      message: successfully reconciled
//	  jobs: []                               # tracks in-flight docling conversion jobs

const (
	DocumentProcessorCondition   = "DocumentProcessorReady"
	DefaultDocumentProcessorType = "docling"
	DefaultOCRPreset             = "auto"
	DefaultPDFBackend            = "docling_parse"
	DefaultPipeline              = "standard"
	DefaultTableMode             = "accurate"
	DefaultImageExportMode       = "embedded"
	DefaultImagesScale           = "2.0"
)

var defaultFromFormats = []string{"docx", "pptx", "html", "image", "pdf", "asciidoc", "md", "csv", "xlsx"}

func boolPtr(b bool) *bool { return &b }

// DocumentProcessorSpec defines the desired state of DocumentProcessor
type DocumentProcessorSpec struct {
	StageName               string                  `json:"stageName,omitempty"`
	DependsOn               []StageDependency       `json:"dependsOn,omitempty"`
	DocumentProcessorConfig DocumentProcessorConfig `json:"config,omitempty"`
	// Deprecated: use StageName and DependsOn instead.
	// +optional
	DataProduct string `json:"dataProduct,omitempty"`
}

// DocumentProcessorStatus defines the observed state of DocumentProcessor
type DocumentProcessorStatus struct {
	LastAppliedGeneration   int64              `json:"lastAppliedGeneration,omitempty"`
	Conditions              []metav1.Condition `json:"conditions,omitempty"`
	Jobs                    []Job              `json:"jobs,omitempty"`
	PermanentlyFailingFiles []string           `json:"permanentlyFailingFiles,omitempty"`
	FilesProcessed          int64              `json:"filesProcessed,omitempty"`
}

type Job struct {
	FilePath          string        `json:"filePath,omitempty"`
	FileIdentifier    string        `json:"fileIdentifier,omitempty"`
	DocumentConverter string        `json:"documentConverter,omitempty"`
	DoclingConfig     DoclingConfig `json:"doclingConfig,omitempty"`
	TaskID            string        `json:"taskID,omitempty"`
	Status            string        `json:"status,omitempty"`
	CreatedOn         string        `json:"createdOn,omitempty"`
	Attempts          int           `json:"attempts,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"DocumentProcessorReady\")].status"
// +kubebuilder:printcolumn:name="Message",type="string",JSONPath=".status.conditions[?(@.type==\"DocumentProcessorReady\")].message"
// +kubebuilder:printcolumn:name="Files",type="integer",JSONPath=".status.filesProcessed"

// DocumentProcessor is the Schema for the documentprocessors API
type DocumentProcessor struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   DocumentProcessorSpec   `json:"spec,omitempty"`
	Status DocumentProcessorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// DocumentProcessorList contains a list of DocumentProcessor
type DocumentProcessorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []DocumentProcessor `json:"items"`
}

func (d *DocumentProcessor) GetFilesProcessed() int64 {
	return d.Status.FilesProcessed
}

func (d *DocumentProcessor) SetWaiting() {
	condition := metav1.Condition{
		Type:               DocumentProcessorCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "DocumentProcessor is getting reconciled",
		Reason:             "Waiting",
	}
	for i, currentCondition := range d.Status.Conditions {
		if currentCondition.Type == condition.Type {
			d.Status.Conditions[i] = condition
			return
		}
	}
	d.Status.Conditions = append(d.Status.Conditions, condition)
}

func (d *DocumentProcessor) AddOrUpdateJob(newJob Job) {
	for i, job := range d.Status.Jobs {
		if job.FilePath == newJob.FilePath {
			d.Status.Jobs[i] = newJob
			return
		}
	}
	d.Status.Jobs = append(d.Status.Jobs, newJob)
}

func (d *DocumentProcessor) GetJobByFilePath(filePath string) *Job {
	for i := range d.Status.Jobs {
		if d.Status.Jobs[i].FilePath == filePath {
			return &d.Status.Jobs[i]
		}
	}
	return nil
}

func (d *DocumentProcessor) DeleteJobByFilePath(filePath string) {
	newJobs := []Job{}
	for _, job := range d.Status.Jobs {
		if job.FilePath != filePath {
			newJobs = append(newJobs, job)
		}
	}
	d.Status.Jobs = newJobs
}

func (d *DocumentProcessor) AddPermanentlyFailingFile(filePath string) {
	if slices.Contains(d.Status.PermanentlyFailingFiles, filePath) {
		return
	}
	d.Status.PermanentlyFailingFiles = append(d.Status.PermanentlyFailingFiles, filePath)
}

func (d *DocumentProcessor) IsFilePermanentlyFailing(filePath string) bool {
	return slices.Contains(d.Status.PermanentlyFailingFiles, filePath)
}

func (d *DocumentProcessor) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               DocumentProcessorCondition,
		LastTransitionTime: metav1.Now(),
	}

	if err == nil {
		condition.Status = metav1.ConditionTrue
		condition.Message = message
		condition.Reason = SuccessfullyReconciled
		d.Status.LastAppliedGeneration = d.Generation
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Message = message + ", error: " + err.Error()
		condition.Reason = ReconcileFailed
	}

	for i, currentCondition := range d.Status.Conditions {
		if currentCondition.Type == condition.Type {
			d.Status.Conditions[i] = condition
			return
		}
	}
	d.Status.Conditions = append(d.Status.Conditions, condition)
}

type DocumentProcessorConfig struct {
	Type          string        `json:"type,omitempty"`
	DoclingConfig DoclingConfig `json:"doclingConfig,omitempty"`
}

type PictureDescriptionAPIParams struct {
	Model     string `json:"model"`
	MaxTokens int    `json:"max_tokens,omitempty"`
}

type PictureDescriptionAPI struct {
	URL         string                      `json:"url"`
	Params      PictureDescriptionAPIParams `json:"params"`
	Prompt      string                      `json:"prompt,omitempty"`
	Timeout     string                      `json:"timeout,omitempty"`
	Concurrency int                         `json:"concurrency,omitempty"`
	Headers     map[string]string           `json:"headers,omitempty"`
}

type DoclingConfig struct {
	FromFormats     []string `json:"from_formats,omitempty"`
	ToFormats       []string `json:"to_formats,omitempty"`
	ImageExportMode string   `json:"image_export_mode,omitempty"`
	DoOCR           bool     `json:"do_ocr,omitempty"`
	ForceOCR        bool     `json:"force_ocr,omitempty"`
	// Deprecated: use OCRPreset instead.
	// +optional
	OCREngine string   `json:"ocr_engine,omitempty"`
	OCRLang   []string `json:"ocr_lang,omitempty"`
	// +optional
	OCRPreset  string `json:"ocr_preset,omitempty"`
	PDFBackend string `json:"pdf_backend,omitempty"`
	// +optional
	Pipeline  string `json:"pipeline,omitempty"`
	TableMode string `json:"table_mode,omitempty"`
	// +optional
	TableCellMatching *bool `json:"table_cell_matching,omitempty"`
	// +optional
	DoTableStructure *bool `json:"do_table_structure,omitempty"`
	// +optional
	IncludeImages *bool `json:"include_images,omitempty"`
	// +optional
	ImagesScale string `json:"images_scale,omitempty"`
	// +optional
	DoCodeEnrichment bool `json:"do_code_enrichment,omitempty"`
	// +optional
	DoFormulaEnrichment bool `json:"do_formula_enrichment,omitempty"`
	// +optional
	DoPictureClassification bool `json:"do_picture_classification,omitempty"`
	// +optional
	DoPictureDescription bool `json:"do_picture_description,omitempty"`
	// +optional
	DoChartExtraction bool `json:"do_chart_extraction,omitempty"`
	// +optional
	PictureDescriptionAPI *PictureDescriptionAPI `json:"picture_description_api,omitempty"`
	// +optional
	PictureDescriptionAreaThreshold string `json:"picture_description_area_threshold,omitempty"`
	// +optional
	DocumentTimeout string `json:"document_timeout,omitempty"`
	// +optional
	PageRange []int `json:"page_range,omitempty"`
	// +optional
	MdPageBreakPlaceholder string `json:"md_page_break_placeholder,omitempty"`
	// Deprecated: this field is ignored; abort_on_error is always false.
	// +optional
	AbortOnError bool `json:"abort_on_error,omitempty"`
}

// SetDefaults fills in sane defaults for any unset fields.
func (c *DocumentProcessorConfig) SetDefaults() {
	if c.Type == "" {
		c.Type = DefaultDocumentProcessorType
	}
	c.DoclingConfig.SetDefaults()
}

// SetDefaults fills in sane defaults for any unset DoclingConfig fields.
func (c *DoclingConfig) SetDefaults() {
	if len(c.FromFormats) == 0 {
		c.FromFormats = append([]string{}, defaultFromFormats...)
	}
	if len(c.ToFormats) == 0 {
		c.ToFormats = []string{"md"}
	}
	if c.ImageExportMode == "" {
		c.ImageExportMode = DefaultImageExportMode
	}
	if c.OCRPreset == "" {
		c.OCRPreset = DefaultOCRPreset
	}
	if c.PDFBackend == "" {
		c.PDFBackend = DefaultPDFBackend
	}
	if c.Pipeline == "" {
		c.Pipeline = DefaultPipeline
	}
	if c.TableMode == "" {
		c.TableMode = DefaultTableMode
	}
	if c.TableCellMatching == nil {
		c.TableCellMatching = boolPtr(true)
	}
	if c.DoTableStructure == nil {
		c.DoTableStructure = boolPtr(true)
	}
	if c.IncludeImages == nil {
		c.IncludeImages = boolPtr(true)
	}
	if c.ImagesScale == "" {
		c.ImagesScale = DefaultImagesScale
	}
}

func init() {
	SchemeBuilder.Register(&DocumentProcessor{}, &DocumentProcessorList{})
}
