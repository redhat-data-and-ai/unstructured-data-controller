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
//	  stageName: chunk
//	  dependsOn:
//	    - name: convert
//	  config:
//	    strategy: recursiveCharacterTextSplitter
//	    recursiveCharacterSplitterConfig:
//	      chunkSize: 1000
//	      chunkOverlap: 200
//	status:
//	  conditions:
//	    - type: ChunksGeneratorReady
//	      status: "True"
//	      message: successfully reconciled
//
// sample config for the docling hybrid chunker, which splits on the
// document's structure rather than on character counts. Only the tokenizer
// has to be given; the rest is defaulted. It is a path into docling's own
// image rather than a model name, because a name has to be downloaded and a
// download that fails returns no chunks and no error.
//
//	config:
//	  strategy: doclingHybridChunker
//	  hybridChunkerConfig:
//	    tokenizer: /modelcache/HuggingFaceTB--SmolVLM-256M-Instruct
//	    maxTokens: 2048                  # the embedding model's window
//	    includeRawText: true             # or there is nothing to display
//	    useMarkdownTables: true          # instead of value triplets

const (
	ChunksGeneratorCondition = "ChunksGeneratorReady"
	DefaultChunkingStrategy  = ChunkingStrategyRecursiveCharacter
	DefaultChunkSize         = 1000
	DefaultChunkOverlap      = 200

	// DefaultHybridMaxTokens is the context window of the embedding model
	// these chunks are destined for.
	//
	// It is not reduced to leave room for the heading trail. The chunker
	// measures the finished, contextualised string and subtracts the headings
	// itself, so this is a cap on what comes out rather than on the body.
	//
	// It is not reduced for the tokenizer mismatch either, though the
	// temptation is real: tokens are counted here by one tokenizer and the
	// chunk is later embedded by a model with a different vocabulary.
	// Measured over 171 chunks of a 143 page document, the counting tokenizer
	// produced more tokens than the embedding model for every chunk large
	// enough to approach this cap, so the error runs in the safe direction.
	// The largest chunks measured came to 0.70 to 0.92 of this count. The
	// ratio only exceeds 1.0 on chunks of a few hundred tokens, which cannot
	// reach the cap, so budgeting against that worst case would cut the
	// window by a quarter to protect chunks that are never at risk.
	DefaultHybridMaxTokens = 2048
)

// ChunksGeneratorSpec defines the desired state of ChunksGenerator
type ChunksGeneratorSpec struct {
	StageName             string                `json:"stageName,omitempty"`
	DependsOn             []StageDependency     `json:"dependsOn,omitempty"`
	ChunksGeneratorConfig ChunksGeneratorConfig `json:"config,omitempty"`
	// Deprecated: use StageName and DependsOn instead.
	// +optional
	DataProduct string `json:"dataProduct,omitempty"`
}

// ChunksGeneratorStatus defines the observed state of ChunksGenerator
type ChunksGeneratorStatus struct {
	LastAppliedGeneration int64              `json:"lastAppliedGeneration,omitempty"`
	Conditions            []metav1.Condition `json:"conditions,omitempty"`
	FilesProcessed        int64              `json:"filesProcessed,omitempty"`
	// Jobs tracks chunking tasks that have been submitted and not yet
	// finished. Only the strategies that chunk remotely use it; the in
	// process splitters finish within one reconcile and leave it empty.
	Jobs []ChunkJob `json:"jobs,omitempty"`
	// PermanentlyFailingFiles are files that will not be retried.
	PermanentlyFailingFiles []string `json:"permanentlyFailingFiles,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="Status",type="string",JSONPath=".status.conditions[?(@.type==\"ChunksGeneratorReady\")].status"
// +kubebuilder:printcolumn:name="Message",type="string",JSONPath=".status.conditions[?(@.type==\"ChunksGeneratorReady\")].message"
// +kubebuilder:printcolumn:name="Files",type="integer",JSONPath=".status.filesProcessed"

// ChunksGenerator is the Schema for the chunksgenerators API
type ChunksGenerator struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ChunksGeneratorSpec   `json:"spec,omitempty"`
	Status ChunksGeneratorStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ChunksGeneratorList contains a list of ChunksGenerator
type ChunksGeneratorList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []ChunksGenerator `json:"items"`
}

func (c *ChunksGenerator) GetFilesProcessed() int64 {
	return c.Status.FilesProcessed
}

func (c *ChunksGenerator) SetWaiting() {
	condition := metav1.Condition{
		Type:               ChunksGeneratorCondition,
		LastTransitionTime: metav1.Now(),
		Status:             metav1.ConditionUnknown,
		Message:            "ChunksGenerator is getting reconciled",
		Reason:             "Waiting",
	}
	for i, currentCondition := range c.Status.Conditions {
		if currentCondition.Type == condition.Type {
			c.Status.Conditions[i] = condition
			return
		}
	}
	c.Status.Conditions = append(c.Status.Conditions, condition)
}

// AddOrUpdateJob records a submitted chunking task, replacing any earlier one
// for the same file. A file has at most one job, because a second task for it
// would write the same artifact twice and hold a second slot against the
// chunking service for no gain.
func (c *ChunksGenerator) AddOrUpdateJob(newJob ChunkJob) {
	for i, job := range c.Status.Jobs {
		if job.FilePath == newJob.FilePath {
			c.Status.Jobs[i] = newJob
			return
		}
	}
	c.Status.Jobs = append(c.Status.Jobs, newJob)
}

// GetJobByFilePath returns the job in flight for a file, or nil if there is
// none. The pointer is into the status slice, so the caller reads it rather
// than mutating it; status changes go through AddOrUpdateJob under a patch.
func (c *ChunksGenerator) GetJobByFilePath(filePath string) *ChunkJob {
	for i := range c.Status.Jobs {
		if c.Status.Jobs[i].FilePath == filePath {
			return &c.Status.Jobs[i]
		}
	}
	return nil
}

// DeleteJobByFilePath forgets a job, which is what finishing with a file
// looks like: either its chunks were written or it has been given up on.
func (c *ChunksGenerator) DeleteJobByFilePath(filePath string) {
	remaining := []ChunkJob{}
	for _, job := range c.Status.Jobs {
		if job.FilePath != filePath {
			remaining = append(remaining, job)
		}
	}
	c.Status.Jobs = remaining
}

// AddPermanentlyFailingFile records a file that will not be tried again.
//
// Without it a file that fails every attempt is resubmitted forever, because
// the only other record of the attempts is the job, and giving up means
// removing that job.
func (c *ChunksGenerator) AddPermanentlyFailingFile(filePath string) {
	if slices.Contains(c.Status.PermanentlyFailingFiles, filePath) {
		return
	}
	c.Status.PermanentlyFailingFiles = append(c.Status.PermanentlyFailingFiles, filePath)
}

func (c *ChunksGenerator) IsFilePermanentlyFailing(filePath string) bool {
	return slices.Contains(c.Status.PermanentlyFailingFiles, filePath)
}

func (c *ChunksGenerator) UpdateStatus(message string, err error) {
	condition := metav1.Condition{
		Type:               ChunksGeneratorCondition,
		LastTransitionTime: metav1.Now(),
	}
	if err == nil {
		condition.Status = metav1.ConditionTrue
		condition.Message = message
		condition.Reason = SuccessfullyReconciled
		c.Status.LastAppliedGeneration = c.Generation
	} else {
		condition.Status = metav1.ConditionFalse
		condition.Message = message + ", error: " + err.Error()
		condition.Reason = ReconcileFailed
	}

	for i, currentCondition := range c.Status.Conditions {
		if currentCondition.Type == condition.Type {
			c.Status.Conditions[i] = condition
			return
		}
	}
	c.Status.Conditions = append(c.Status.Conditions, condition)
}

// The rule uses size() rather than comparing against an empty string literal.
// Two adjacent apostrophes are what text editors and chat tools turn into a
// typographic quote, and a marker is a comment, so Go, vet and the linters
// all accept the result. It surfaces only when the API server compiles the
// rule at CRD install. See TestKubebuilderMarkersAreASCII.
// +kubebuilder:validation:XValidation:rule="self.strategy != 'doclingHybridChunker' || (has(self.hybridChunkerConfig) && has(self.hybridChunkerConfig.tokenizer) && size(self.hybridChunkerConfig.tokenizer) > 0)",message="hybridChunkerConfig.tokenizer is required when strategy is doclingHybridChunker"
type ChunksGeneratorConfig struct {
	// Strategy selects the chunker.
	//
	// The empty string is accepted because this field is required in the
	// generated CRD, so resources that want the default set it empty rather
	// than omitting it, and SetDefaults fills it in.
	// +kubebuilder:validation:Enum=recursiveCharacterTextSplitter;markdownTextSplitter;tokenTextSplitter;doclingHybridChunker;""
	Strategy                         ChunkingStrategy                 `json:"strategy"`
	RecursiveCharacterSplitterConfig RecursiveCharacterSplitterConfig `json:"recursiveCharacterSplitterConfig,omitempty"`
	MarkdownSplitterConfig           MarkdownSplitterConfig           `json:"markdownSplitterConfig,omitempty"`
	TokenSplitterConfig              TokenSplitterConfig              `json:"tokenSplitterConfig,omitempty"`
	HybridChunkerConfig              HybridChunkerConfig              `json:"hybridChunkerConfig,omitempty"`
}

// HybridChunkerConfig configures docling's own chunker, which splits on the
// document's structure instead of on character counts. The other three
// strategies split text in process; this one is a call to docling, and each
// chunk it returns knows which part of the document it came from.
//
// Every one of docling's own defaults is wrong for this use, which is why all
// four settings are explicit.
type HybridChunkerConfig struct {
	// Tokenizer counts the tokens MaxTokens is measured in. Required, and
	// admission rejects an empty one, because there is no default that works
	// and the failure is silent.
	//
	// A HuggingFace model name only resolves if docling can reach
	// huggingface.co. Where it cannot, an unreachable tokenizer is not an
	// error: the request returns success with zero chunks once the download
	// times out. Measured against a deployment with no egress, a HuggingFace
	// name returned 0 chunks in 58s where a path to a tokenizer inside
	// docling's own image returned 1 in 9.5s. Docling's own default is a
	// HuggingFace name.
	// +optional
	Tokenizer string `json:"tokenizer,omitempty"`

	// MaxTokens caps the finished chunk, headings included, because the
	// chunker subtracts the heading trail itself. Set it to the context
	// window of the embedding model the chunks are destined for.
	//
	// Leaving it unset is not an option either. Docling falls back to the
	// tokenizer's own limit, which is several times a typical embedding
	// window, and produces chunks nothing can embed.
	// +kubebuilder:validation:Minimum=1
	// +optional
	MaxTokens int `json:"maxTokens,omitempty"`

	// IncludeRawText returns the body separately from the contextualised
	// text. Both are wanted: the contextualised one is embedded, so a chunk
	// is findable under its section heading, and the raw one is what gets
	// displayed. Defaults true. False leaves nothing to display.
	// +optional
	IncludeRawText *bool `json:"includeRawText,omitempty"`

	// UseMarkdownTables serialises a table as markdown rather than as
	// "RowLabel, ColumnName = value" triplets. Defaults true.
	//
	// Settable rather than fixed because on a table-dense corpus this is a
	// genuine retrieval-quality tradeoff rather than an obvious improvement.
	// +optional
	UseMarkdownTables *bool `json:"useMarkdownTables,omitempty"`
}

// ChunkJob tracks one chunking task that docling has accepted and not yet
// finished. Chunking is submitted and then polled, so without this a restart
// loses every outstanding task and the files behind them are never completed.
//
// It carries the config that was in effect at submission. The result can
// arrive after the spec has changed, and the metadata written then has to
// describe the chunking that actually ran.
type ChunkJob struct {
	FilePath              string                `json:"filePath,omitempty"`
	FileIdentifier        string                `json:"fileIdentifier,omitempty"`
	ChunksGeneratorConfig ChunksGeneratorConfig `json:"chunksGeneratorConfig,omitempty"`
	TaskID                string                `json:"taskID,omitempty"`
	Status                string                `json:"status,omitempty"`
	CreatedOn             string                `json:"createdOn,omitempty"`
	Attempts              int                   `json:"attempts,omitempty"`
}

type RecursiveCharacterSplitterConfig struct {
	Separators    []string `json:"separators,omitempty"`
	ChunkSize     int      `json:"chunkSize,omitempty"`
	ChunkOverlap  int      `json:"chunkOverlap,omitempty"`
	KeepSeparator bool     `json:"keepSeparator,omitempty"`
}

type MarkdownSplitterConfig struct {
	ChunkSize        int  `json:"chunkSize,omitempty"`
	ChunkOverlap     int  `json:"chunkOverlap,omitempty"`
	CodeBlocks       bool `json:"codeBlocks,omitempty"`
	ReferenceLinks   bool `json:"referenceLinks,omitempty"`
	HeadingHierarchy bool `json:"headingHierarchy,omitempty"`
	JoinTableRows    bool `json:"joinTableRows,omitempty"`
}

type TokenSplitterConfig struct {
	ChunkSize         int      `json:"chunkSize,omitempty"`
	ChunkOverlap      int      `json:"chunkOverlap,omitempty"`
	ModelName         string   `json:"modelName,omitempty"`
	EncodingName      string   `json:"encodingName,omitempty"`
	AllowedSpecial    []string `json:"allowedSpecial,omitempty"`
	DisallowedSpecial []string `json:"disallowedSpecial,omitempty"`
}

// SetDefaults fills in sane defaults for any unset fields.
func (c *ChunksGeneratorConfig) SetDefaults() {
	if c.Strategy == "" {
		c.Strategy = DefaultChunkingStrategy
	}

	switch c.Strategy {
	case ChunkingStrategyRecursiveCharacter:
		if c.RecursiveCharacterSplitterConfig.ChunkSize == 0 {
			c.RecursiveCharacterSplitterConfig.ChunkSize = DefaultChunkSize
		}
		if c.RecursiveCharacterSplitterConfig.ChunkOverlap == 0 {
			c.RecursiveCharacterSplitterConfig.ChunkOverlap = DefaultChunkOverlap
		}
	case ChunkingStrategyMarkdown:
		if c.MarkdownSplitterConfig.ChunkSize == 0 {
			c.MarkdownSplitterConfig.ChunkSize = DefaultChunkSize
		}
		if c.MarkdownSplitterConfig.ChunkOverlap == 0 {
			c.MarkdownSplitterConfig.ChunkOverlap = DefaultChunkOverlap
		}
	case ChunkingStrategyToken:
		if c.TokenSplitterConfig.ChunkSize == 0 {
			c.TokenSplitterConfig.ChunkSize = DefaultChunkSize
		}
		if c.TokenSplitterConfig.ChunkOverlap == 0 {
			c.TokenSplitterConfig.ChunkOverlap = DefaultChunkOverlap
		}
	case ChunkingStrategyDoclingHybrid:
		// Only this strategy, never unconditionally. This config travels into
		// the metadata that decides whether a file needs chunking again, so
		// filling it in for every generator would re-chunk every corpus,
		// including those of the generators that never read it.
		//
		// Tokenizer is absent on purpose. There is no value that works
		// everywhere, and admission already rejects an empty one for this
		// strategy, so a default here could only be a wrong guess.
		if c.HybridChunkerConfig.MaxTokens == 0 {
			c.HybridChunkerConfig.MaxTokens = DefaultHybridMaxTokens
		}
		if c.HybridChunkerConfig.IncludeRawText == nil {
			c.HybridChunkerConfig.IncludeRawText = boolPtr(true)
		}
		if c.HybridChunkerConfig.UseMarkdownTables == nil {
			c.HybridChunkerConfig.UseMarkdownTables = boolPtr(true)
		}
	default:
	}
}

func init() {
	SchemeBuilder.Register(&ChunksGenerator{}, &ChunksGeneratorList{})
}
