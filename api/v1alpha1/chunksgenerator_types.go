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

const (
	ChunksGeneratorCondition = "ChunksGeneratorReady"
	DefaultChunkingStrategy  = ChunkingStrategyRecursiveCharacter
	DefaultChunkSize         = 1000
	DefaultChunkOverlap      = 200

	// DefaultMaxChunkChars caps the text sent to the embedding model, sized
	// for nomic-embed-text-v1.5's 2048-token context.
	//
	// Measured, not estimated: a 3,943 character chunk of HTML table markup
	// exceeded 2048 tokens, so the worst case here is under 1.9 characters
	// per token. Tags like `<th colspan="1" rowspan="1">` are mostly
	// punctuation, which tokenizes far denser than prose.
	//
	// 2500 characters is about 1,300 tokens at that ratio, leaving headroom
	// for markup denser still. The cost of being wrong is asymmetric:
	// over-splitting fragments a table, while under-splitting is an HTTP 400
	// that fails the whole document.
	//
	// Raise it only alongside an embedding model with a larger context.
	DefaultMaxChunkChars = 2500
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

type ChunksGeneratorConfig struct {
	Strategy                         ChunkingStrategy                 `json:"strategy"`
	RecursiveCharacterSplitterConfig RecursiveCharacterSplitterConfig `json:"recursiveCharacterSplitterConfig,omitempty"`
	MarkdownSplitterConfig           MarkdownSplitterConfig           `json:"markdownSplitterConfig,omitempty"`
	TokenSplitterConfig              TokenSplitterConfig              `json:"tokenSplitterConfig,omitempty"`

	// MaxChunkChars caps a single block chunk, splitting anything larger
	// across several chunks that each keep the block's metadata.
	//
	// Block-based chunking produces chunks as large as the block, and a
	// full-page table can be tens of kilobytes of HTML. The limit here is the
	// EMBEDDING model, not the parser: nomic-embed-text-v1.5 as deployed
	// accepts 2048 tokens and rejects an oversized input with HTTP 400 rather
	// than truncating it.
	//
	// The default is deliberately conservative. HTML tables tokenize far
	// worse than prose (every tag is several tokens), so the usual
	// four-characters-per-token rule does not hold, and the metadata prefix
	// in embedText is counted too.
	//
	// Only applies to block-based chunking. Zero disables splitting.
	// +optional
	MaxChunkChars int `json:"maxChunkChars,omitempty"`
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
	if c.MaxChunkChars == 0 {
		c.MaxChunkChars = DefaultMaxChunkChars
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
	default:
	}
}

func init() {
	SchemeBuilder.Register(&ChunksGenerator{}, &ChunksGeneratorList{})
}
