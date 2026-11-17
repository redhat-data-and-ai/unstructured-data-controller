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

package controller

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/docling"
)

const (
	testVLMAPIKey = "test-key"
	testVLMAPIURL = "https://example.invalid/v1/chat/completions"
)

// withVLMPipelineSettings swaps the package-level engine settings for the
// duration of a test and restores them afterwards, so cases cannot leak into
// each other.
func withVLMPipelineSettings(t *testing.T, settings vlmPipelineSettings) {
	t.Helper()
	previous := vlmPipelineCfg.Load()
	vlmPipelineCfg.Store(&settings)
	t.Cleanup(func() { vlmPipelineCfg.Store(previous) })
}

// modelConfig is what SetDefaults produces for a processor that asked for the
// vlm pipeline and nothing else.
func modelConfig() *operatorv1alpha1.VLMPipelineCustomConfig {
	doclingConfig := operatorv1alpha1.DoclingConfig{Pipeline: operatorv1alpha1.PipelineVLM}
	doclingConfig.SetDefaults()
	return doclingConfig.VLMPipelineCustomConfig
}

func TestInjectVLMEngine(t *testing.T) {
	tests := []struct {
		name         string
		settings     vlmPipelineSettings
		pipeline     string
		modelCfg     *operatorv1alpha1.VLMPipelineCustomConfig
		wantInjected bool
		wantAuth     bool
		wantErr      string
	}{
		{
			name:         "injects when the vlm pipeline is selected and the engine is reachable",
			settings:     vlmPipelineSettings{apiKey: testVLMAPIKey, apiURL: testVLMAPIURL},
			pipeline:     operatorv1alpha1.PipelineVLM,
			modelCfg:     modelConfig(),
			wantInjected: true,
			wantAuth:     true,
		},
		{
			name:         "leaves the standard pipeline alone",
			settings:     vlmPipelineSettings{apiKey: testVLMAPIKey, apiURL: testVLMAPIURL},
			pipeline:     operatorv1alpha1.PipelineStandard,
			modelCfg:     modelConfig(),
			wantInjected: false,
		},
		{
			name:         "leaves an unset pipeline alone",
			settings:     vlmPipelineSettings{apiKey: testVLMAPIKey, apiURL: testVLMAPIURL},
			pipeline:     "",
			modelCfg:     modelConfig(),
			wantInjected: false,
		},
		{
			name:         "injects without a credential, sending no Authorization header",
			settings:     vlmPipelineSettings{apiURL: testVLMAPIURL},
			pipeline:     operatorv1alpha1.PipelineVLM,
			modelCfg:     modelConfig(),
			wantInjected: true,
			wantAuth:     false,
		},
		{
			// The four cases below must fail rather than skip: a skipped
			// injection converts on docling's own model and answers 200, while
			// the artifact is fingerprinted as though the named model ran.
			name:     "fails when the endpoint is missing",
			settings: vlmPipelineSettings{apiKey: testVLMAPIKey},
			pipeline: operatorv1alpha1.PipelineVLM,
			modelCfg: modelConfig(),
			wantErr:  "vlmPipelineAPIURL is not set",
		},
		{
			name:     "fails without a model config",
			settings: vlmPipelineSettings{apiKey: testVLMAPIKey, apiURL: testVLMAPIURL},
			pipeline: operatorv1alpha1.PipelineVLM,
			modelCfg: nil,
			wantErr:  "vlm_pipeline_custom_config is not set",
		},
		{
			name:     "fails when the model has no prompt and no catalogued default",
			settings: vlmPipelineSettings{apiKey: testVLMAPIKey, apiURL: testVLMAPIURL},
			pipeline: operatorv1alpha1.PipelineVLM,
			modelCfg: &operatorv1alpha1.VLMPipelineCustomConfig{
				Model:          "example/uncatalogued",
				ResponseFormat: "example_format",
			},
			wantErr: "has no prompt",
		},
		{
			name:     "fails when the model has no response format and no catalogued default",
			settings: vlmPipelineSettings{apiKey: testVLMAPIKey, apiURL: testVLMAPIURL},
			pipeline: operatorv1alpha1.PipelineVLM,
			modelCfg: &operatorv1alpha1.VLMPipelineCustomConfig{
				Model:  "example/uncatalogued",
				Prompt: "example prompt",
			},
			wantErr: "has no response format",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			withVLMPipelineSettings(t, tt.settings)

			doclingCfg := &docling.DoclingConfig{Pipeline: tt.pipeline}
			err := injectVLMEngine(doclingCfg, tt.modelCfg)

			if tt.wantErr != "" {
				require.Error(t, err, "a vlm pipeline that cannot be built must fail, not skip")
				assert.Contains(t, err.Error(), tt.wantErr)
				assert.Nil(t, doclingCfg.VLMPipelineCustomConfig,
					"nothing should be injected on the failure path")
				return
			}
			require.NoError(t, err)

			if !tt.wantInjected {
				assert.Nil(t, doclingCfg.VLMPipelineCustomConfig,
					"expected the wire config to be left untouched")
				return
			}

			customConfig := doclingCfg.VLMPipelineCustomConfig
			require.NotNil(t, customConfig, "expected the engine config to be set")

			// Address and credential come from the controller.
			assert.Equal(t, testVLMAPIURL, customConfig.EngineOptions.URL)
			assert.Equal(t, docling.VLMEngineTypeAPI, customConfig.EngineOptions.EngineType,
				"docling cannot resolve the engine without this discriminator")
			if tt.wantAuth {
				assert.Equal(t, "Bearer "+testVLMAPIKey, customConfig.EngineOptions.Headers["Authorization"],
					"the model gateway expects a Bearer token, unlike the docling gateway")
			} else {
				assert.NotContains(t, customConfig.EngineOptions.Headers, "Authorization",
					"an empty Bearer token is worse than no header")
			}

			// Everything that changes the parse comes from the spec.
			assert.Equal(t, tt.modelCfg.Model, customConfig.ModelSpec.DefaultRepoID)
			assert.Equal(t, tt.modelCfg.Prompt, customConfig.ModelSpec.Prompt)
			assert.Equal(t, tt.modelCfg.ResponseFormat, customConfig.ModelSpec.ResponseFormat)
			assert.NotEmpty(t, customConfig.ModelSpec.Name, "docling requires a non-empty name")
			assert.Equal(t, tt.modelCfg.MaxSize, customConfig.MaxSize)
			require.NotNil(t, customConfig.Scale)
			assert.InDelta(t, 2.78, *customConfig.Scale, 0.001)
		})
	}
}

// An absent or unparseable scale must be omitted rather than sent as zero,
// which would rasterise pages at nothing.
func TestInjectVLMEngineOmitsAnUnsetScale(t *testing.T) {
	withVLMPipelineSettings(t, vlmPipelineSettings{apiKey: testVLMAPIKey, apiURL: testVLMAPIURL})

	doclingCfg := &docling.DoclingConfig{Pipeline: operatorv1alpha1.PipelineVLM}
	require.NoError(t, injectVLMEngine(doclingCfg, &operatorv1alpha1.VLMPipelineCustomConfig{
		Model:          "example/model",
		Prompt:         "example prompt",
		ResponseFormat: "example_format",
	}))

	require.NotNil(t, doclingCfg.VLMPipelineCustomConfig)
	assert.Nil(t, doclingCfg.VLMPipelineCustomConfig.Scale)
}

// The credential is only safe to inject because it goes into the wire struct
// and never into the CR spec, which is stored verbatim in
// ConvertedFileMetadata and shipped to the destination bucket. The endpoint
// is asserted absent because it is not part of the conversion identity, so
// moving the service does not reconvert the corpus.
func TestInjectVLMEngineDoesNotLeakTheKeyIntoStoredMetadata(t *testing.T) {
	withVLMPipelineSettings(t, vlmPipelineSettings{apiKey: testVLMAPIKey, apiURL: testVLMAPIURL})

	documentProcessorCR := &operatorv1alpha1.DocumentProcessor{
		Spec: operatorv1alpha1.DocumentProcessorSpec{
			DocumentProcessorConfig: operatorv1alpha1.DocumentProcessorConfig{
				DoclingConfig: operatorv1alpha1.DoclingConfig{
					Pipeline: operatorv1alpha1.PipelineVLM,
				},
			},
		},
	}
	documentProcessorCR.Spec.DocumentProcessorConfig.SetDefaults()

	// Mirrors how Reconcile builds the wire config: a fresh struct derived
	// from the CR, never the CR's own config.
	crConfig := documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig
	doclingCfg := &docling.DoclingConfig{Pipeline: crConfig.Pipeline}
	require.NoError(t, injectVLMEngine(doclingCfg, crConfig.VLMPipelineCustomConfig))

	require.NotNil(t, doclingCfg.VLMPipelineCustomConfig,
		"precondition: the key should have been injected into the wire config")

	storedMetadata, err := json.Marshal(documentProcessorCR.Spec.DocumentProcessorConfig.DoclingConfig)
	require.NoError(t, err)
	assert.NotContains(t, string(storedMetadata), testVLMAPIKey,
		"the credential reached the CR config that is persisted into every artifact")
	assert.NotContains(t, string(storedMetadata), "Authorization",
		"an auth header reached the CR config that is persisted into every artifact")
	assert.NotContains(t, string(storedMetadata), testVLMAPIURL,
		"the endpoint reached the cache key, so moving the service would reconvert the corpus")
}

func TestVLMPipelineSettingsConfigured(t *testing.T) {
	tests := []struct {
		name     string
		settings *vlmPipelineSettings
		want     bool
	}{
		{name: "nil, before any ControllerConfig reconcile", settings: nil, want: false},
		{name: "empty", settings: &vlmPipelineSettings{}, want: false},
		{name: "endpoint only", settings: &vlmPipelineSettings{apiURL: testVLMAPIURL}, want: true},
		{name: "credential only", settings: &vlmPipelineSettings{apiKey: testVLMAPIKey}, want: false},
		{
			name:     "both",
			settings: &vlmPipelineSettings{apiKey: testVLMAPIKey, apiURL: testVLMAPIURL},
			want:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, tt.settings.configured())
		})
	}
}

func TestStringValue(t *testing.T) {
	value := "  spaced  "
	assert.Equal(t, "", stringValue(nil))
	assert.Equal(t, "spaced", stringValue(&value))
}
