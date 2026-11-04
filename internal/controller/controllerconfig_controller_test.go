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
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	corev1 "k8s.io/api/core/v1"
	"sigs.k8s.io/yaml"
)

const operatorSecretSamplePath = "../../config/samples/unstructured-secret.yaml"

// operatorSecretSample returns the operator-level Secret from the sample
// manifest. Anyone standing the controller up copies that file, so it is the
// de facto interface for every credential the controller reads.
func operatorSecretSample(t *testing.T) corev1.Secret {
	t.Helper()

	raw, err := os.ReadFile(operatorSecretSamplePath)
	require.NoError(t, err)

	// The sample holds the operator and pipeline Secrets in one file.
	for _, document := range strings.Split(string(raw), "\n---\n") {
		var secret corev1.Secret
		require.NoError(t, yaml.Unmarshal([]byte(document), &secret))
		if secret.Name == "operator-secret" {
			return secret
		}
	}

	t.Fatalf("no operator-secret document in %s", operatorSecretSamplePath)
	return corev1.Secret{}
}

// A credential the controller looks for under one name and the sample
// advertises under another is invisible: the settings stay unconfigured,
// injection is skipped, and every document converts on the standard pipeline
// while the stage reports success. Nothing else in the system would notice,
// which is why this is asserted rather than left to review.
func TestSampleSecretDeclaresTheKeysTheControllerReads(t *testing.T) {
	secret := operatorSecretSample(t)

	// Only the keys this package reads by name. Listing every key in the
	// sample would turn an unrelated addition into a failure here.
	for _, key := range []string{
		"DOCLING_USER_KEY",
		"VLM_API_KEY",
		vlmPipelineAPIKeySecretKey,
	} {
		assert.Contains(t, secret.StringData, key,
			"the controller reads this key, so the sample must declare it")
	}
}

// The reverse of the test above: a key in the sample that no code reads is
// just as misleading, because an operator who fills it in gets nothing and no
// error. This caught VLM_PIPELINE_API_KEY being documented as NEMOTRON_API_KEY.
func TestSampleSecretDeclaresNoUnreadVLMKey(t *testing.T) {
	secret := operatorSecretSample(t)

	// Ranging a nil map runs zero iterations, so without this the test passes
	// vacuously if the sample switches to the equally valid data form.
	require.NotEmpty(t, secret.StringData,
		"the sample must use stringData, or this test asserts nothing")

	for key := range secret.StringData {
		if !strings.Contains(key, "VLM") && !strings.Contains(key, "NEMOTRON") {
			continue
		}
		assert.Contains(t, []string{"VLM_API_KEY", vlmPipelineAPIKeySecretKey}, key,
			"the sample declares a model credential no code reads")
	}
}
