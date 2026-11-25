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

package controllerutils

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"k8s.io/apimachinery/pkg/runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

// operatorScheme returns a scheme with the operator's types, as main registers them.
func operatorScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	scheme := runtime.NewScheme()
	require.NoError(t, operatorv1alpha1.AddToScheme(scheme))
	return scheme
}

// lookupKeys maps each controller's For() kind to the GroupKindConcurrency key
// controller-runtime looks it up under, derived the same way the builder does.
func lookupKeys(t *testing.T, scheme *runtime.Scheme) map[string]string {
	t.Helper()
	objects := []client.Object{
		&operatorv1alpha1.ControllerConfig{},
		&operatorv1alpha1.UnstructuredDataPipeline{},
		&operatorv1alpha1.SourceCrawler{},
		&operatorv1alpha1.DocumentProcessor{},
		&operatorv1alpha1.ChunksGenerator{},
		&operatorv1alpha1.VectorEmbeddingsGenerator{},
		&operatorv1alpha1.DestinationSyncer{},
	}
	keys := make(map[string]string, len(objects))
	for _, object := range objects {
		gvk, err := apiutil.GVKForObject(object, scheme)
		require.NoError(t, err)
		keys[gvk.Kind] = gvk.GroupKind().String()
	}
	return keys
}

// clearOverrides empties every override, so one set in the test process's own
// environment cannot leak into an assertion.
func clearOverrides(t *testing.T, kinds map[string]string) {
	t.Helper()
	for kind := range kinds {
		t.Setenv(ConcurrencyEnvVar(kind), "")
	}
}

func TestConcurrencyEnvVar(t *testing.T) {
	tests := []struct {
		kind string
		want string
	}{
		// The six names deployments already set; changing any breaks them.
		{kind: "UnstructuredDataPipeline", want: "CONCURRENCY_UNSTRUCTURED_DATA_PIPELINE"},
		{kind: "DocumentProcessor", want: "CONCURRENCY_DOCUMENT_PROCESSOR"},
		{kind: "ChunksGenerator", want: "CONCURRENCY_CHUNKS_GENERATOR"},
		{kind: "VectorEmbeddingsGenerator", want: "CONCURRENCY_VECTOR_EMBEDDINGS_GENERATOR"},
		{kind: "SourceCrawler", want: "CONCURRENCY_SOURCE_CRAWLER"},
		{kind: "DestinationSyncer", want: "CONCURRENCY_DESTINATION_SYNCER"},
		{kind: "ControllerConfig", want: "CONCURRENCY_CONTROLLER_CONFIG"},
		{kind: "Pod", want: "CONCURRENCY_POD"},
		{kind: "HTTPRoute", want: "CONCURRENCY_HTTP_ROUTE"},
		{kind: "S3Bucket", want: "CONCURRENCY_S3_BUCKET"},
	}
	for _, test := range tests {
		t.Run(test.kind, func(t *testing.T) {
			assert.Equal(t, test.want, ConcurrencyEnvVar(test.kind))
		})
	}
}

func TestReconciledKindsAreExactlyTheOperatorObjects(t *testing.T) {
	scheme := operatorScheme(t)
	keys := lookupKeys(t, scheme)
	want := make([]string, 0, len(keys))
	for kind := range keys {
		want = append(want, kind)
	}

	// The scheme also registers list kinds and metav1 option types for the
	// group version; none of them is reconciled.
	assert.ElementsMatch(t, want, reconciledKinds(scheme))
}

func TestConcurrencyOverrides(t *testing.T) {
	scheme := operatorScheme(t)
	keys := lookupKeys(t, scheme)

	t.Run("no overrides set leaves every kind on the global default", func(t *testing.T) {
		clearOverrides(t, keys)
		assert.Empty(t, ConcurrencyOverrides(scheme))
	})

	t.Run("an invalid override is ignored", func(t *testing.T) {
		clearOverrides(t, keys)
		t.Setenv(ConcurrencyEnvVar("DocumentProcessor"), "ten")
		assert.Empty(t, ConcurrencyOverrides(scheme))
	})

	for kind, key := range keys {
		t.Run("override for "+kind+" lands under its lookup key only", func(t *testing.T) {
			clearOverrides(t, keys)
			t.Setenv(ConcurrencyEnvVar(kind), "17")
			assert.Equal(t, map[string]int{key: 17}, ConcurrencyOverrides(scheme))
		})
	}
}

func TestConcurrencyFromEnv(t *testing.T) {
	tests := []struct {
		name    string
		value   string
		set     bool
		want    int
		wantSet bool
	}{
		{name: "unset is no override", set: false},
		{name: "empty is no override", value: "", set: true},
		{name: "positive integer is used", value: "10", set: true, want: 10, wantSet: true},
		{name: "one is a valid override", value: "1", set: true, want: 1, wantSet: true},
		{name: "zero is ignored", value: "0", set: true},
		{name: "negative is ignored", value: "-1", set: true},
		{name: "non-numeric is ignored", value: "ten", set: true},
		{name: "float is ignored", value: "3.5", set: true},
		{name: "surrounding whitespace is ignored", value: " 10", set: true},
	}
	const envVar = "TEST_CONCURRENCY_FROM_ENV"
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.set {
				t.Setenv(envVar, test.value)
			}
			got, ok := concurrencyFromEnv(envVar)
			assert.Equal(t, test.wantSet, ok, "override reported")
			assert.Equal(t, test.want, got)
		})
	}
}
