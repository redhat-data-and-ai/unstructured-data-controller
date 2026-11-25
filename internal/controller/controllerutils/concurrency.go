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
	"os"
	"reflect"
	"sort"
	"strconv"
	"unicode"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
)

const (
	// Default number of concurrent reconcile workers per controller.
	DefaultReconcilerConcurrency = 5

	concurrencyEnvPrefix = "CONCURRENCY_"
)

// ConcurrencyOverrides returns GroupKindConcurrency entries for the operator's
// kinds whose ConcurrencyEnvVar is set to a positive integer. Kinds without
// one are left out so they use the manager's global default.
func ConcurrencyOverrides(scheme *runtime.Scheme) map[string]int {
	overrides := map[string]int{}
	for _, kind := range reconciledKinds(scheme) {
		if workers, ok := concurrencyFromEnv(ConcurrencyEnvVar(kind)); ok {
			overrides[groupKind(kind)] = workers
		}
	}
	return overrides
}

// ConcurrencyEnvVar returns the variable that overrides kind's worker count,
// e.g. CONCURRENCY_DOCUMENT_PROCESSOR for DocumentProcessor.
func ConcurrencyEnvVar(kind string) string {
	runes := []rune(kind)
	name := []rune(concurrencyEnvPrefix)
	for i, current := range runes {
		// A word starts at an upper-case letter after a lower-case one, or at
		// the last capital of an acronym (HTTPRoute -> HTTP_ROUTE).
		if i > 0 && unicode.IsUpper(current) {
			previous := runes[i-1]
			nextIsLower := i+1 < len(runes) && unicode.IsLower(runes[i+1])
			if unicode.IsLower(previous) || unicode.IsDigit(previous) || (unicode.IsUpper(previous) && nextIsLower) {
				name = append(name, '_')
			}
		}
		name = append(name, unicode.ToUpper(current))
	}
	return string(name)
}

// reconciledKinds returns the object kinds scheme registers for the operator's
// group version, without the list kinds and the option types metav1 registers
// beside them.
func reconciledKinds(scheme *runtime.Scheme) []string {
	var kinds []string
	for kind, goType := range scheme.KnownTypes(operatorv1alpha1.GroupVersion) {
		instance := reflect.New(goType).Interface()
		if _, isObject := instance.(client.Object); !isObject {
			continue
		}
		if _, isList := instance.(client.ObjectList); isList {
			continue
		}
		kinds = append(kinds, kind)
	}
	sort.Strings(kinds)
	return kinds
}

// concurrencyFromEnv reads a worker count from envVar. It reports false when
// the variable is unset or not a positive integer.
func concurrencyFromEnv(envVar string) (int, bool) {
	value := os.Getenv(envVar)
	if value == "" {
		return 0, false
	}
	workers, err := strconv.Atoi(value)
	if err != nil || workers <= 0 {
		log.Log.Info("ignoring invalid concurrency env var, using default",
			"envVar", envVar, "value", value, "default", DefaultReconcilerConcurrency)
		return 0, false
	}
	return workers, true
}

// groupKind formats kind the way controller-runtime looks it up in GroupKindConcurrency.
func groupKind(kind string) string {
	return schema.GroupKind{Group: operatorv1alpha1.GroupVersion.Group, Kind: kind}.String()
}
