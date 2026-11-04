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
	"os"
	"path/filepath"
	"strings"
	"testing"
	"unicode"
)

// TestKubebuilderMarkersAreASCII catches a curly quote in a kubebuilder
// marker, which is otherwise expensive to find.
//
// A marker lives in a comment, so a smart quote in one compiles, vets, lints
// and unit tests clean. In a CEL rule it then fails at CRD install with
// "Syntax error: token recognition error", which is the slowest possible
// place to learn about it: the e2e job, ten minutes later, and that job
// reports only "exit status 2" because it does not keep kubectl's output.
//
// It happens because editors and chat tools substitute ' and " for their
// typographic forms, and Go has no reason to complain.
func TestKubebuilderMarkersAreASCII(t *testing.T) {
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatalf("reading the package directory: %v", err)
	}

	for _, entry := range entries {
		name := entry.Name()
		if filepath.Ext(name) != ".go" {
			continue
		}

		content, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("reading %s: %v", name, err)
		}

		for i, line := range strings.Split(string(content), "\n") {
			if !strings.Contains(line, "+kubebuilder:") {
				continue
			}
			for _, r := range line {
				if r > unicode.MaxASCII {
					t.Errorf("%s:%d: kubebuilder marker contains the non-ASCII character %q (%U). "+
						"Quotes in markers must be ' and \", not their typographic forms, "+
						"or the generated CRD is rejected at install time.\n\t%s",
						name, i+1, r, r, strings.TrimSpace(line))
					break
				}
			}
		}
	}
}
