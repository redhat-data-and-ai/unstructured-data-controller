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

import "testing"

// TestStageCRName pins the naming convention: the pipeline controller creates
// stage CRs under these names, and every stage controller finds its upstream
// by building the same one.
func TestStageCRName(t *testing.T) {
	tests := []struct {
		name         string
		pipelineName string
		stageName    string
		want         string
	}{
		{name: "joins pipeline and stage with a hyphen", pipelineName: "finance", stageName: "convert", want: "finance-convert"},
		{name: "hyphens inside either part are kept", pipelineName: "q3-reports", stageName: "embed-v2", want: "q3-reports-embed-v2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := StageCRName(test.pipelineName, test.stageName); got != test.want {
				t.Errorf("StageCRName(%q, %q) = %q, want %q", test.pipelineName, test.stageName, got, test.want)
			}
		})
	}
}
