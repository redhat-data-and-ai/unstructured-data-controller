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
)

func TestConcurrencyOverride(t *testing.T) {
	tests := []struct {
		name  string
		value string
		set   bool
		want  int
	}{
		{name: "unset falls back to one worker", set: false, want: 0},
		{name: "empty falls back to one worker", value: "", set: true, want: 0},
		{name: "positive integer is used", value: "10", set: true, want: 10},
		{name: "one is a valid override", value: "1", set: true, want: 1},
		{name: "zero falls back to one worker", value: "0", set: true, want: 0},
		{name: "negative falls back to one worker", value: "-1", set: true, want: 0},
		{name: "non-numeric falls back to one worker", value: "ten", set: true, want: 0},
		{name: "float falls back to one worker", value: "3.5", set: true, want: 0},
		{name: "surrounding whitespace falls back to one worker", value: " 10", set: true, want: 0},
	}

	const envVar = "TEST_CONCURRENCY_OVERRIDE"
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.set {
				t.Setenv(envVar, test.value)
			}
			if got := ConcurrencyOverride(envVar); got != test.want {
				t.Errorf("ConcurrencyOverride(%q=%q) = %d, want %d", envVar, test.value, got, test.want)
			}
		})
	}
}
