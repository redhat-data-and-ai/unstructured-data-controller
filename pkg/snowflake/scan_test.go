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

package snowflake

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDBValueString(t *testing.T) {
	tests := []struct {
		name  string
		value any
		want  string
	}{
		{name: "float score", value: 0.6131276457157876, want: "0.6131276457157876"},
		{name: "string", value: "chunk", want: "chunk"},
		{name: "byte slice", value: []byte("document"), want: "document"},
		{name: "integer", value: int64(42), want: "42"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, dbValueString(tt.value))
		})
	}
}
