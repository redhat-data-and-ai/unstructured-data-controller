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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	operatorv1alpha1 "github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

func TestExtractGDriveFolderID(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		want    string
		wantErr bool
	}{
		{
			name:  "standard folder URL",
			input: "https://drive.google.com/drive/folders/1ABCdef_example_folder_id",
			want:  "1ABCdef_example_folder_id",
		},
		{
			name:  "URL with user path segment",
			input: "https://drive.google.com/drive/u/0/folders/1ABCdef_example_folder_id",
			want:  "1ABCdef_example_folder_id",
		},
		{
			name:  "URL with query parameters",
			input: "https://drive.google.com/drive/folders/1ABCdef_example_folder_id?usp=sharing",
			want:  "1ABCdef_example_folder_id",
		},
		{
			name:  "URL with trailing slash",
			input: "https://drive.google.com/drive/folders/1ABCdef_example_folder_id/",
			want:  "1ABCdef_example_folder_id",
		},
		{
			name:  "raw folder ID",
			input: "1ABCdef_example_folder_id",
			want:  "1ABCdef_example_folder_id",
		},
		{
			name:    "URL without folders segment",
			input:   "https://drive.google.com/drive/some/other/path",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := extractGDriveFolderID(tt.input)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBuildGDriveStatus(t *testing.T) {
	tests := []struct {
		name   string
		gds    *unstructured.GDriveSource
		config *operatorv1alpha1.GoogleDriveConfig
		want   []operatorv1alpha1.GDriveFolderStatus
	}{
		{
			name: "all folders successful",
			gds: &unstructured.GDriveSource{
				FolderIDs:         []string{"id1", "id2"},
				FailedRootFolders: nil,
			},
			config: &operatorv1alpha1.GoogleDriveConfig{
				Folders: []operatorv1alpha1.GoogleDriveFolders{
					{Name: "docs", URL: "https://drive.google.com/drive/folders/id1"},
					{Name: "reports", URL: "https://drive.google.com/drive/folders/id2"},
				},
			},
			want: []operatorv1alpha1.GDriveFolderStatus{
				{Name: "docs", URL: "https://drive.google.com/drive/folders/id1"},
				{Name: "reports", URL: "https://drive.google.com/drive/folders/id2"},
			},
		},
		{
			name: "one folder failed",
			gds: &unstructured.GDriveSource{
				FolderIDs: []string{"id1", "id2"},
				FailedRootFolders: []unstructured.FailedRootFolder{
					{FolderID: "id2", Error: "access denied"},
				},
			},
			config: &operatorv1alpha1.GoogleDriveConfig{
				Folders: []operatorv1alpha1.GoogleDriveFolders{
					{Name: "docs", URL: "https://drive.google.com/drive/folders/id1"},
					{Name: "reports", URL: "https://drive.google.com/drive/folders/id2"},
				},
			},
			want: []operatorv1alpha1.GDriveFolderStatus{
				{Name: "docs", URL: "https://drive.google.com/drive/folders/id1"},
				{Name: "reports", URL: "https://drive.google.com/drive/folders/id2", Error: "access denied"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := buildGDriveStatus(tt.gds, tt.config)
			assert.Equal(t, tt.want, got)
		})
	}
}
