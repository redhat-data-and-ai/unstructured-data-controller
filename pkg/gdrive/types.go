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

package gdrive

// Permission represents a single permission entry on a file.
type Permission struct {
	Type   string `json:"type"`
	Role   string `json:"role"`
	UID    string `json:"uid"`
	Domain string `json:"domain"`
	Email  string `json:"email,omitempty"`
}

// PermissionResponse wraps permissions for a single file.
type PermissionResponse struct {
	FileID      string       `json:"file_id"`
	Permissions []Permission `json:"permissions"`
}

// FileIDs is the input format for file-based permission processing.
type FileIDs struct {
	FileIDs []string `json:"file_ids"`
}

// FolderIDs is the input format for folder-based crawling.
type FolderIDs struct {
	FolderIDs []string `json:"folder_ids"`
}

// CrawlRecord represents one file discovered during folder crawling.
type CrawlRecord struct {
	FileID           string `json:"file_id"`
	FileName         string `json:"file_name"`
	MimeType         string `json:"mime_type"`
	CreatedAt        string `json:"created_at"`
	UpdatedAt        string `json:"updated_at"`
	Owner            string `json:"owner"`
	FileSize         int64  `json:"file_size"`
	RootFolderID     string `json:"root_folder_id"`
	ParentFolderID   string `json:"parent_folder_id"`
	ParentFolderName string `json:"parent_folder_name"`
	Status           string `json:"status"`
	Reason           string `json:"reason,omitempty"`
}

// InaccessibleItems collects all items that could not be accessed during crawling.
type InaccessibleItems struct {
	Folders               []InaccessibleFolder         `json:"inaccessible_folders"`
	Files                 []InaccessibleFile           `json:"inaccessible_files"`
	ShortcutTargetFolders []InaccessibleShortcutFolder `json:"inaccessible_shortcut_target_folders"`
	ShortcutTargetFiles   []InaccessibleShortcutFile   `json:"inaccessible_shortcut_target_files"`
}

// InaccessibleFolder represents a folder that could not be listed.
type InaccessibleFolder struct {
	FolderID     string `json:"folder_id"`
	FolderName   string `json:"folder_name"`
	RootFolderID string `json:"root_folder_id"`
}

// InaccessibleFile represents a file whose metadata could not be fetched.
type InaccessibleFile struct {
	FileID         string `json:"file_id"`
	ParentFolderID string `json:"parent_folder_id"`
	RootFolderID   string `json:"root_folder_id"`
}

// InaccessibleShortcutFolder represents a shortcut target folder that could not be accessed.
type InaccessibleShortcutFolder struct {
	ShortcutFileID string `json:"shortcut_file_id"`
	TargetFolderID string `json:"target_folder_id"`
	RootFolderID   string `json:"root_folder_id"`
}

// InaccessibleShortcutFile represents a shortcut target file that could not be accessed.
type InaccessibleShortcutFile struct {
	ShortcutFileID string `json:"shortcut_file_id"`
	TargetFileID   string `json:"target_file_id"`
	RootFolderID   string `json:"root_folder_id"`
}
