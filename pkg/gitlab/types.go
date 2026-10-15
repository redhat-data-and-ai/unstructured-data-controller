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

package gitlab

// Permission represents a resolved GitLab project member's access.
type Permission struct {
	Type  string `json:"type"`            // always "user" for GitLab members
	Role  string `json:"role"`            // guest, reporter, developer, maintainer, owner
	UID   string `json:"uid"`             // GitLab username
	Email string `json:"email,omitempty"` // only available with admin-scoped tokens
}

// gitlabMember represents a single entry from the GitLab Members API response.
type gitlabMember struct {
	ID          int    `json:"id"`
	Username    string `json:"username"`
	Name        string `json:"name"`
	State       string `json:"state"`
	AccessLevel int    `json:"access_level"`
	Email       string `json:"email,omitempty"`
}

// accessLevelToRole maps GitLab numeric access levels to human-readable roles.
// https://docs.gitlab.com/ee/api/access_requests.html
func accessLevelToRole(level int) string {
	switch {
	case level >= 50:
		return "owner"
	case level >= 40:
		return "maintainer"
	case level >= 30:
		return "developer"
	case level >= 20:
		return "reporter"
	default:
		return "guest"
	}
}
