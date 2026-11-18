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

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"
)

// GetProjectMembers fetches all members of a GitLab project, including
// members inherited from parent groups, via GET /projects/:id/members/all.
// Only active members are included in the result.
func (c *Client) GetProjectMembers(ctx context.Context, repoURL string) ([]Permission, error) {
	logger := log.FromContext(ctx).WithValues("repoURL", repoURL)
	start := time.Now()

	projectPath, err := ProjectPathFromURL(repoURL)
	if err != nil {
		return nil, err
	}
	encodedPath := url.PathEscape(projectPath)

	var allPermissions []Permission
	page := 1

	for {
		endpoint := fmt.Sprintf("/projects/%s/members/all?per_page=100&page=%d", encodedPath, page)

		resp, err := c.doRequest(ctx, endpoint)
		if err != nil {
			return nil, fmt.Errorf("GitLab API request failed for %s: %w", repoURL, err)
		}

		body, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if err != nil {
			return nil, fmt.Errorf("failed to read GitLab API response for %s: %w", repoURL, err)
		}

		if resp.StatusCode != 200 {
			return nil, fmt.Errorf("GitLab API returned status %d for project %s: %s",
				resp.StatusCode, repoURL, string(body))
		}

		var members []gitlabMember
		if err := json.Unmarshal(body, &members); err != nil {
			return nil, fmt.Errorf("failed to parse GitLab members response for %s: %w", repoURL, err)
		}

		if len(members) == 0 {
			break
		}

		for _, member := range members {
			if member.State != "active" {
				continue
			}
			allPermissions = append(allPermissions, Permission{
				Type:  "user",
				Role:  accessLevelToRole(member.AccessLevel),
				UID:   member.Username,
				Email: member.Email,
			})
		}

		nextPage := resp.Header.Get("X-Next-Page")
		if nextPage == "" {
			break
		}
		page++
	}

	logger.Info("project members fetched",
		"memberCount", len(allPermissions),
		"timeElapsed", time.Since(start),
	)

	return allPermissions, nil
}

// ProjectPathFromURL extracts the GitLab project path from a repository URL.
// "https://gitlab.com/group/subgroup/project.git" returns "group/subgroup/project".
func ProjectPathFromURL(repoURL string) (string, error) {
	u, err := url.Parse(repoURL)
	if err != nil {
		return "", fmt.Errorf("invalid repo URL %q: %w", repoURL, err)
	}
	projectPath := strings.TrimPrefix(u.Path, "/")
	projectPath = strings.TrimSuffix(projectPath, "/")
	projectPath = strings.TrimSuffix(projectPath, ".git")
	if projectPath == "" {
		return "", fmt.Errorf("could not extract project path from URL %q", repoURL)
	}
	return projectPath, nil
}
