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

import (
	"context"
	"fmt"
	"strings"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/cache"
)

// GetFilePermissions retrieves all permissions for a given file ID.
// It filters out service accounts, expands Google group memberships,
// and resolves user identities via LDAP when configured.
// Returns permissions, a list of warning messages (if any), and an error.
func (c *Client) GetFilePermissions(
	ctx context.Context,
	fileID string,
	maxRetries int,
) ([]Permission, []string, error) {
	logger := log.FromContext(ctx).WithValues("fileId", fileID)
	start := time.Now()

	logger.Info("processing file permissions")

	permissions, err := c.googleClient.GetFilePermissions(
		ctx, fileID, maxRetries)
	if err != nil {
		logger.Error(err, "failed to fetch file permissions from Drive API")
		return nil, nil, err
	}

	permissionsList := make([]Permission, 0, len(permissions))
	var warnings []string
	skippedServiceAccounts := 0

	for _, p := range permissions {
		switch p.Type {
		case "user":
			if strings.Contains(
				p.EmailAddress, "iam.gserviceaccount.com") {
				skippedServiceAccounts++
				continue
			}
			uid := p.DisplayName

			// Check cache first, then fall back to LDAP lookup
			userCacheKey := fmt.Sprintf("ldap_user_%s", p.EmailAddress)
			userCacheValue, err := c.cacheClient.Get(
				ctx, userCacheKey)
			if err == nil {
				// Cache hit
				if s, ok := userCacheValue.(string); ok {
					uid = s
				}
				logger.V(1).Info("user LDAP data found in cache",
					"email", p.EmailAddress,
				)
			} else {
				// Cache miss - query LDAP
				userData, err := c.ldapClient.GetUserByEmail(
					ctx, p.EmailAddress)
				if err == nil {
					if u, ok := userData["uid"].(string); ok {
						uid = u
					}
					logger.V(1).Info("user LDAP data fetched and cached",
						"email", p.EmailAddress,
						"uid", uid,
					)
				} else {
					logger.V(1).Info("LDAP lookup failed, using display name",
						"email", p.EmailAddress,
						"error", err.Error(),
					)
				}
				// Update cache regardless of LDAP success
				_ = c.cacheClient.Set(
					ctx, userCacheKey, uid, cache.NoExpiration)
			}

			permission := Permission{
				Type: p.Type,
				Role: p.Role,
				UID:  uid,
			}
			permissionsList = append(permissionsList, permission)

		case "group":
			if p.EmailAddress == "" {
				msg := "group permission with empty email address"
				logger.Info("skipping " + msg)
				warnings = append(warnings, msg)
				continue
			}

			var groupPermissions []Permission
			var groupMembers []string
			cacheHit := false

			groupCacheKey := fmt.Sprintf(
				"google_group_%s", p.EmailAddress)
			groupCacheValue, err := c.cacheClient.Get(
				ctx, groupCacheKey)
			if err == nil {
				groupMembersRes, ok := groupCacheValue.(*[]string)
				if ok {
					groupMembers = *groupMembersRes
					cacheHit = true
				}
			} else {
				// Use singleflight to ensure only one API call per group
				result, err, shared := c.groupFlight.Do(
					groupCacheKey, func() (any, error) {
						// Double-check cache after acquiring lock
						if cachedValue, err := c.cacheClient.Get(
							ctx, groupCacheKey); err == nil {
							if members, ok := cachedValue.(*[]string); ok {
								return *members, nil
							}
						}

						// Fetch from API
						members, err := c.googleClient.GetGroupMembers(
							ctx, p.EmailAddress)
						if err != nil {
							return nil, err
						}

						// Store in cache
						_ = c.cacheClient.Set(
							ctx, groupCacheKey, &members,
							cache.NoExpiration)
						return members, nil
					})

				if err != nil {
					if strings.Contains(
						p.EmailAddress, "hangouts-chat") {
						msg := fmt.Sprintf(
							"hangouts-chat group skipped: %s",
							p.EmailAddress)
						logger.Info(msg,
							"groupEmail", p.EmailAddress,
						)
						warnings = append(warnings, msg)
						continue
					}

					logger.Error(err, "failed to get group members",
						"groupEmail", p.EmailAddress,
					)

					return nil, warnings, err
				}
				if members, ok := result.([]string); ok {
					groupMembers = members
				}

				if shared {
					logger.V(1).Info(
						"group members request deduplicated "+
							"via singleflight",
						"groupEmail", p.EmailAddress,
					)
				}
			}

			source := "API"
			if cacheHit {
				source = "cache"
			}
			logger.Info("expanded group permissions",
				"groupEmail", p.EmailAddress,
				"memberCount", len(groupMembers),
				"source", source,
			)

			groupPermissions = make(
				[]Permission, 0, len(groupMembers))
			for _, member := range groupMembers {
				permission := Permission{
					Type: "user",
					Role: p.Role,
					UID:  member,
				}
				groupPermissions = append(
					groupPermissions, permission)
			}
			permissionsList = append(
				permissionsList, groupPermissions...)

		case "domain":
			permission := Permission{
				Type:   p.Type,
				Role:   p.Role,
				Domain: p.Domain,
			}
			permissionsList = append(permissionsList, permission)

		default:
			logger.Info("unknown permission type encountered",
				"permissionType", p.Type,
			)
		}
	}

	logger.Info("file permissions processed successfully",
		"totalPermissions", len(permissionsList),
		"rawPermissions", len(permissions),
		"skippedServiceAccounts", skippedServiceAccounts,
		"warnings", len(warnings),
		"timeElapsed", time.Since(start),
	)

	return permissionsList, warnings, nil
}
