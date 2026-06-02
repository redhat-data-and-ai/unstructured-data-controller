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

package google

import (
	"context"
	"fmt"
	"strings"
	"time"

	cloudidentity "google.golang.org/api/cloudidentity/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// GetGroupMembers retrieves all transitive members of a Google group by email.
// It uses Cloud Identity API to expand nested group memberships.
func (c *Client) GetGroupMembers(
	ctx context.Context,
	groupEmail string,
) ([]string, error) {
	logger := log.FromContext(ctx).WithValues(
		"groupEmail", groupEmail,
	)

	start := time.Now()

	logger.V(1).Info("looking up Google group")
	group, err := c.cloudIdentityService.Groups.Lookup().
		Context(ctx).
		GroupKeyId(groupEmail).Do()
	if err != nil {
		logger.Error(err, "group lookup failed")
		return nil, fmt.Errorf(
			"failed to lookup group %s: %w", groupEmail, err)
	}
	groupKey := group.Name
	logger = logger.WithValues("groupKey", groupKey)

	var memberships []string

	searchCall := c.cloudIdentityService.Groups.Memberships.
		SearchTransitiveMemberships(groupKey)
	transitiveMembershipsResponse, err := searchCall.Context(ctx).Do()
	if err != nil {
		logger.Error(err, "failed to fetch transitive group members")
		return nil, fmt.Errorf(
			"failed to fetch members for group %s: %w", groupEmail, err)
	}

	memberships = append(memberships,
		getMembersFromMemberRelation(
			transitiveMembershipsResponse.Memberships)...)

	for transitiveMembershipsResponse.NextPageToken != "" {
		transitiveMembershipsResponse, err = searchCall.
			PageToken(transitiveMembershipsResponse.NextPageToken).
			Context(ctx).
			Do()
		if err != nil {
			logger.Error(err, "failed to fetch paginated transitive members")
			return nil, fmt.Errorf(
				"failed to fetch paginated members for group %s: %w",
				groupEmail, err)
		}
		memberships = append(memberships,
			getMembersFromMemberRelation(
				transitiveMembershipsResponse.Memberships)...)
	}

	logger.V(1).Info("Google Cloud Identity API call successful",
		"totalMembers", len(memberships),
		"timeElapsed", time.Since(start),
	)

	return memberships, nil
}

// getMembersFromMemberRelation extracts user IDs from Cloud Identity member relations.
func getMembersFromMemberRelation(
	memberRelations []*cloudidentity.MemberRelation,
) []string {
	result := make([]string, 0, len(memberRelations))
	for _, memberRelation := range memberRelations {
		preferredMemberKeys := memberRelation.PreferredMemberKey

		for _, preferredMemberKey := range preferredMemberKeys {
			// Extract the user ID part before the @ domain
			id := strings.Split(preferredMemberKey.Id, "@")[0]
			result = append(result, id)
		}
	}
	return result
}
