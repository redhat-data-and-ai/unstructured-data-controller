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

package ldap

import (
	"context"
	"errors"
	"fmt"

	ldap "github.com/go-ldap/ldap/v3"
)

// SyncUser represents a user resolved from an LDAP group membership.
type SyncUser struct {
	UID string
}

// GetGroupData retrieves all members of an LDAP group by its common name.
func (l *LDAPConn) GetGroupData(_ context.Context, groupName string) ([]SyncUser, error) {
	conn := l.getConn()
	if conn == nil {
		return nil, errors.New("failed to get LDAP connection")
	}

	searchRequest := ldap.NewSearchRequest(
		l.groupDN,
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		fmt.Sprintf("(cn=%s)", ldap.EscapeFilter(groupName)),
		[]string{"dn", "uniqueMember", "cn"}, nil,
	)

	searchResult, err := conn.Search(searchRequest)
	if err != nil {
		return nil, fmt.Errorf("failed to search for group data: %w", err)
	}

	if len(searchResult.Entries) != 1 {
		return nil, fmt.Errorf("expected one group match, got %d", len(searchResult.Entries))
	}

	entry := searchResult.Entries[0]

	syncUsers := make([]SyncUser, 0, len(entry.GetAttributeValues("uniqueMember")))
	for _, member := range entry.GetAttributeValues("uniqueMember") {
		parsedMember, err := ldap.ParseDN(member)
		if err != nil {
			return nil, fmt.Errorf("failed to parse LDAP DN: %w", err)
		}
		syncUsers = append(syncUsers, SyncUser{
			UID: parsedMember.RDNs[0].Attributes[0].Value,
		})
	}
	return syncUsers, nil
}
