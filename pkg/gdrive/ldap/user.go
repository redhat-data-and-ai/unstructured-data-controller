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

var (
	ErrNoUserFound = errors.New("no LDAP entries found for user")
)

// parseLDAPEntry extracts configured attribute values from an LDAP entry.
func (l *LDAPConn) parseLDAPEntry(entry *ldap.Entry) map[string]any {
	userData := make(map[string]any)
	for _, attr := range l.attributes {
		if len(entry.GetAttributeValues(attr)) > 0 {
			userData[attr] = entry.GetAttributeValue(attr)
		} else {
			userData[attr] = ""
		}
	}
	return userData
}

// executeSearch executes an LDAP search request and returns parsed results.
func (l *LDAPConn) executeSearch(_ context.Context, searchRequest *ldap.SearchRequest) (map[string]any, error) {
	conn := l.getConn()
	if conn == nil {
		return nil, errors.New("LDAP connection is nil")
	}

	resp, err := conn.Search(searchRequest)
	if err != nil {
		var ldapErr *ldap.Error
		if errors.As(err, &ldapErr) {
			if ldapErr.ResultCode == ldap.LDAPResultNoSuchObject {
				return nil, ErrNoUserFound
			}
		}
		return nil, err
	}

	if len(resp.Entries) == 0 {
		return nil, ErrNoUserFound
	}

	return l.parseLDAPEntry(resp.Entries[0]), nil
}

// GetUserByID retrieves user data from LDAP using a user ID.
// It constructs the user DN from the configured template and performs a base object search.
func (l *LDAPConn) GetUserByID(ctx context.Context, userID string) (map[string]any, error) {
	filter := fmt.Sprintf("(%s)", l.userSearchFilter)

	searchRequest := ldap.NewSearchRequest(
		fmt.Sprintf(l.userDN, ldap.EscapeFilter(userID)),
		ldap.ScopeBaseObject, ldap.NeverDerefAliases, 0, 0, false,
		filter,
		l.attributes,
		nil,
	)

	return l.executeSearch(ctx, searchRequest)
}

// GetUserByEmail retrieves user data from LDAP using an email address.
// It searches the base user DN subtree using the configured email attribute.
func (l *LDAPConn) GetUserByEmail(ctx context.Context, email string) (map[string]any, error) {
	emailFilter := fmt.Sprintf("(%s=%s)", l.emailAttribute, ldap.EscapeFilter(email))
	filter := fmt.Sprintf("(&%s%s)", l.userSearchFilter, emailFilter)

	searchRequest := ldap.NewSearchRequest(
		l.GetBaseUserDN(),
		ldap.ScopeWholeSubtree, ldap.NeverDerefAliases, 0, 0, false,
		filter,
		l.attributes,
		nil,
	)

	return l.executeSearch(ctx, searchRequest)
}
