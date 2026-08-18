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
	"fmt"
	"net"
	"sync"
	"time"

	ldap "github.com/go-ldap/ldap/v3"
)

// Config holds configuration for connecting to an LDAP server.
type Config struct {
	// Server is the LDAP server URL (e.g., "ldap://ldap.example.com:389").
	Server string `yaml:"server" json:"server"`
	// GroupDN is the base DN for group searches.
	GroupDN string `yaml:"groupDN" json:"groupDN"`
	// UserDN is the DN template for user lookups (must contain %s for the user ID).
	UserDN string `yaml:"userDN" json:"userDN"`
	// BaseUserDN is the base DN for user searches.
	BaseUserDN string `yaml:"baseUserDN" json:"baseUserDN"`
	// UserSearchFilter is the LDAP filter for user searches (e.g., "(objectClass=person)").
	UserSearchFilter string `yaml:"userSearchFilter" json:"userSearchFilter"`
	// EmailAttribute is the LDAP attribute containing user email addresses (e.g., "mail").
	EmailAttribute string `yaml:"emailAttribute" json:"emailAttribute"`
	// Attributes is the list of LDAP attributes to retrieve.
	Attributes []string `yaml:"attributes" json:"attributes"`

	BindUserName string `yaml:"bindUserName" json:"bindUserName"`
	BindPassword string `yaml:"bindPassword" json:"bindPassword"`
}

// DefaultConfig returns a Config with generic LDAP defaults.
func DefaultConfig() Config {
	return Config{
		UserSearchFilter: "(objectClass=person)",
		EmailAttribute:   "mail",
		Attributes:       []string{"uid", "mail"},
	}
}

// LDAPConnClient abstracts the underlying LDAP connection for testability.
type LDAPConnClient interface {
	IsClosing() bool
	Search(*ldap.SearchRequest) (*ldap.SearchResult, error)
	Bind(username, password string) error
	UnauthenticatedBind(username string) error
}

// LDAPConn manages an LDAP connection and provides query methods.
type LDAPConn struct {
	mu               sync.Mutex
	conn             LDAPConnClient
	userDN           string
	groupDN          string
	baseUserDN       string
	server           string
	userSearchFilter string
	emailAttribute   string
	attributes       []string
	bindUserName     string
	bindPassword     string
}

// Client defines the interface for LDAP operations used by the gdrive package.
type Client interface {
	GetGroupData(ctx context.Context, groupDN string) ([]SyncUser, error)
	GetUserByEmail(ctx context.Context, email string) (map[string]any, error)
	GetUserByID(ctx context.Context, userID string) (map[string]any, error)
}

// InitLDAP initializes a connection to the LDAP server using the provided configuration.
func InitLDAP(config Config) (Client, error) {
	ldapConn, err := ldap.DialURL(config.Server, ldap.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to LDAP server %s: %w", config.Server, err)
	}

	if config.BindUserName != "" && config.BindPassword != "" {
		err = ldapConn.Bind(config.BindUserName, config.BindPassword)
		if err != nil {
			_ = ldapConn.Close()
			return nil, fmt.Errorf("failed to bind LDAP connection: %w", err)
		}
	} else {
		// Perform anonymous bind
		err = ldapConn.UnauthenticatedBind("")
		if err != nil {
			_ = ldapConn.Close()
			return nil, fmt.Errorf("failed to bind LDAP connection: %w", err)
		}
	}

	emailAttr := config.EmailAttribute
	if emailAttr == "" {
		emailAttr = "mail"
	}

	return &LDAPConn{
		conn:             ldapConn,
		server:           config.Server,
		userDN:           config.UserDN,
		groupDN:          config.GroupDN,
		baseUserDN:       config.BaseUserDN,
		userSearchFilter: config.UserSearchFilter,
		emailAttribute:   emailAttr,
		attributes:       config.Attributes,
		bindUserName:     config.BindUserName,
		bindPassword:     config.BindPassword,
	}, nil
}

// getConn returns the underlying LDAP connection, re-establishing it if necessary.
func (l *LDAPConn) getConn() LDAPConnClient {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.conn != nil && l.conn.IsClosing() {
		newConn, err := ldap.DialURL(l.server, ldap.DialWithDialer(&net.Dialer{Timeout: 5 * time.Second}))
		if err != nil {
			return nil
		}

		if l.bindUserName != "" && l.bindPassword != "" {
			err = newConn.Bind(l.bindUserName, l.bindPassword)
			if err != nil {
				_ = newConn.Close()
				return nil
			}
		} else {
			err = newConn.UnauthenticatedBind("")
			if err != nil {
				_ = newConn.Close()
				return nil
			}
		}
		l.conn = newConn
	}
	return l.conn
}

// GetUserDN returns the user DN template.
func (l *LDAPConn) GetUserDN() string {
	return l.userDN
}

// GetGroupDN returns the group DN.
func (l *LDAPConn) GetGroupDN() string {
	return l.groupDN
}

// GetBaseUserDN returns the base user DN.
func (l *LDAPConn) GetBaseUserDN() string {
	return l.baseUserDN
}
