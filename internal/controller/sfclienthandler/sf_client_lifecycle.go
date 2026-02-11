package sfclienthandler

import (
	"fmt"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
)

const (
	ErrStrSfClientNotFound = "snowflake client not found for %s"
)

var (
	SfClients = make(SnowflakeClients)
)

func (s SnowflakeClients) GetClientFor(name string) (*snowflake.Client, error) {
	if client, exists := s[name]; exists {
		return client.Client, nil
	}
	return nil, fmt.Errorf(ErrStrSfClientNotFound, name)
}
