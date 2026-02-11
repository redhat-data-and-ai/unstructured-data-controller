package sfclienthandler

import "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"

// SnowflakeClientDetails contains the client details
type SnowflakeClientDetails struct {
	Client         *snowflake.Client
	SfClientConfig *snowflake.ClientConfig
}

// SnowflakeClients map with env name as key and client details as value
type SnowflakeClients map[string]*SnowflakeClientDetails
