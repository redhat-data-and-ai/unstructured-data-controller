package utils

import (
	"crypto/rsa"
	"os"

	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
	"github.com/snowflakedb/gosnowflake"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/keyutil"
)

func NewClientConfig(secret *corev1.Secret) *snowflake.ClientConfig {
	clientConfig := snowflake.ClientConfig{}
	clientConfig.Account = os.Getenv("SNOWFLAKE_ACCOUNT")
	clientConfig.User = os.Getenv("SNOWFLAKE_USER")
	clientConfig.Role = os.Getenv("SNOWFLAKE_ROLE")
	clientConfig.Warehouse = os.Getenv("SNOWFLAKE_WAREHOUSE")
	privateKeyData := secret.Data["privateKey"]
	privateKeyInterface, _ := keyutil.ParsePrivateKeyPEM(privateKeyData)
	privateKey, ok := privateKeyInterface.(*rsa.PrivateKey)
	if !ok {
		return nil
	}
	clientConfig.Authenticator = gosnowflake.AuthTypeJwt
	clientConfig.PrivateKey = privateKey
	clientConfig.ClientStoreTemporaryCredential = gosnowflake.ConfigBoolTrue
	return &clientConfig
}

func NewClientConfigFromSnowflakeSpec(spec *v1alpha1.SnowflakeConfig, secret *corev1.Secret) *snowflake.ClientConfig {
	if spec == nil || secret == nil {
		return nil
	}
	clientConfig := snowflake.ClientConfig{}
	clientConfig.Account = spec.Account
	clientConfig.User = spec.User
	clientConfig.Role = spec.Role
	clientConfig.Warehouse = spec.Warehouse
	clientConfig.Region = spec.Region
	privateKeyData := secret.Data["privateKey"]
	privateKeyInterface, _ := keyutil.ParsePrivateKeyPEM(privateKeyData)
	privateKey, ok := privateKeyInterface.(*rsa.PrivateKey)
	if !ok {
		return nil
	}
	clientConfig.Authenticator = gosnowflake.AuthTypeJwt
	clientConfig.PrivateKey = privateKey
	clientConfig.ClientStoreTemporaryCredential = gosnowflake.ConfigBoolTrue
	return &clientConfig
}

type StageFile struct {
	FileName string `db:"name"`
}
