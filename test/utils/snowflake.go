package utils

import (
	"crypto/rsa"
	"encoding/base64"
	"log"

	"github.com/redhat-data-and-ai/unstructured-data-controller/api/v1alpha1"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/snowflake"
	"github.com/snowflakedb/gosnowflake"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/util/keyutil"
)

func NewClientConfigFromSnowflakeSpec(spec *v1alpha1.SnowflakeConfig, secret *corev1.Secret) *snowflake.ClientConfig {
	if secret == nil {
		return nil
	}
	clientConfig := snowflake.ClientConfig{}
	clientConfig.Account = spec.Account
	clientConfig.User = spec.User
	clientConfig.Role = spec.Role
	clientConfig.Warehouse = spec.Warehouse
	clientConfig.Region = spec.Region
	privateKeyData := secret.Data["SNOWFLAKE_PRIVATE_KEY"]
	if len(privateKeyData) == 0 {
		return nil
	}
	// Decode the private key from base64
	decoded, err := base64.StdEncoding.DecodeString(string(privateKeyData))
	if err != nil {
		decoded = privateKeyData
	}
	privateKeyInterface, err := keyutil.ParsePrivateKeyPEM(decoded)
	if err != nil {
		log.Println("failed to parse private key", err)
		return nil
	}
	privateKey, ok := privateKeyInterface.(*rsa.PrivateKey)
	if !ok {
		log.Println("private key is not an RSA private key")
		return nil
	}
	clientConfig.Authenticator = gosnowflake.AuthTypeJwt
	clientConfig.PrivateKey = privateKey
	return &clientConfig
}

type StageFile struct {
	FileName string `db:"name"`
}
