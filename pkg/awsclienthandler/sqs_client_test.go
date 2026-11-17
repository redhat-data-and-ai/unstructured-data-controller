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

package awsclienthandler

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewSQSClientFromConfigBuildsOnePerConfig guards against a shared client:
// one crawler's credentials and endpoint must never serve another.
func TestNewSQSClientFromConfigBuildsOnePerConfig(t *testing.T) {
	ctx := context.Background()
	first, err := NewSQSClientFromConfig(ctx, &AWSConfig{Region: "us-east-1", Endpoint: "http://sqs-a.invalid"})
	require.NoError(t, err)
	second, err := NewSQSClientFromConfig(ctx, &AWSConfig{Region: "eu-west-1", Endpoint: "http://sqs-b.invalid"})
	require.NoError(t, err)

	assert.NotSame(t, first, second)
	assert.Equal(t, "http://sqs-a.invalid", aws.ToString(first.Options().BaseEndpoint))
	assert.Equal(t, "http://sqs-b.invalid", aws.ToString(second.Options().BaseEndpoint))
	assert.Equal(t, "eu-west-1", second.Options().Region)
}

func TestNewSQSClientFromConfigRejectsANilConfig(t *testing.T) {
	_, err := NewSQSClientFromConfig(context.Background(), nil)
	require.EqualError(t, err, "cannot create an SQS client without an AWS config")
}
