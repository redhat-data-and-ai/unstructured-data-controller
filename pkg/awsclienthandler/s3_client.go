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
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/kms"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	S3Client      *s3.Client
	PresignClient *s3.PresignClient
)

// NewS3ClientFromConfig creates and returns an Amazon S3 client using the provided context and AWS configuration.
func NewS3ClientFromConfig(ctx context.Context, awsConfig *AWSConfig) (*s3.Client, error) {
	logger := log.FromContext(ctx)
	if S3Client != nil {
		return S3Client, nil
	}

	cfg, err := getAWSConfig(ctx, awsConfig)
	if err != nil {
		return nil, err
	}

	s3Options := func(o *s3.Options) {
		o.UsePathStyle = true
		if awsConfig.Endpoint != "" {
			o.BaseEndpoint = aws.String(awsConfig.Endpoint)
		}
	}
	S3Client = s3.NewFromConfig(cfg, s3Options)
	logger.Info("S3 client initialized ...")
	return S3Client, nil
}

// GetS3Client returns the initialized Amazon S3 client instance.
func GetS3Client() (*s3.Client, error) {
	if S3Client == nil {
		return nil, errors.New("S3 client not initialized yet")
	}
	return S3Client, nil
}

// NewPresignClient creates and returns an Amazon S3 presign client using the provided context and AWS configuration.
func NewPresignClient(ctx context.Context) (*s3.PresignClient, error) {
	logger := log.FromContext(ctx)
	if PresignClient != nil {
		return PresignClient, nil
	}

	// get the s3 client
	s3Client, err := GetS3Client()
	if err != nil {
		return nil, err
	}

	PresignClient = s3.NewPresignClient(s3Client)
	logger.Info("Presign client initialized ...")
	return PresignClient, nil
}

// GetPresignClient returns the initialized Amazon S3 presign client instance.
func GetPresignClient() (*s3.PresignClient, error) {
	if PresignClient == nil {
		return nil, errors.New("presign client not initialized yet")
	}
	return PresignClient, nil
}

func GetPresignedURL(ctx context.Context, bucketName, objectKey string) (string, error) {
	presignClient, err := GetPresignClient()
	if err != nil {
		return "", err
	}
	request, err := presignClient.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	}, func(options *s3.PresignOptions) {
		options.Expires = 60 * time.Minute
	})
	if err != nil {
		return "", err
	}
	return request.URL, nil
}

func GetBucketEncryptionKeyARN(ctx context.Context, bucketName string) (string, error) {
	s3Client, err := GetS3Client()
	if err != nil {
		return "", err
	}

	bucketEncryption, err := s3Client.GetBucketEncryption(ctx, &s3.GetBucketEncryptionInput{
		Bucket: aws.String(bucketName),
	})
	if err != nil {
		return "", err
	}

	if bucketEncryption.ServerSideEncryptionConfiguration == nil ||
		len(bucketEncryption.ServerSideEncryptionConfiguration.Rules) == 0 {
		// no encryption key is set for the bucket, that's OK, return empty string
		return "", nil
	}

	// we only support one rule for now
	kmsMasterKeyID := bucketEncryption.ServerSideEncryptionConfiguration.Rules[0].
		ApplyServerSideEncryptionByDefault.KMSMasterKeyID
	if kmsMasterKeyID == nil {
		// no KMS key is set for the bucket, that's OK, return empty string
		return "", nil
	}

	// now we need to get the ARN of the KMS key
	kmsClient, err := GetKMSClient()
	if err != nil {
		return "", err
	}

	kmsKey, err := kmsClient.DescribeKey(ctx, &kms.DescribeKeyInput{
		KeyId: kmsMasterKeyID,
	})
	if err != nil {
		return "", err
	}

	return *kmsKey.KeyMetadata.Arn, nil
}

// ApplyTagToObject applies a single tag to an object in an S3 bucket
func ApplyTagToObject(ctx context.Context, bucketName, objectKey, tagKey, tagValue string) error {
	logger := log.FromContext(ctx)
	s3Client, err := GetS3Client()
	if err != nil {
		return err
	}

	newTag := s3types.Tag{
		Key:   aws.String(tagKey),
		Value: aws.String(tagValue),
	}

	tagSet := []s3types.Tag{}
	// get existing tags first
	existingTags, err := s3Client.GetObjectTagging(ctx, &s3.GetObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		// if the object doesn't exist, ignore the error and proceed with empty tag set
		var noSuchKey *s3types.NoSuchKey
		if !errors.As(err, &noSuchKey) {
			return err
		}
		logger.Info("object does not exist", "bucketName", bucketName, "objectKey", objectKey)
	} else {
		tagSet = existingTags.TagSet
	}

	// if the tag already exists, update it
	tagExists := false
	for i, tag := range tagSet {
		if tag.Key == nil {
			continue
		}
		if *tag.Key == tagKey {
			logger.Info("tag already exists", "bucketName", bucketName, "objectKey",
				objectKey, "tagKey", tagKey, "tagValue", tagValue)
			tagSet[i] = newTag
			tagExists = true
			break
		}
	}

	// if the tag doesn't exist, add it
	if !tagExists {
		tagSet = append(tagSet, newTag)
	}

	// Put the combined tags
	logger.Info("putting tags", "bucketName", bucketName, "objectKey", objectKey, "tagKey", tagKey, "tagValue", tagValue)
	_, err = s3Client.PutObjectTagging(ctx, &s3.PutObjectTaggingInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
		Tagging: &s3types.Tagging{
			TagSet: tagSet,
		},
	})

	return err
}

func ObjectExists(ctx context.Context, bucketName, objectKey string) (bool, error) {
	s3Client, err := GetS3Client()
	if err != nil {
		return false, err
	}

	_, err = s3Client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
	if err != nil {
		var notFound *s3types.NotFound
		var noSuchKey *s3types.NoSuchKey
		if errors.As(err, &notFound) || errors.As(err, &noSuchKey) {
			return false, nil
		}
		return false, err
	}

	return true, nil
}

func GetObject(ctx context.Context, bucketName, objectKey string) (*s3.GetObjectOutput, error) {
	s3Client, err := GetS3Client()
	if err != nil {
		return nil, err
	}

	return s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(objectKey),
	})
}

func ListObjectsInPrefix(ctx context.Context, bucketName, prefix string) ([]s3types.Object, error) {
	s3Client, err := GetS3Client()
	if err != nil {
		return nil, err
	}

	listObjectPaginator := s3.NewListObjectsV2Paginator(s3Client, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucketName),
		Prefix: aws.String(prefix),
	})

	objects := []s3types.Object{}

	for listObjectPaginator.HasMorePages() {
		listOutput, err := listObjectPaginator.NextPage(ctx)
		if err != nil {
			return nil, err
		}
		objects = append(objects, listOutput.Contents...)
	}

	return objects, nil
}
