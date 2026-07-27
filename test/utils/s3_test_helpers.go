package utils

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	apimachinerywait "k8s.io/apimachinery/pkg/util/wait"
)

const (
	defaultS3PollInterval = 5 * time.Second
	defaultS3PollTimeout  = 10 * time.Minute
)

type S3ObjectMetadata struct {
	Key            string
	ChecksumSHA256 string
	LastModified   time.Time
}

// FindDestinationKey lists objects under bucket/prefix and returns the
// .json key whose path contains fileName (e.g. "pdflatex-4-pages.pdf").
func FindDestinationKey(ctx context.Context, client *s3.Client, bucket, prefix, fileName string) (string, error) {
	listOut, err := client.ListObjectsV2(ctx, &s3.ListObjectsV2Input{
		Bucket: aws.String(bucket),
		Prefix: aws.String(prefix),
	})
	if err != nil {
		return "", fmt.Errorf("list destination objects: %w", err)
	}

	for _, obj := range listOut.Contents {
		key := aws.ToString(obj.Key)
		if strings.Contains(key, fileName) && strings.HasSuffix(key, ".json") {
			return key, nil
		}
	}

	return "", fmt.Errorf("no destination object found for %q under %s/%s", fileName, bucket, prefix)
}

// GetS3ObjectMetadata HeadObjects an S3 key with checksum enabled.
func GetS3ObjectMetadata(ctx context.Context, client *s3.Client, bucket, key string) (S3ObjectMetadata, error) {
	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket:       aws.String(bucket),
		Key:          aws.String(key),
		ChecksumMode: types.ChecksumModeEnabled,
	})
	if err != nil {
		return S3ObjectMetadata{}, fmt.Errorf("HeadObject %q: %w", key, err)
	}
	if head.LastModified == nil {
		return S3ObjectMetadata{}, fmt.Errorf("HeadObject %q: LastModified is nil", key)
	}
	if head.ChecksumSHA256 == nil || *head.ChecksumSHA256 == "" {
		return S3ObjectMetadata{}, fmt.Errorf("HeadObject %q: ChecksumSHA256 is missing", key)
	}

	return S3ObjectMetadata{
		Key:            key,
		ChecksumSHA256: *head.ChecksumSHA256,
		LastModified:   *head.LastModified,
	}, nil
}

func WaitForS3ObjectHashChange(
	ctx context.Context,
	client *s3.Client,
	bucket, key, hashBefore string,
) (S3ObjectMetadata, error) {
	var headAfter S3ObjectMetadata

	err := apimachinerywait.PollUntilContextTimeout(
		ctx,
		defaultS3PollInterval,
		defaultS3PollTimeout,
		false,
		func(ctx context.Context) (bool, error) {
			current, ok := getS3ObjectMetadataIfReady(ctx, client, bucket, key)
			if !ok || current.ChecksumSHA256 == hashBefore {
				return false, nil
			}
			headAfter = current
			return true, nil
		},
	)
	if err != nil {
		return S3ObjectMetadata{}, fmt.Errorf("timeout waiting for hash change on %q: %w", key, err)
	}

	return headAfter, nil
}

func getS3ObjectMetadataIfReady(ctx context.Context, client *s3.Client, bucket, key string) (S3ObjectMetadata, bool) {
	current, err := GetS3ObjectMetadata(ctx, client, bucket, key)
	if err != nil {
		return S3ObjectMetadata{}, false
	}
	return current, true
}
