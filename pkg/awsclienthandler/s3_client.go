package awsclienthandler

import (
	"context"
	"errors"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
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
