package filestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type FileStore struct {
	// all files will be stored relative to this root
	root string
	// s3 client to connect to S3
	s3Client *s3.Client
	// s3 bucket to store files
	s3Bucket string
	// file locks
	localFileLocks sync.Map
	s3FileLocks    sync.Map
}

// New creates a new FileStore instance, examples:
// rootPath: /var/lib/unstructured/
// s3Bucket: unstructured-data-bucket
func New(_ context.Context, rootPath string, s3Bucket string) (*FileStore, error) {
	// check if the root directory exists, and verify it is a directory
	info, err := os.Stat(rootPath)
	if err != nil {
		return nil, err
	}
	if !info.IsDir() {
		return nil, fmt.Errorf("root is not a directory: %s", rootPath)
	}

	// generate s3 client
	s3Client, err := awsclienthandler.GetS3Client()
	if err != nil {
		return nil, err
	}

	return &FileStore{
		root:     rootPath,
		s3Client: s3Client,
		s3Bucket: s3Bucket,
	}, nil
}

// Store locally and then to S3
func (fs *FileStore) Store(ctx context.Context, path string, data []byte) error {
	logger := log.FromContext(ctx)

	localPath := filepath.Join(fs.root, path)
	s3Path := path

	// acquire both the locks first
	fs.lockFile(localPath, s3Path)
	defer fs.unlockFile(localPath, s3Path)

	// write to local filesystem
	logger.Info("writing file to local filesystem", "path", localPath)
	// Create parent directories if they don't exist
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return fmt.Errorf("failed to create parent directory %s: %w", dir, err)
	}
	err := os.WriteFile(localPath, data, 0644)
	if err != nil {
		return err
	}

	// upload to S3
	logger.Info("uploading file to S3", "path", s3Path)
	_, err = fs.s3Client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(fs.s3Bucket),
		Key:    aws.String(s3Path),
		Body:   bytes.NewReader(data),
	})
	if err != nil {
		// if we are not able to store in s3, then also delete the local file
		logger.Error(err, "failed to store file in S3, deleting local file", "path", localPath)
		if removeErr := os.Remove(localPath); removeErr != nil {
			return removeErr
		}
		return err
	}
	logger.Info("successfully stored file in S3", "path", s3Path)
	return nil
}

// please acquire the lock before calling this function
// this only checks if the file exists and is not a directory
// this function is used to check if the file exists in the local cache
func (fs *FileStore) fileExistsInCache(path string) (bool, error) {
	localPath := filepath.Join(fs.root, path)

	// check if the file exists locally
	exists := true
	info, err := os.Stat(localPath)
	if err != nil {
		if !os.IsNotExist(err) {
			// if error is something else than file not found, return the error
			return false, err
		}
		exists = false
	}

	if exists {
		if info.IsDir() {
			return false, fmt.Errorf("path is a directory: %s", localPath)
		}
	}
	return exists, nil
}

// make sure to acquire the locks before calling this function
func (fs *FileStore) fileExistsInS3(ctx context.Context, path string) (bool, error) {
	s3Path := path

	s3PathExists, err := awsclienthandler.ObjectExists(ctx, fs.s3Bucket, s3Path)
	if err != nil {
		return false, err
	}
	return s3PathExists, nil
}

func (fs *FileStore) Exists(ctx context.Context, path string) (bool, error) {
	localPath := filepath.Join(fs.root, path)
	s3Path := path

	// acquire both the locks first
	fs.lockFile(localPath, s3Path)
	defer fs.unlockFile(localPath, s3Path)

	// check if the file exists locally
	exists, err := fs.fileExistsInCache(path)
	if err != nil {
		return false, err
	}
	if exists {
		return true, nil
	}

	// file does not exist locally, try to retrieve from S3
	return fs.fileExistsInS3(ctx, path)
}

// Try to retrieve from local filesystem first, if not found, try to retrieve from S3
// if the file is found in S3, it will be stored in the local cache as well
func (fs *FileStore) Retrieve(ctx context.Context, path string) ([]byte, error) {
	localPath := filepath.Join(fs.root, path)
	s3Path := path

	// acquire both the locks first
	fs.lockFile(localPath, s3Path)
	defer fs.unlockFile(localPath, s3Path)

	existsInCache, err := fs.fileExistsInCache(path)
	if err != nil {
		return nil, err
	}
	if existsInCache {
		return os.ReadFile(localPath)
	}

	// file does not exist locally, try to retrieve from S3
	existsInS3, err := fs.fileExistsInS3(ctx, path)
	if err != nil {
		return nil, err
	}
	if !existsInS3 {
		return nil, fmt.Errorf("file does not exist: %s", localPath)
	}

	s3Object, err := fs.s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(fs.s3Bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, err
	}

	data, err := io.ReadAll(s3Object.Body)
	if err != nil {
		return nil, err
	}

	// write to local filesystem
	// Create parent directories if they don't exist
	dir := filepath.Dir(localPath)
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create parent directory %s: %w", dir, err)
	}

	err = os.WriteFile(localPath, data, 0644)
	if err != nil {
		return nil, err
	}

	return data, nil
}

func (fs *FileStore) GetFileURL(ctx context.Context, path string) (string, error) {
	s3Path := path
	return awsclienthandler.GetPresignedURL(ctx, fs.s3Bucket, s3Path)
}

func (fs *FileStore) Delete(ctx context.Context, path string) error {
	localPath := filepath.Join(fs.root, path)
	s3Path := path

	// acquire both the locks first
	fs.lockFile(localPath, s3Path)
	defer fs.unlockFile(localPath, s3Path)

	// delete from local filesystem
	err := os.Remove(localPath)
	// if the file does not exist, ignore it and move on to delete from S3
	if err != nil && !os.IsNotExist(err) {
		return err
	}

	// delete from S3
	_, err = fs.s3Client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(fs.s3Bucket),
		Key:    aws.String(s3Path),
	})
	if err != nil {
		var notFound *s3types.NotFound
		if errors.As(err, &notFound) {
			return fmt.Errorf("file does not exist: %s", localPath)
		}
		return err
	}

	return nil
}

// Note: this function is not thread safe and is prone to race conditions
// as we do not have directory level locks implemented
func (fs *FileStore) ListFilesInPath(ctx context.Context, path string) ([]string, error) {
	objects, err := awsclienthandler.ListObjectsInPrefix(ctx, fs.s3Bucket, path)
	if err != nil {
		return nil, err
	}

	files := []string{}
	for _, object := range objects {
		files = append(files, *object.Key)
	}
	return files, nil
}

func (fs *FileStore) lockLocalFile(path string) {
	// if this is the first time we're locking this file, create a new mutex
	// LoadOrStore will take care of initializing the mutex if not already present
	mutex, _ := fs.localFileLocks.LoadOrStore(path, &sync.Mutex{})
	mutex.(*sync.Mutex).Lock()
}

func (fs *FileStore) unlockLocalFile(path string) {
	// we should not be using LoadOrStore because we expect the mutex to be present
	// but well, just in case
	mutex, _ := fs.localFileLocks.LoadOrStore(path, &sync.Mutex{})
	mutex.(*sync.Mutex).Unlock()
}

func (fs *FileStore) lockS3File(path string) {
	mutex, _ := fs.s3FileLocks.LoadOrStore(path, &sync.Mutex{})
	mutex.(*sync.Mutex).Lock()
}

func (fs *FileStore) unlockS3File(path string) {
	mutex, _ := fs.s3FileLocks.LoadOrStore(path, &sync.Mutex{})
	mutex.(*sync.Mutex).Unlock()
}

func (fs *FileStore) lockFile(localPath, s3Path string) {
	fs.lockLocalFile(localPath)
	fs.lockS3File(s3Path)
}

func (fs *FileStore) unlockFile(localPath, s3Path string) {
	fs.unlockLocalFile(localPath)
	fs.unlockS3File(s3Path)
}
