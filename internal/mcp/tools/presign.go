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

package tools

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

// downloadURLTTL bounds how long a signed download link works.
//
// Short on purpose: the link carries no identity, so anyone holding it can
// fetch the file until it expires. Long enough for a human to click a result
// they were just shown.
const downloadURLTTL = 5 * time.Minute

// signDownloadURLs fills in DownloadURL for each result, in place.
//
// Best effort by design. A file the destination syncer has not copied yet is
// still a correct search result, so a signing failure is logged and leaves the
// URL empty rather than removing the file from the answer.
func signDownloadURLs(
	ctx context.Context,
	destinations *destinationClients,
	pipelineName, stageName string,
	results []DriveSearchResult,
) {
	log := logger.FromContext(ctx)

	destination, err := destinations.get(ctx, pipelineName)
	if err != nil {
		log.Info("no destination to sign download links against, returning metadata only",
			"pipeline", pipelineName, "error", err)
		return
	}

	for i := range results {
		if results[i].storedPath == "" {
			continue
		}
		url, err := destination.presignStoredFile(ctx, results[i].storedPath, stageName)
		if err != nil {
			log.Error("could not sign a download link", "file_id", results[i].FileID, "error", err)
			continue
		}
		results[i].DownloadURL = url
	}
}

// storedObjectKey turns the crawler's own path for a file into the object key
// under a destination prefix.
//
// Separate from the signing so it can be tested without an S3 client, and
// because getting it wrong is the only way this code reaches another
// pipeline's data in a shared bucket.
func storedObjectKey(prefix, storedPath, stageName string) (string, error) {
	key := unstructured.DestinationKey(storedPath, stageName)
	if key == "" {
		return "", fmt.Errorf("stored path %q does not sit under stage %q", storedPath, stageName)
	}

	// path.Join resolves ".." rather than refusing it, which would walk out of
	// this pipeline's prefix and into another product's data in the same
	// bucket, so the key is checked before it is joined.
	if path.IsAbs(key) || key != path.Clean(key) || strings.HasPrefix(key, "../") {
		return "", fmt.Errorf("object key %q is not a plain relative path", key)
	}
	return path.Join(prefix, key), nil
}

// presignStoredFile returns a short-lived URL for the crawler's stored copy of
// a file, which the caller fetches directly from S3 with no credential.
//
// storedPath is the crawler's own path, as recorded in the warehouse. The
// object key is derived from it rather than from anything the caller supplied.
func (d *destinationClient) presignStoredFile(
	ctx context.Context, storedPath, stageName string,
) (string, error) {
	fullKey, err := storedObjectKey(d.prefix, storedPath, stageName)
	if err != nil {
		return "", err
	}

	presigned, err := s3.NewPresignClient(d.s3Client).PresignGetObject(ctx,
		&s3.GetObjectInput{Bucket: &d.bucket, Key: &fullKey},
		s3.WithPresignExpires(downloadURLTTL))
	if err != nil {
		return "", fmt.Errorf("signing a download link for %s: %w", fullKey, err)
	}
	return presigned.URL, nil
}
