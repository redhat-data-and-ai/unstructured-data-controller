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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/awsclienthandler"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/unstructured"
)

// presignedURLTTL is how long a URL handed out by drive_fetch stays valid.
// Short because the URL is a bearer capability that leaves this server: the
// caller needs it long enough for one download, not for a working session.
const presignedURLTTL = 15 * time.Minute

// maxACLBytes bounds one permissions read. A widely shared file flattens to
// thousands of members, but an object larger than this at that key is not an
// ACL and must be refused rather than decoded.
const maxACLBytes = 8 << 20

// driveFileID is Drive's alphabet for a file ID. A caller-supplied ID is
// matched against it before being joined onto a bucket prefix, so one
// carrying a slash or ".." cannot walk out of the pipeline's own directory.
var driveFileID = regexp.MustCompile(`^[A-Za-z0-9_-]{1,256}$`)

var (
	// errNoCrawledFile reports a Drive file that this pipeline never
	// downloaded, which is ordinary: a search runs against all of Drive.
	errNoCrawledFile = errors.New("file is not in this pipeline's crawled corpus")

	// errNoACL reports a crawled file with no stored permissions. Treated as
	// "nobody may read it" rather than "everybody may": the crawler writes
	// the ACL on every sync, so its absence means the sync did not finish.
	errNoACL = errors.New("file has no stored permissions")

	// errNotADriveFileID reports an ID that does not match Drive's alphabet.
	errNotADriveFileID = errors.New("not a Drive file ID")
)

// crawlStore reads the shared file store the source crawler downloads into.
//
// Cached because the alternative is reading a CR, a Secret and handshaking
// TLS on every call. One client for the whole store, not one per pipeline:
// unlike a destination bucket, the store is operator-level and every
// pipeline's stage directory already lives inside it.
type crawlStore struct {
	k8sClient *k8sclient.Client

	mu     sync.Mutex
	cached *cachedCrawlStore
}

type cachedCrawlStore struct {
	s3Client *s3.Client
	bucket   string
	builtAt  time.Time
}

func newCrawlStore(k8sClient *k8sclient.Client) *crawlStore {
	return &crawlStore{k8sClient: k8sClient}
}

// client returns the file store's S3 client and bucket.
func (c *crawlStore) client(ctx context.Context) (*s3.Client, string, error) {
	if cached := c.fromCache(); cached != nil {
		return cached.s3Client, cached.bucket, nil
	}

	// Built outside the lock: it reads a CR and a Secret from the API
	// server, and holding a process-wide lock across that would make one
	// slow read stall every other caller. A lost race rebuilds a client.
	fileStore, err := c.k8sClient.GetFileStore(ctx)
	if err != nil {
		return nil, "", err
	}
	s3Client, err := awsclienthandler.NewS3Client(ctx, fileStore.AWS)
	if err != nil {
		return nil, "", fmt.Errorf("creating file store client: %w", err)
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	c.cached = &cachedCrawlStore{s3Client: s3Client, bucket: fileStore.Bucket, builtAt: time.Now()}
	return s3Client, fileStore.Bucket, nil
}

func (c *crawlStore) fromCache() *cachedCrawlStore {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.cached == nil || time.Since(c.cached.builtAt) > destinationClientTTL {
		return nil
	}
	return c.cached
}

// stagePath returns the directory a pipeline's Drive crawler downloads into.
func (c *crawlStore) stagePath(ctx context.Context, pipelineName string) (string, error) {
	stageName, err := c.k8sClient.GDriveCrawlStage(ctx, pipelineName)
	if err != nil {
		return "", err
	}
	return unstructured.StagePath(pipelineName, stageName), nil
}

// permits reports whether username may read a crawled Drive file, according
// to the ACL the crawler stored beside it. Returns errNoCrawledFile when the
// file was never downloaded and errNoACL when its permissions are missing.
func (c *crawlStore) permits(
	ctx context.Context, stagePath, fileID, username string,
) (bool, error) {
	permissions, err := c.acl(ctx, stagePath, fileID)
	if err != nil {
		return false, err
	}
	return permits(permissions, username), nil
}

// acl reads one file's stored permissions.
func (c *crawlStore) acl(ctx context.Context, stagePath, fileID string) ([]gdrive.Permission, error) {
	if !driveFileID.MatchString(fileID) {
		return nil, fmt.Errorf("%q is %w", fileID, errNotADriveFileID)
	}

	s3Client, bucket, err := c.client(ctx)
	if err != nil {
		return nil, err
	}

	key := path.Join(stagePath, "permissions", fileID+".json")
	object, err := awsclienthandler.GetObject(ctx, s3Client, bucket, key)
	if err != nil {
		var noSuchKey *s3types.NoSuchKey
		var notFound *s3types.NotFound
		if errors.As(err, &noSuchKey) || errors.As(err, &notFound) {
			return nil, fmt.Errorf("%w: %s", errNoACL, key)
		}
		return nil, fmt.Errorf("reading %s: %w", key, err)
	}
	defer object.Body.Close() //nolint:errcheck // read-only body

	data, err := io.ReadAll(io.LimitReader(object.Body, maxACLBytes+1))
	if err != nil {
		return nil, fmt.Errorf("reading %s: %w", key, err)
	}
	if len(data) > maxACLBytes {
		return nil, fmt.Errorf("permissions %s are larger than %d bytes", key, maxACLBytes)
	}

	var permissions []gdrive.Permission
	if err := json.Unmarshal(data, &permissions); err != nil {
		return nil, fmt.Errorf("decoding %s: %w", key, err)
	}
	return permissions, nil
}

// presignedURL mints a time-limited download URL for a crawled Drive file.
// Returns errNoCrawledFile when the pipeline never downloaded it.
//
// Authorization is the caller's job and must happen first: this hands out a
// URL that needs no further authentication to redeem.
func (c *crawlStore) presignedURL(ctx context.Context, stagePath, fileID string) (string, error) {
	s3Client, bucket, err := c.client(ctx)
	if err != nil {
		return "", err
	}

	key, err := c.objectKey(ctx, s3Client, bucket, stagePath, fileID)
	if err != nil {
		return "", err
	}

	url, err := awsclienthandler.GetPresignedURLWithExpiry(
		ctx, s3Client, bucket, key, presignedURLTTL)
	if err != nil {
		return "", fmt.Errorf("presigning %s: %w", key, err)
	}
	return url, nil
}

// objectKey locates the crawled file for a Drive ID. The crawler names it
// "<fileID><ext>" with the extension taken from Drive, so the extension is
// not known here and the directory is listed to find it.
func (*crawlStore) objectKey(
	ctx context.Context, s3Client *s3.Client, bucket, stagePath, fileID string,
) (string, error) {
	if !driveFileID.MatchString(fileID) {
		return "", fmt.Errorf("%q is %w", fileID, errNotADriveFileID)
	}

	prefix := path.Join(stagePath, fileID)
	objects, err := awsclienthandler.ListObjectsInPrefix(ctx, s3Client, bucket, prefix)
	if err != nil {
		return "", fmt.Errorf("listing %s: %w", prefix, err)
	}

	keys := make([]string, 0, len(objects))
	for _, object := range objects {
		if object.Key != nil {
			keys = append(keys, *object.Key)
		}
	}

	key := selectCrawledKey(keys, stagePath, fileID)
	if key == "" {
		return "", fmt.Errorf("%w: %s", errNoCrawledFile, prefix)
	}
	return key, nil
}

// selectCrawledKey picks the crawled file for a Drive ID out of a prefix
// listing, or "" when the listing holds none. The crawler writes a metadata
// sidecar beside every file it stores, so requiring one is what separates a
// crawled "<fileID>.json" document from the sidecar of a crawled "<fileID>"
// that has no extension at all.
func selectCrawledKey(keys []string, stagePath, fileID string) string {
	for _, key := range unstructured.FilterRawFilePaths(keys) {
		// The listing is prefix-matched, so a longer ID starting with this
		// one also comes back. Only an exact name, or one with an extension
		// appended, is this file.
		name := strings.TrimPrefix(key, stagePath)
		if name == fileID || strings.HasPrefix(name, fileID+".") {
			return key
		}
	}
	return ""
}

// permits reports whether username appears in a file's crawled ACL.
//
// Matching is on the LDAP uid: the crawler resolves every user grant through
// LDAP and flattens each group to its members' uids, which is the same value
// an OAuth token carries as preferred_username. A caller identified by email
// also matches on the part before the "@", since the uid is what was stored.
//
// A domain grant matches only a caller whose own identity carries that
// domain. A caller known by bare uid has no domain to compare against, so a
// file shared with a whole domain and with nobody by name is not reachable
// for them; widening that would admit every authenticated caller.
func permits(permissions []gdrive.Permission, username string) bool {
	username = strings.TrimSpace(username)
	if username == "" {
		return false
	}
	uid, domain, _ := strings.Cut(username, "@")

	for _, permission := range permissions {
		switch permission.Type {
		case "user":
			if permission.UID != "" &&
				(strings.EqualFold(permission.UID, username) || strings.EqualFold(permission.UID, uid)) {
				return true
			}
		case "domain":
			if domain != "" && strings.EqualFold(permission.Domain, domain) {
				return true
			}
		default:
			// "anyone" is a link share, which the crawler does not record.
			// An unrecognised type is never read as a grant.
		}
	}
	return false
}
