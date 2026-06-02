//go:build e2e

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

// Package gdrive_test contains e2e tests for the gdrive package.
// Google Drive calls hit the real API; LDAP is mocked from
// testdata/ldap_users.json.
//
// Required:
//
//	GDRIVE_SERVICE_ACCOUNT_FILE – path to Google service account JSON
//
// Run:
//
//	GDRIVE_SERVICE_ACCOUNT_FILE=/path/to/sa.json \
//	  go test ./test/pkg/gdrive/ -tags=e2e -v -count=1
package gdrive_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"

	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/cache"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/cache/inmemory"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive"
	googleclient "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/google"
	gdriveldap "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/gdrive/ldap"
)

const testdataDir = "testdata"

func TestMain(m *testing.M) {
	log.SetLogger(zap.New(zap.UseDevMode(true)))
	os.Exit(m.Run())
}

// ---------------------------------------------------------------------------
// Mock LDAP client (loaded from testdata/ldap_users.json)
// ---------------------------------------------------------------------------

type ldapTestUser struct {
	UID  string `json:"uid"`
	Mail string `json:"mail"`
}

type mockLDAPClient struct {
	byEmail map[string]string // email → uid
	byUID   map[string]string // uid → uid
}

var _ gdriveldap.Client = (*mockLDAPClient)(nil)

func newMockLDAPClient(t *testing.T) *mockLDAPClient {
	t.Helper()
	data, err := os.ReadFile(
		filepath.Join(testdataDir, "ldap_users.json"))
	require.NoError(t, err)

	var users []ldapTestUser
	require.NoError(t, json.Unmarshal(data, &users))

	m := &mockLDAPClient{
		byEmail: make(map[string]string, len(users)),
		byUID:   make(map[string]string, len(users)),
	}
	for _, u := range users {
		m.byEmail[u.Mail] = u.UID
		m.byUID[u.UID] = u.UID
	}
	return m
}

func (m *mockLDAPClient) GetUserByEmail(
	_ context.Context, email string,
) (map[string]any, error) {
	uid, ok := m.byEmail[email]
	if !ok {
		return nil, fmt.Errorf("user not found: %s", email)
	}
	return map[string]any{"uid": uid}, nil
}

func (m *mockLDAPClient) GetUserByID(
	_ context.Context, userID string,
) (map[string]any, error) {
	uid, ok := m.byUID[userID]
	if !ok {
		return nil, fmt.Errorf("user not found: %s", userID)
	}
	return map[string]any{"uid": uid}, nil
}

func (m *mockLDAPClient) GetGroupData(
	_ context.Context, _ string,
) ([]gdriveldap.SyncUser, error) {
	return nil, fmt.Errorf("not implemented in mock")
}

// ldapUserUIDs returns the set of UIDs the mock LDAP knows about.
func ldapUserUIDs(t *testing.T) map[string]bool {
	t.Helper()
	m := newMockLDAPClient(t)
	uids := make(map[string]bool, len(m.byUID))
	for uid := range m.byUID {
		uids[uid] = true
	}
	return uids
}

// ---------------------------------------------------------------------------
// Client construction
// ---------------------------------------------------------------------------

func requireEnv(t *testing.T, key string) string {
	t.Helper()
	val := os.Getenv(key)
	if val == "" {
		t.Skipf("skipping: %s is not set", key)
	}
	return val
}

func newGoogleClient(t *testing.T) googleclient.GoogleClient {
	t.Helper()
	saFile := requireEnv(t, "GDRIVE_SERVICE_ACCOUNT_FILE")
	client, err := googleclient.NewClient(
		context.Background(), saFile)
	require.NoError(t, err, "failed to create Google client")
	return client
}

func newCacheClient(t *testing.T) cache.Cache {
	t.Helper()
	c, err := cache.New(&cache.Config{
		Driver: cache.DriverMemory,
		InMemory: &inmemory.Config{
			DefaultExpiration: -1,
			CleanupInterval:   -1,
		},
	})
	require.NoError(t, err)
	return c
}

func newTestClient(t *testing.T) *gdrive.Client {
	t.Helper()
	client, err := gdrive.NewClient(
		newGoogleClient(t),
		newMockLDAPClient(t),
		newCacheClient(t),
	)
	require.NoError(t, err)
	return client
}

// ---------------------------------------------------------------------------
// Testdata loaders
// ---------------------------------------------------------------------------

func loadFolderIDs(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(
		filepath.Join(testdataDir, "folder_ids.json"))
	require.NoError(t, err)
	var input gdrive.FolderIDs
	require.NoError(t, json.Unmarshal(data, &input))
	require.NotEmpty(t, input.FolderIDs)
	return input.FolderIDs
}

func loadExpectedCrawlMetadata(
	t *testing.T,
) []gdrive.CrawlRecord {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(
		testdataDir, "permissions", "crawl_metadata.json"))
	require.NoError(t, err)
	var records []gdrive.CrawlRecord
	require.NoError(t, json.Unmarshal(data, &records))
	return records
}

func loadExpectedPermissions(
	t *testing.T, fileID string,
) []gdrive.Permission {
	t.Helper()
	path := filepath.Join(testdataDir, "permissions",
		fmt.Sprintf("permissions_%s.json", fileID))
	data, err := os.ReadFile(path)
	require.NoError(t, err)
	var perms []gdrive.Permission
	require.NoError(t, json.Unmarshal(data, &perms))
	return perms
}

func loadExpectedInaccessibleItems(
	t *testing.T,
) gdrive.InaccessibleItems {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(
		testdataDir, "permissions", "inaccessible_items.json"))
	require.NoError(t, err)
	var items gdrive.InaccessibleItems
	require.NoError(t, json.Unmarshal(data, &items))
	return items
}

func loadExpectedFailedIDs(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(
		testdataDir, "permissions", "failed.json"))
	require.NoError(t, err)
	var ids []string
	require.NoError(t, json.Unmarshal(data, &ids))
	return ids
}

func loadExpectedWarnings(t *testing.T) []string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(
		testdataDir, "permissions", "warnings.json"))
	require.NoError(t, err)
	var ids []string
	require.NoError(t, json.Unmarshal(data, &ids))
	return ids
}

func recordsByFileID(
	records []gdrive.CrawlRecord,
) map[string]gdrive.CrawlRecord {
	m := make(map[string]gdrive.CrawlRecord, len(records))
	for _, r := range records {
		m[r.FileID] = r
	}
	return m
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

// TestLDAPResolution verifies the mock LDAP resolves each test
// user by email → uid. If ldap_users.json is modified
// incorrectly, this catches it.
func TestLDAPResolution(t *testing.T) {
	ldapClient := newMockLDAPClient(t)

	data, err := os.ReadFile(
		filepath.Join(testdataDir, "ldap_users.json"))
	require.NoError(t, err)
	var users []ldapTestUser
	require.NoError(t, json.Unmarshal(data, &users))

	for _, u := range users {
		t.Run(u.UID, func(t *testing.T) {
			result, err := ldapClient.GetUserByEmail(
				context.Background(), u.Mail)
			require.NoError(t, err,
				"lookup for %s should succeed", u.Mail)
			assert.Equal(t, u.UID, result["uid"])
		})
	}
}

func TestCrawlFolders(t *testing.T) {
	client := newTestClient(t)
	folderIDs := loadFolderIDs(t)
	expectedRecords := loadExpectedCrawlMetadata(t)
	expectedInaccessible := loadExpectedInaccessibleItems(t)

	var allRecords []gdrive.CrawlRecord
	mergedInaccessible := gdrive.InaccessibleItems{
		Folders:               []gdrive.InaccessibleFolder{},
		Files:                 []gdrive.InaccessibleFile{},
		ShortcutTargetFolders: []gdrive.InaccessibleShortcutFolder{},
		ShortcutTargetFiles:   []gdrive.InaccessibleShortcutFile{},
	}

	for _, folderID := range folderIDs {
		result, err := client.CrawlFolder(
			context.Background(), folderID, []string{}, 3)
		require.NoError(t, err,
			"CrawlFolder should not error for %s", folderID)

		allRecords = append(allRecords, result.Records...)
		mergedInaccessible.Folders = append(
			mergedInaccessible.Folders,
			result.InaccessibleItems.Folders...)
		mergedInaccessible.Files = append(
			mergedInaccessible.Files,
			result.InaccessibleItems.Files...)
		mergedInaccessible.ShortcutTargetFolders = append(
			mergedInaccessible.ShortcutTargetFolders,
			result.InaccessibleItems.ShortcutTargetFolders...)
		mergedInaccessible.ShortcutTargetFiles = append(
			mergedInaccessible.ShortcutTargetFiles,
			result.InaccessibleItems.ShortcutTargetFiles...)
	}

	assert.Equal(t, len(expectedRecords), len(allRecords),
		"should discover the same number of files")

	expectedByID := recordsByFileID(expectedRecords)
	actualByID := recordsByFileID(allRecords)

	for fileID, expected := range expectedByID {
		actual, ok := actualByID[fileID]
		if !assert.True(t, ok,
			"expected file %s (%s) not found",
			fileID, expected.FileName) {
			continue
		}
		assert.Equal(t, expected.FileName, actual.FileName)
		assert.Equal(t, expected.MimeType, actual.MimeType)
		assert.Equal(t, expected.Owner, actual.Owner)
		assert.Equal(t, expected.FileSize, actual.FileSize)
		assert.Equal(t, expected.RootFolderID, actual.RootFolderID)
		assert.Equal(t, expected.ParentFolderID,
			actual.ParentFolderID)
		assert.Equal(t, expected.ParentFolderName,
			actual.ParentFolderName)
		assert.Equal(t, expected.Status, actual.Status)
	}

	assert.Len(t, mergedInaccessible.Folders,
		len(expectedInaccessible.Folders))
	assert.Len(t, mergedInaccessible.Files,
		len(expectedInaccessible.Files))
	assert.Len(t, mergedInaccessible.ShortcutTargetFolders,
		len(expectedInaccessible.ShortcutTargetFolders))
	assert.Len(t, mergedInaccessible.ShortcutTargetFiles,
		len(expectedInaccessible.ShortcutTargetFiles))
}

func TestFilePermissions(t *testing.T) {
	client := newTestClient(t)
	expectedRecords := loadExpectedCrawlMetadata(t)
	validUIDs := ldapUserUIDs(t)

	for _, record := range expectedRecords {
		t.Run(record.FileName, func(t *testing.T) {
			expectedPerms := loadExpectedPermissions(
				t, record.FileID)

			permissions, warnings, err := client.GetFilePermissions(
				context.Background(), record.FileID, 3)
			require.NoError(t, err)
			assert.Empty(t, warnings)
			require.Len(t, permissions, len(expectedPerms))

			sort.Slice(permissions, func(i, j int) bool {
				return permissions[i].UID < permissions[j].UID
			})
			sort.Slice(expectedPerms, func(i, j int) bool {
				return expectedPerms[i].UID < expectedPerms[j].UID
			})

			for i, expected := range expectedPerms {
				actual := permissions[i]
				assert.Equal(t, expected.Type, actual.Type)
				assert.Equal(t, expected.Role, actual.Role)
				assert.Equal(t, expected.UID, actual.UID)
				assert.Equal(t, expected.Domain, actual.Domain)

				if actual.Type == "user" {
					assert.True(t, validUIDs[actual.UID],
						"uid %q not in LDAP — "+
							"may be DisplayName fallback",
						actual.UID)
				}
			}
		})
	}
}

// TestDownloadFile downloads every file discovered in
// crawl_metadata.json and verifies that content is returned.
// For CSV/PNG files this exercises the raw binary download path
// (Case 3). The importFormats lookup (Case 2) is also exercised
// — it runs for every file but correctly falls through for
// non-convertible MIME types.
func TestDownloadFile(t *testing.T) {
	client := newTestClient(t)
	expectedRecords := loadExpectedCrawlMetadata(t)

	for _, record := range expectedRecords {
		t.Run(record.FileName, func(t *testing.T) {
			reader, err := client.DownloadFile(
				context.Background(), record.FileID, 3)
			require.NoError(t, err,
				"DownloadFile should not error for %s",
				record.FileID)
			defer reader.Close()

			// Read content and verify it's non-empty
			content, err := io.ReadAll(reader)
			require.NoError(t, err,
				"reading download body should not error")
			assert.Greater(t, len(content), 0,
				"downloaded content should not be empty "+
					"for %s (%s)",
				record.FileName, record.MimeType)

			t.Logf("downloaded %s: %d bytes (mime: %s)",
				record.FileName, len(content),
				record.MimeType)
		})
	}
}

func TestFullPipeline(t *testing.T) {
	client := newTestClient(t)
	folderIDs := loadFolderIDs(t)

	// Phase 1: Crawl
	var allRecords []gdrive.CrawlRecord
	mergedInaccessible := gdrive.InaccessibleItems{
		Folders:               []gdrive.InaccessibleFolder{},
		Files:                 []gdrive.InaccessibleFile{},
		ShortcutTargetFolders: []gdrive.InaccessibleShortcutFolder{},
		ShortcutTargetFiles:   []gdrive.InaccessibleShortcutFile{},
	}

	for _, folderID := range folderIDs {
		result, err := client.CrawlFolder(
			context.Background(), folderID, []string{}, 3)
		require.NoError(t, err)
		allRecords = append(allRecords, result.Records...)
		mergedInaccessible.Folders = append(
			mergedInaccessible.Folders,
			result.InaccessibleItems.Folders...)
		mergedInaccessible.Files = append(
			mergedInaccessible.Files,
			result.InaccessibleItems.Files...)
		mergedInaccessible.ShortcutTargetFolders = append(
			mergedInaccessible.ShortcutTargetFolders,
			result.InaccessibleItems.ShortcutTargetFolders...)
		mergedInaccessible.ShortcutTargetFiles = append(
			mergedInaccessible.ShortcutTargetFiles,
			result.InaccessibleItems.ShortcutTargetFiles...)
	}

	// Phase 2: Permissions + Download for each discovered file
	var discoveredFileIDs []string
	seen := make(map[string]bool)
	for _, record := range allRecords {
		if record.Status == "successful" && !seen[record.FileID] {
			seen[record.FileID] = true
			discoveredFileIDs = append(
				discoveredFileIDs, record.FileID)
		}
	}

	allPermissions := make(map[string][]gdrive.Permission)
	var failedIDs, warnedIDs []string
	downloadedBytes := make(map[string]int)
	for _, fileID := range discoveredFileIDs {
		// Permissions
		perms, warnings, err := client.GetFilePermissions(
			context.Background(), fileID, 3)
		if err != nil {
			failedIDs = append(failedIDs, fileID)
			continue
		}
		if len(warnings) > 0 {
			warnedIDs = append(warnedIDs, fileID)
		}
		allPermissions[fileID] = perms

		// Download
		reader, err := client.DownloadFile(
			context.Background(), fileID, 3)
		require.NoError(t, err,
			"download should not error for %s", fileID)
		content, err := io.ReadAll(reader)
		reader.Close()
		require.NoError(t, err)
		downloadedBytes[fileID] = len(content)
	}

	// Assertions
	expectedRecords := loadExpectedCrawlMetadata(t)
	assert.Equal(t, len(expectedRecords), len(allRecords))

	expectedByID := recordsByFileID(expectedRecords)
	for fileID, expected := range expectedByID {
		actual, ok := recordsByFileID(allRecords)[fileID]
		if assert.True(t, ok, "missing %s", fileID) {
			assert.Equal(t, expected.FileName, actual.FileName)
			assert.Equal(t, expected.MimeType, actual.MimeType)
			assert.Equal(t, expected.Status, actual.Status)
		}
	}

	expectedInaccessible := loadExpectedInaccessibleItems(t)
	assert.Len(t, mergedInaccessible.Folders,
		len(expectedInaccessible.Folders))
	assert.Len(t, mergedInaccessible.Files,
		len(expectedInaccessible.Files))

	assert.Equal(t, len(loadExpectedFailedIDs(t)), len(failedIDs))
	assert.Equal(t, len(loadExpectedWarnings(t)), len(warnedIDs))

	assert.Equal(t, len(expectedRecords), len(allPermissions))
	for fileID, actualPerms := range allPermissions {
		expectedPerms := loadExpectedPermissions(t, fileID)
		sort.Slice(actualPerms, func(i, j int) bool {
			return actualPerms[i].UID < actualPerms[j].UID
		})
		sort.Slice(expectedPerms, func(i, j int) bool {
			return expectedPerms[i].UID < expectedPerms[j].UID
		})
		assert.Equal(t, expectedPerms, actualPerms,
			"permissions mismatch for file %s", fileID)
	}

	// Downloads
	assert.Equal(t, len(discoveredFileIDs), len(downloadedBytes),
		"should have downloaded every discovered file")
	for fileID, size := range downloadedBytes {
		assert.Greater(t, size, 0,
			"downloaded content should not be empty for %s",
			fileID)
	}
}
