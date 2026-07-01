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

package google

import (
	"context"
	"fmt"
	"io"
	"net/http"

	cloudidentity "google.golang.org/api/cloudidentity/v1"
	drive "google.golang.org/api/drive/v3"
	"google.golang.org/api/option"
	htransport "google.golang.org/api/transport/http"
)

// GoogleClient defines the interface for Google API operations
// used by the gdrive package.
type GoogleClient interface {
	GetFilePermissions(
		ctx context.Context, fileID string, maxRetries int,
	) ([]*drive.Permission, error)
	GetGroupMembers(
		ctx context.Context, groupEmail string,
	) ([]string, error)
	ListFolderContents(
		ctx context.Context, folderID string, maxRetries int,
	) ([]*drive.File, error)
	GetFileMetadata(
		ctx context.Context, fileID string, maxRetries int,
	) (*drive.File, error)
	IsFolderAccessible(
		ctx context.Context, folderID string,
	) bool
	DownloadFile(
		ctx context.Context, fileID string, maxRetries int,
	) (io.ReadCloser, error)
}

// Client implements GoogleClient using Google Drive and Cloud
// Identity APIs.
type Client struct {
	driveService         *drive.Service
	cloudIdentityService *cloudidentity.Service
	// httpClient is an authenticated HTTP client used for direct
	// export URL requests (bypasses the 10 MB Files.Export limit).
	httpClient *http.Client
}

// NewClient creates a new Google API client using service account
// credentials.
func NewClient(
	ctx context.Context,
	credentialsFile string,
) (*Client, error) {
	opts := []option.ClientOption{
		option.WithAuthCredentialsFile(option.ServiceAccount, credentialsFile),
		option.WithScopes(drive.DriveReadonlyScope),
	}

	driveService, err := drive.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create Drive service: %w", err)
	}

	//nolint:staticcheck // TODO: migrate to credentials.DetectDefault
	cloudIdentityService, err := cloudidentity.NewService(
		ctx,
		option.WithAuthCredentialsFile(option.ServiceAccount, credentialsFile),
		option.WithScopes(
			cloudidentity.CloudIdentityGroupsReadonlyScope),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create Cloud Identity service: %w", err)
	}

	// Create an authenticated HTTP client with the same
	// credentials for direct export URL requests.
	httpClient, _, err := htransport.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create HTTP client: %w", err)
	}

	return &Client{
		driveService:         driveService,
		cloudIdentityService: cloudIdentityService,
		httpClient:           httpClient,
	}, nil
}

// NewClientFromJSON creates a new Google API client using raw service
// account credentials JSON, avoiding the need to write credentials
// to a temporary file.
func NewClientFromJSON(
	ctx context.Context,
	credentialsJSON []byte,
) (*Client, error) {
	opts := []option.ClientOption{
		option.WithAuthCredentialsJSON(option.ServiceAccount, credentialsJSON),
		option.WithScopes(drive.DriveReadonlyScope),
	}

	driveService, err := drive.NewService(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create Drive service: %w", err)
	}

	//nolint:staticcheck // TODO: migrate to credentials.DetectDefault
	cloudIdentityService, err := cloudidentity.NewService(
		ctx,
		option.WithAuthCredentialsJSON(option.ServiceAccount, credentialsJSON),
		option.WithScopes(
			cloudidentity.CloudIdentityGroupsReadonlyScope),
	)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create Cloud Identity service: %w", err)
	}

	httpClient, _, err := htransport.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to create HTTP client: %w", err)
	}

	return &Client{
		driveService:         driveService,
		cloudIdentityService: cloudIdentityService,
		httpClient:           httpClient,
	}, nil
}
