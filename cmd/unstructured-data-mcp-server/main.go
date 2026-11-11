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

package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	mcptools "github.com/redhat-data-and-ai/unstructured-data-controller/internal/mcp/tools"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/reranker"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	serverName    = "unstructured-data-controller"
	serverVersion = "0.1.0"
	defaultAddr   = ":8080"
)

func main() {
	logger.Init()
	ctrl.SetLogger(logr.FromSlogHandler(slog.Default().Handler()))

	oauthCfg, err := auth.NewOAuthConfigFromEnv()
	if err != nil {
		slog.Error("failed to load OAuth configuration", "error", err)
		os.Exit(1)
	}

	provider, err := auth.NewGenericProvider(oauthCfg)
	if err != nil {
		slog.Error("failed to create OAuth provider", "error", err)
		os.Exit(1)
	}

	k8sClient, err := k8sclient.NewClient()
	if err != nil {
		slog.Error("failed to create kubernetes client", "error", err)
		os.Exit(1)
	}
	slog.Info("kubernetes client initialized successfully")

	mcpServer := mcp.NewServer(
		&mcp.Implementation{
			Name:    serverName,
			Version: serverVersion,
		},
		nil,
	)

	embeddingClient := embedding.NewHTTPClient(&embedding.HTTPClientConfig{
		Endpoint:   os.Getenv("EMBEDDING_ENDPOINT"),
		APIKey:     os.Getenv("EMBEDDING_API_KEY"),
		AuthFormat: "Bearer",
		ModelName:  os.Getenv("EMBEDDING_MODEL_NAME"),
	})

	// Reranking is enabled by an explicit flag, never by the presence of its
	// other settings. Inferring enablement from configuration makes "switched
	// off" and "misconfigured" look identical from the outside, so a typo in a
	// variable name silently turns the feature off.
	rerankerClient, rerankerPoolDepth, err := buildRerankerClient()
	if err != nil {
		slog.Error("invalid reranker configuration", "error", err)
		os.Exit(1)
	}

	mcptools.RegisterListPipelines(mcpServer, k8sClient)
	mcptools.RegisterGetChunksForEmbeddings(mcpServer, k8sClient, embeddingClient, rerankerClient, rerankerPoolDepth)
	mcptools.RegisterGetProcessedDocument(mcpServer, k8sClient)
	mcptools.RegisterGetCitationImage(mcpServer, k8sClient)

	oauthStore := auth.NewOAuthStore()

	var middlewareOpts []auth.MiddlewareOption
	if oauthCfg.TokenValidation == auth.ValidationJWKS {
		middlewareOpts = append(middlewareOpts, auth.WithJWKSVerifier(
			auth.NewJWKSVerifier(oauthCfg.JWKSURL, oauthCfg.Issuer, nil)))
		slog.Warn("validating tokens by signature, not introspection",
			"issuer", oauthCfg.Issuer,
			"note", "a token revoked before it expires stays accepted; use introspection in production")
	}

	oauthMiddleware := auth.NewMiddleware(provider, slog.Default(), oauthCfg.DisableIntrospection,
		middlewareOpts...)
	oauthServer := auth.NewOAuthServer(provider, oauthCfg.CallbackURL, oauthStore, slog.Default())

	mcpHandler := mcp.NewStreamableHTTPHandler(
		func(_ *http.Request) *mcp.Server { return mcpServer },
		nil,
	)

	mux := http.NewServeMux()
	protectedMCP := oauthMiddleware.Authenticate(mcpHandler)
	filteredMCP := mcptools.PipelineFilterMiddleware(protectedMCP)
	mux.Handle("/mcp", filteredMCP)
	mux.Handle("/mcp/{$}", filteredMCP) // match with trailing slash too

	// OAuth discovery (unauthenticated)
	mux.Handle("/.well-known/oauth-protected-resource", oauthMiddleware.ProtectedResourceMetadataHandler())
	mux.Handle("/.well-known/oauth-authorization-server", oauthMiddleware.MetadataHandler())

	// OAuth authorization server endpoints (unauthenticated)
	mux.HandleFunc("/auth/register", oauthServer.HandleRegister)
	mux.HandleFunc("/auth/authorize", oauthServer.HandleAuthorize)
	mux.HandleFunc("/auth/callback/oidc", oauthServer.HandleCallback)
	mux.HandleFunc("/auth/complete/{token}", oauthServer.HandleComplete)
	mux.HandleFunc("/auth/token", oauthServer.HandleToken)

	mux.HandleFunc("/healthz", healthHandler)
	mux.HandleFunc("/readyz", healthHandler)

	addr := defaultAddr
	if port := os.Getenv("MCP_SERVER_PORT"); port != "" {
		addr = ":" + port
	}

	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       120 * time.Second,
		// WriteTimeout intentionally unset: Streamable HTTP uses SSE which requires long-lived responses.
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go func() {
		slog.Info("MCP server starting", "addr", addr, "endpoint", "/mcp")
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server failed to start", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down MCP server")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Close active MCP sessions concurrently — terminates SSE streams so
	// Shutdown doesn't block. Shutdown is called first to stop accepting
	// new connections, preventing new sessions from sneaking in.
	go func() {
		for session := range mcpServer.Sessions() {
			_ = session.Close()
		}
	}()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("server shutdown error", "error", err)
	}
	oauthMiddleware.Close()
	oauthStore.Close()
	slog.Info("MCP server stopped")
}

// buildRerankerClient constructs the reranker from the environment. It returns
// a nil client when reranking is switched off, and an error when it is
// switched on but configured wrongly: the runtime fallback to cosine ordering
// is for a reranker that breaks later, not one never configured correctly.
func buildRerankerClient() (reranker.Reranker, int, error) {
	if !rerankerEnabled() {
		slog.Info("reranking disabled, ENABLE_RERANKER is not true")
		return nil, mcptools.DefaultRerankerPool, nil
	}

	endpoint := os.Getenv("RERANKER_ENDPOINT")
	if endpoint == "" {
		return nil, 0, errors.New("ENABLE_RERANKER is true but RERANKER_ENDPOINT is empty")
	}
	modelName := os.Getenv("RERANKER_MODEL_NAME")
	if modelName == "" {
		return nil, 0, errors.New("ENABLE_RERANKER is true but RERANKER_MODEL_NAME is empty")
	}

	poolDepth, err := resolveRerankerPoolDepth(os.Getenv("RERANKER_CANDIDATE_POOL"))
	if err != nil {
		return nil, 0, err
	}

	apiKey := os.Getenv("RERANKER_API_KEY")
	// A bearer token on a plain-HTTP endpoint travels in cleartext. In-cluster
	// HTTP is a legitimate deployment, so this warns rather than refusing, but
	// it must be visible: the alternative is leaking a credential on every query
	// with nothing in the logs to say so.
	if apiKey != "" && !strings.HasPrefix(strings.ToLower(endpoint), "https://") {
		slog.Warn("RERANKER_API_KEY will be sent over a non-HTTPS connection in cleartext",
			"endpoint", endpoint)
	}

	slog.Info("reranker client initialized",
		"endpoint", endpoint, "model", modelName, "candidate_pool", poolDepth)

	return reranker.NewHTTPClient(&reranker.HTTPClientConfig{
		Endpoint:   endpoint,
		APIKey:     apiKey,
		AuthFormat: "Bearer",
		ModelName:  modelName,
	}), poolDepth, nil
}

// rerankerEnabled reads the explicit on switch. Enablement is deliberately not
// inferred from whether the other reranker settings happen to be present.
func rerankerEnabled() bool {
	return strings.EqualFold(strings.TrimSpace(os.Getenv("ENABLE_RERANKER")), "true")
}

// resolveRerankerPoolDepth validates RERANKER_CANDIDATE_POOL. A value that
// cannot be honoured is an error rather than a fallback, because a pool running
// at the default when the operator asked for something else is invisible once
// the server is up.
func resolveRerankerPoolDepth(raw string) (int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return mcptools.DefaultRerankerPool, nil
	}

	poolDepth, err := strconv.Atoi(raw)
	if err != nil {
		return 0, fmt.Errorf("RERANKER_CANDIDATE_POOL must be a number, got %q", raw)
	}
	if poolDepth <= 0 {
		return 0, fmt.Errorf("RERANKER_CANDIDATE_POOL must be positive, got %d", poolDepth)
	}
	if poolDepth > mcptools.MaxRerankerPool {
		return 0, fmt.Errorf("RERANKER_CANDIDATE_POOL is %d, which exceeds the maximum of %d",
			poolDepth, mcptools.MaxRerankerPool)
	}
	return poolDepth, nil
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
