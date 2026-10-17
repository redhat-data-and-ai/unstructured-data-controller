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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
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

	// Reranking is opt-in and needs both the endpoint and the model name: an empty
	// model would be sent as "model": "" and rejected on every request, so treat a
	// half-configured reranker as disabled rather than silently failing per query.
	// Every branch logs, so the active retrieval path is always visible at startup.
	var rerankerClient reranker.Reranker
	rerankerEndpoint := os.Getenv("RERANKER_ENDPOINT")
	rerankerModelName := os.Getenv("RERANKER_MODEL_NAME")
	rerankerPoolDepth := mcptools.DefaultRerankerPool
	switch {
	case rerankerEndpoint == "":
		slog.Info("reranking disabled, RERANKER_ENDPOINT is not set")
	case rerankerModelName == "":
		slog.Error("reranking disabled, RERANKER_ENDPOINT is set but RERANKER_MODEL_NAME is empty")
	default:
		rerankerPoolDepth = resolveRerankerPoolDepth(os.Getenv("RERANKER_CANDIDATE_POOL"))
		rerankerClient = reranker.NewHTTPClient(&reranker.HTTPClientConfig{
			Endpoint:   rerankerEndpoint,
			APIKey:     os.Getenv("RERANKER_API_KEY"),
			AuthFormat: "Bearer",
			ModelName:  rerankerModelName,
		})
		slog.Info("reranker client initialized",
			"endpoint", rerankerEndpoint, "model", rerankerModelName, "candidate_pool", rerankerPoolDepth)
	}

	mcptools.RegisterListPipelines(mcpServer, k8sClient)
	mcptools.RegisterGetChunksForEmbeddings(mcpServer, k8sClient, embeddingClient, rerankerClient, rerankerPoolDepth)
	mcptools.RegisterGetProcessedDocument(mcpServer, k8sClient)

	oauthStore := auth.NewOAuthStore()
	oauthMiddleware := auth.NewMiddleware(provider, slog.Default(), oauthCfg.DisableIntrospection)
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

// resolveRerankerPoolDepth validates RERANKER_CANDIDATE_POOL. An environment
// variable is external input, so a bad value degrades to the default with a
// warning rather than failing startup: reranking is best-effort and a
// misconfigured pool should not take the server down.
func resolveRerankerPoolDepth(raw string) int {
	if raw == "" {
		return mcptools.DefaultRerankerPool
	}

	poolDepth, err := strconv.Atoi(raw)
	if err != nil {
		slog.Warn("RERANKER_CANDIDATE_POOL is not a number, using the default",
			"value", raw, "default", mcptools.DefaultRerankerPool)
		return mcptools.DefaultRerankerPool
	}
	if poolDepth <= 0 {
		slog.Warn("RERANKER_CANDIDATE_POOL must be positive, using the default",
			"value", poolDepth, "default", mcptools.DefaultRerankerPool)
		return mcptools.DefaultRerankerPool
	}
	if poolDepth > mcptools.MaxRerankerPool {
		slog.Warn("RERANKER_CANDIDATE_POOL exceeds the maximum, clamping",
			"value", poolDepth, "max", mcptools.MaxRerankerPool)
		return mcptools.MaxRerankerPool
	}
	return poolDepth
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
