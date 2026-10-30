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
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	mcptools "github.com/redhat-data-and-ai/unstructured-data-controller/internal/mcp/tools"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/telemetry"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	serverName  = "unstructured-data-controller"
	defaultAddr = ":8080"

	// Tool calls are user-driven and low volume, unlike the controller's
	// continuous reconcile loops, so every trace is kept by default.
	// OTEL_TRACES_SAMPLER and OTEL_TRACES_SAMPLER_ARG override this.
	defaultTraceSampleRatio = 1.0

	// Telemetry gets its own flush budget rather than sharing the server's
	// shutdown context, which srv.Shutdown may already have consumed. The pod
	// sets no terminationGracePeriodSeconds, so Kubernetes allows 30s and the
	// worst case here (10s drain + 5s flush) fits comfortably.
	telemetryFlushTimeout = 5 * time.Second
)

// serverVersion is stamped at build time with -ldflags "-X main.serverVersion=...".
// It is reported both over the MCP protocol and as the service.version
// resource attribute.
var serverVersion = "dev"

func main() {
	logger.Init()
	ctrl.SetLogger(logr.FromSlogHandler(slog.Default().Handler()))

	// Configured from the standard OTEL_* environment variables, which the
	// deployment supplies through its existing envFrom ConfigMap. No endpoint
	// means no telemetry and no behaviour change.
	telemetryProvider, err := telemetry.Setup(context.Background(), telemetry.Config{
		ServiceName:      "unstructured-data-mcp-server",
		ServiceVersion:   serverVersion,
		TraceSampleRatio: defaultTraceSampleRatio,
		Logger:           ctrl.Log.WithName("telemetry"),
	})
	if err != nil {
		slog.Error("failed to set up telemetry", "error", err)
		os.Exit(1)
	}

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

	mcptools.RegisterListPipelines(mcpServer, k8sClient)
	mcptools.RegisterGetChunksForEmbeddings(mcpServer, k8sClient, embeddingClient)
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

	serveErr := make(chan error, 1)
	go func() {
		slog.Info("MCP server starting", "addr", addr, "endpoint", "/mcp")
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serveErr <- err
		}
	}()

	// Wait for a signal or for the listener to fail. Reporting the failure
	// over a channel rather than calling os.Exit inside the goroutine keeps
	// every exit path running the shutdown sequence below, so the telemetry
	// flush cannot be skipped.
	var listenErr error
	select {
	case <-ctx.Done():
	case listenErr = <-serveErr:
		slog.Error("server failed to start", "error", listenErr)
	}

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

	// Fresh context: shutdownCtx above shares its 10s budget with
	// srv.Shutdown and may already be expired, and the SDK's flush returns
	// immediately on an expired context, dropping whatever is buffered.
	flushCtx, cancelFlush := context.WithTimeout(context.Background(), telemetryFlushTimeout)
	if err := telemetryProvider.Shutdown(flushCtx); err != nil {
		slog.Error("failed to flush telemetry", "error", err)
	}
	cancelFlush()

	slog.Info("MCP server stopped")

	if listenErr != nil {
		os.Exit(1)
	}
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
