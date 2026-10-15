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
	"syscall"
	"time"

	"github.com/go-logr/logr"
	"github.com/modelcontextprotocol/go-sdk/mcp"

	mcptools "github.com/redhat-data-and-ai/unstructured-data-controller/internal/mcp/tools"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/auth"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/embedding"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/k8sclient"
	"github.com/redhat-data-and-ai/unstructured-data-controller/pkg/logger"
	udcmetrics "github.com/redhat-data-and-ai/unstructured-data-controller/pkg/metrics"
	ctrl "sigs.k8s.io/controller-runtime"
)

const (
	serverName     = "unstructured-data-controller"
	serverVersion  = "0.1.0"
	defaultAddr    = ":8080"
	defaultMetrics = ":8000"
)

func main() {
	logger.Init()
	ctrl.SetLogger(logr.FromSlogHandler(slog.Default().Handler()))

	// Initialize OTel metrics and tracing. Prometheus metrics are served on /metrics.
	// OTLP push (for Datadog/Langfuse) activates when OTEL_EXPORTER_OTLP_ENDPOINT is set.
	metricsProvider, err := udcmetrics.Init(
		context.Background(), udcmetrics.Config{
			ServiceName:    serverName,
			ServiceVersion: serverVersion,
		},
	)
	if err != nil {
		slog.Error("failed to initialize metrics provider", "error", err)
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

	mcptools.RegisterListPipelines(mcpServer, k8sClient, metricsProvider)
	mcptools.RegisterGetChunksForEmbeddings(mcpServer, k8sClient, embeddingClient, metricsProvider)
	mcptools.RegisterGetProcessedDocument(mcpServer, k8sClient, metricsProvider)

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

	// Metrics are served on a dedicated port so scraping tools cannot reach
	// the MCP protocol, OAuth, or health endpoints through the metrics port.
	metricsAddr := defaultMetrics
	if port := os.Getenv("METRICS_PORT"); port != "" {
		metricsAddr = ":" + port
	}
	metricsMux := http.NewServeMux()
	if h := metricsProvider.PrometheusHandler(); h != nil {
		metricsMux.Handle("/metrics", h)
	}
	metricsSrv := &http.Server{
		Addr:              metricsAddr,
		Handler:           metricsMux,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       30 * time.Second,
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

	go func() {
		slog.Info("metrics server starting", "addr", metricsAddr)
		if err := metricsSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server failed to start", "error", err)
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
	if err := metricsSrv.Shutdown(shutdownCtx); err != nil {
		slog.Error("metrics server shutdown error", "error", err)
	}
	// Flush pending OTLP metrics/traces before exit.
	if err := metricsProvider.Shutdown(shutdownCtx); err != nil {
		slog.Error("metrics provider shutdown error", "error", err)
	}
	oauthMiddleware.Close()
	oauthStore.Close()
	slog.Info("MCP server stopped")
}

func healthHandler(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(`{"status":"ok"}`))
}
