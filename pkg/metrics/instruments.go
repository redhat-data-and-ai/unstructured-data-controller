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

package metrics

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/trace"
)

const meterName = "udc"

// Instruments holds pre-registered OTel metric instruments for the controller and MCP server.
type Instruments struct {
	// Controller reconcile metrics
	reconcileCount    metric.Int64Counter
	reconcileDuration metric.Float64Histogram
	reconcileErrors   metric.Int64Counter
	filesProcessed    metric.Int64Counter

	// MCP tool metrics
	toolCallCount        metric.Int64Counter
	toolCallDuration     metric.Float64Histogram
	toolCallErrors       metric.Int64Counter
	externalCallDuration metric.Float64Histogram
}

// durationBuckets covers the range from fast API calls (5ms) to long reconcile
// loops processing large file batches (up to 5 minutes).
var durationBuckets = []float64{
	0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 120, 300,
}

func newInstruments(mp metric.MeterProvider) (*Instruments, error) {
	meter := mp.Meter(meterName)
	var inst Instruments
	var err error

	bucketOpt := metric.WithExplicitBucketBoundaries(durationBuckets...)

	// Controller reconcile metrics - recorded by the ReconcileObserver in each reconciler.
	inst.reconcileCount, err = meter.Int64Counter("udc.reconcile.count",
		metric.WithDescription("Total number of reconcile invocations"),
		metric.WithUnit("{reconcile}"))
	if err != nil {
		return nil, err
	}

	inst.reconcileDuration, err = meter.Float64Histogram("udc.reconcile.duration",
		metric.WithDescription("Wall-clock duration of a reconcile loop"),
		metric.WithUnit("s"),
		bucketOpt)
	if err != nil {
		return nil, err
	}

	inst.reconcileErrors, err = meter.Int64Counter("udc.reconcile.errors",
		metric.WithDescription("Total number of reconcile invocations that returned an error"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, err
	}

	inst.filesProcessed, err = meter.Int64Counter("udc.files.processed",
		metric.WithDescription("Total number of files processed across all reconcile loops"),
		metric.WithUnit("{file}"))
	if err != nil {
		return nil, err
	}

	// MCP tool metrics - recorded by the ToolObserver in each MCP tool handler.
	inst.toolCallCount, err = meter.Int64Counter("udc.mcp.tool.calls",
		metric.WithDescription("Total number of MCP tool invocations"),
		metric.WithUnit("{call}"))
	if err != nil {
		return nil, err
	}

	inst.toolCallDuration, err = meter.Float64Histogram("udc.mcp.tool.duration",
		metric.WithDescription("Wall-clock duration of an MCP tool invocation"),
		metric.WithUnit("s"),
		bucketOpt)
	if err != nil {
		return nil, err
	}

	inst.toolCallErrors, err = meter.Int64Counter("udc.mcp.tool.errors",
		metric.WithDescription("Total number of MCP tool invocations that returned an error"),
		metric.WithUnit("{error}"))
	if err != nil {
		return nil, err
	}

	// External service call metrics (embedding API, Snowflake, K8s) - recorded by RecordExternalCall.
	inst.externalCallDuration, err = meter.Float64Histogram("udc.mcp.external.duration",
		metric.WithDescription("Duration of external service calls from MCP tools"),
		metric.WithUnit("s"),
		bucketOpt)
	if err != nil {
		return nil, err
	}

	return &inst, nil
}

// ReconcileObserver captures the start time and controller name for a reconcile loop.
// Use with named returns and defer:
//
//	func (r *MyReconciler) Reconcile(ctx, req) (result, retErr) {
//	    obs := metricsProvider.ReconcileObserver("MyController")
//	    defer obs.End(ctx, &retErr)
type ReconcileObserver struct {
	instruments    *Instruments
	controllerName string
	start          time.Time
}

// ReconcileObserver returns an observer that tracks reconcile duration and outcome.
func (p *Provider) ReconcileObserver(controllerName string) *ReconcileObserver {
	return &ReconcileObserver{
		instruments:    p.Instruments,
		controllerName: controllerName,
		start:          time.Now(),
	}
}

// End records reconcile count, duration, and error metrics.
// Pass pointers to the named return values so the deferred call captures the final values.
func (o *ReconcileObserver) End(ctx context.Context, retErr *error) {
	attrs := metric.WithAttributes(
		attribute.String("controller", o.controllerName),
	)
	elapsed := time.Since(o.start).Seconds()

	o.instruments.reconcileCount.Add(ctx, 1, attrs)
	o.instruments.reconcileDuration.Record(ctx, elapsed, attrs)
	if retErr != nil && *retErr != nil {
		o.instruments.reconcileErrors.Add(ctx, 1, attrs)
	}
}

// RecordFilesProcessed increments the files-processed counter for a controller.
func (p *Provider) RecordFilesProcessed(ctx context.Context, controllerName string, count int64) {
	p.Instruments.filesProcessed.Add(ctx, count,
		metric.WithAttributes(attribute.String("controller", controllerName)))
}

// ToolObserver starts a trace span and returns a finish function that records tool metrics.
// The returned context carries the span for propagation to child spans in pkg/ packages.
//
//	ctx, endTool := metricsProvider.ToolObserver(ctx, "get_chunks_for_embeddings")
//	defer func() { endTool(hasError) }()
func (p *Provider) ToolObserver(
	ctx context.Context, toolName string, spanAttrs ...attribute.KeyValue,
) (context.Context, func(isError bool)) {
	tracer := otel.Tracer(meterName)
	allAttrs := append([]attribute.KeyValue{attribute.String("mcp.tool.name", toolName)}, spanAttrs...)
	ctx, span := tracer.Start(ctx, "mcp.tool/"+toolName,
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes(allAttrs...))

	start := time.Now()
	toolAttr := metric.WithAttributes(attribute.String("tool", toolName))

	return ctx, func(isError bool) {
		elapsed := time.Since(start).Seconds()

		p.Instruments.toolCallCount.Add(ctx, 1, toolAttr)
		p.Instruments.toolCallDuration.Record(ctx, elapsed, toolAttr)
		if isError {
			p.Instruments.toolCallErrors.Add(ctx, 1, toolAttr)
			span.SetStatus(codes.Error, "tool call failed")
		}
		span.End()
	}
}

// RecordExternalCall records the duration of an external service call and, on error,
// marks the current span with the error. Used in MCP tool handlers to time
// calls to K8s, Snowflake, and embedding services.
func (p *Provider) RecordExternalCall(
	ctx context.Context, service, operation string, duration time.Duration, err error,
) {
	attrs := metric.WithAttributes(
		attribute.String("service", service),
		attribute.String("operation", operation),
	)
	p.Instruments.externalCallDuration.Record(ctx, duration.Seconds(), attrs)

	if err != nil {
		span := trace.SpanFromContext(ctx)
		span.RecordError(err)
		span.SetStatus(codes.Error, err.Error())
	}
}
