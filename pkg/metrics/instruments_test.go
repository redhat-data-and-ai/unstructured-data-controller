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
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/attribute"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// setupTestProvider creates a Provider backed by an in-memory metric reader
// for assertions. Call reader.Collect() to gather recorded metrics.
func setupTestProvider(t *testing.T) (*Provider, *sdkmetric.ManualReader) {
	t.Helper()

	reader := sdkmetric.NewManualReader()
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))

	instruments, err := newInstruments(mp)
	require.NoError(t, err)

	return &Provider{
		meterProvider: mp,
		Instruments:   instruments,
	}, reader
}

// collectMetrics gathers all recorded metrics from the reader.
func collectMetrics(
	t *testing.T, reader *sdkmetric.ManualReader,
) metricdata.ResourceMetrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	require.NoError(t, reader.Collect(context.Background(), &rm))
	return rm
}

// findMetric searches for a metric by name in the collected data.
func findMetric(
	rm metricdata.ResourceMetrics, name string,
) *metricdata.Metrics {
	for _, sm := range rm.ScopeMetrics {
		for i := range sm.Metrics {
			if sm.Metrics[i].Name == name {
				return &sm.Metrics[i]
			}
		}
	}
	return nil
}

// requireSumMetric finds a named metric and asserts it is a Sum[int64].
func requireSumMetric(
	t *testing.T, rm metricdata.ResourceMetrics, name string,
) metricdata.Sum[int64] {
	t.Helper()
	m := findMetric(rm, name)
	require.NotNil(t, m, "metric %q should exist", name)
	sum, ok := m.Data.(metricdata.Sum[int64])
	require.True(t, ok, "metric %q should be Sum[int64]", name)
	return sum
}

// requireHistogramMetric finds a named metric and asserts it is a Histogram.
func requireHistogramMetric(
	t *testing.T, rm metricdata.ResourceMetrics, name string,
) metricdata.Histogram[float64] {
	t.Helper()
	m := findMetric(rm, name)
	require.NotNil(t, m, "metric %q should exist", name)
	hist, ok := m.Data.(metricdata.Histogram[float64])
	require.True(t, ok, "metric %q should be Histogram[float64]", name)
	return hist
}

func TestReconcileObserverSuccess(t *testing.T) {
	p, reader := setupTestProvider(t)
	defer func() {
		require.NoError(t, p.meterProvider.Shutdown(context.Background()))
	}()

	obs := p.ReconcileObserver("TestController")
	var retErr error
	obs.End(context.Background(), &retErr)

	rm := collectMetrics(t, reader)

	countSum := requireSumMetric(t, rm, "udc.reconcile.count")
	require.Len(t, countSum.DataPoints, 1)
	assert.Equal(t, int64(1), countSum.DataPoints[0].Value)

	require.NotNil(t, findMetric(rm, "udc.reconcile.duration"),
		"reconcile duration metric should exist")

	// Error counter should not be incremented on success.
	errMetric := findMetric(rm, "udc.reconcile.errors")
	if errMetric != nil {
		sum, ok := errMetric.Data.(metricdata.Sum[int64])
		if ok {
			for _, dp := range sum.DataPoints {
				assert.Equal(t, int64(0), dp.Value,
					"error counter should be 0 on success")
			}
		}
	}
}

func TestReconcileObserverWithError(t *testing.T) {
	p, reader := setupTestProvider(t)
	defer func() {
		require.NoError(t, p.meterProvider.Shutdown(context.Background()))
	}()

	obs := p.ReconcileObserver("FailingController")
	retErr := errors.New("reconcile failed")
	obs.End(context.Background(), &retErr)

	rm := collectMetrics(t, reader)

	errSum := requireSumMetric(t, rm, "udc.reconcile.errors")
	require.Len(t, errSum.DataPoints, 1)
	assert.Equal(t, int64(1), errSum.DataPoints[0].Value,
		"error counter should be 1")

	hasControllerAttr := false
	for _, attr := range errSum.DataPoints[0].Attributes.ToSlice() {
		if attr.Key == "controller" &&
			attr.Value.AsString() == "FailingController" {
			hasControllerAttr = true
		}
	}
	assert.True(t, hasControllerAttr,
		"should have controller=FailingController attribute")
}

func TestRecordFilesProcessed(t *testing.T) {
	p, reader := setupTestProvider(t)
	defer func() {
		require.NoError(t, p.meterProvider.Shutdown(context.Background()))
	}()

	p.RecordFilesProcessed(context.Background(), "DocumentProcessor", 42)

	rm := collectMetrics(t, reader)

	sum := requireSumMetric(t, rm, "udc.files.processed")
	require.Len(t, sum.DataPoints, 1)
	assert.Equal(t, int64(42), sum.DataPoints[0].Value)
}

func TestToolObserver(t *testing.T) {
	p, reader := setupTestProvider(t)
	defer func() {
		require.NoError(t, p.meterProvider.Shutdown(context.Background()))
	}()

	ctx, endTool := p.ToolObserver(
		context.Background(), "get_chunks",
		attribute.String("mcp.pipeline_name", "test-pipeline"),
	)
	require.NotNil(t, ctx, "context should not be nil")

	time.Sleep(5 * time.Millisecond)
	endTool(false)

	rm := collectMetrics(t, reader)

	callsSum := requireSumMetric(t, rm, "udc.mcp.tool.calls")
	require.Len(t, callsSum.DataPoints, 1)
	assert.Equal(t, int64(1), callsSum.DataPoints[0].Value)

	hasToolAttr := false
	for _, attr := range callsSum.DataPoints[0].Attributes.ToSlice() {
		if attr.Key == "tool" && attr.Value.AsString() == "get_chunks" {
			hasToolAttr = true
		}
	}
	assert.True(t, hasToolAttr,
		"tool calls metric should have tool=get_chunks attribute")

	// Error counter should not be incremented on success.
	errMetric := findMetric(rm, "udc.mcp.tool.errors")
	if errMetric != nil {
		errSum, ok := errMetric.Data.(metricdata.Sum[int64])
		if ok {
			for _, dp := range errSum.DataPoints {
				assert.Equal(t, int64(0), dp.Value,
					"error counter should be 0 on success")
			}
		}
	}
}

func TestToolObserverWithError(t *testing.T) {
	p, reader := setupTestProvider(t)
	defer func() {
		require.NoError(t, p.meterProvider.Shutdown(context.Background()))
	}()

	_, endTool := p.ToolObserver(
		context.Background(), "failing_tool",
	)
	endTool(true)

	rm := collectMetrics(t, reader)

	errSum := requireSumMetric(t, rm, "udc.mcp.tool.errors")
	require.Len(t, errSum.DataPoints, 1)
	assert.Equal(t, int64(1), errSum.DataPoints[0].Value)
}

func TestRecordExternalCall(t *testing.T) {
	p, reader := setupTestProvider(t)
	defer func() {
		require.NoError(t, p.meterProvider.Shutdown(context.Background()))
	}()

	p.RecordExternalCall(
		context.Background(), "snowflake", "search_chunks",
		150*time.Millisecond, nil,
	)

	rm := collectMetrics(t, reader)

	hist := requireHistogramMetric(t, rm, "udc.mcp.external.duration")
	require.Len(t, hist.DataPoints, 1)
	assert.Greater(t, hist.DataPoints[0].Sum, 0.0,
		"histogram sum should be positive")

	hasService := false
	hasOperation := false
	for _, attr := range hist.DataPoints[0].Attributes.ToSlice() {
		if attr.Key == "service" &&
			attr.Value.AsString() == "snowflake" {
			hasService = true
		}
		if attr.Key == "operation" &&
			attr.Value.AsString() == "search_chunks" {
			hasOperation = true
		}
	}
	assert.True(t, hasService, "should have service=snowflake attribute")
	assert.True(t, hasOperation,
		"should have operation=search_chunks attribute")
}

func TestRecordExternalCallWithError(t *testing.T) {
	p, reader := setupTestProvider(t)
	defer func() {
		require.NoError(t, p.meterProvider.Shutdown(context.Background()))
	}()

	// Even with an error, the duration should be recorded.
	p.RecordExternalCall(
		context.Background(), "embedding", "generate",
		500*time.Millisecond, errors.New("connection timeout"),
	)

	rm := collectMetrics(t, reader)

	hist := requireHistogramMetric(t, rm, "udc.mcp.external.duration")
	require.Len(t, hist.DataPoints, 1)
	assert.Greater(t, hist.DataPoints[0].Sum, 0.0)
}
