package main

import (
	"context"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/prometheus"
	otelmetric "go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

var (
	meter           = otel.Meter("waterway")
	bytesSizeBucket = otelmetric.WithExplicitBucketBoundaries(100, 500, 1000, 5000, 10000, 50000, 100000, 500000, 1000000)
	secondsBucket   = otelmetric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10)
	metrics         = struct {
		requestsTotal       otelmetric.Int64Counter
		requestsCacheHit    otelmetric.Int64Counter
		requestsCacheMiss   otelmetric.Int64Counter
		requestsBlocked     otelmetric.Int64Counter
		requestsRateLimited otelmetric.Int64Counter
		errorsTotal         otelmetric.Int64Counter
		fallbackTotal       otelmetric.Int64Counter
		requestDuration     otelmetric.Float64Histogram
		upstreamDuration    otelmetric.Float64Histogram
		requestSize         otelmetric.Int64Histogram
		responseSize        otelmetric.Int64Histogram
		activeConnections   otelmetric.Int64UpDownCounter
		wsPoolSize          otelmetric.Int64UpDownCounter

		// New metrics for upstream monitoring
		malformedResponses otelmetric.Int64Counter
		upstreamErrors     otelmetric.Int64Counter
		droppedMessages    otelmetric.Int64Counter
	}{
		requestsTotal:       must(meter.Int64Counter("requests_total", otelmetric.WithDescription("Total number of requests processed"))),
		requestsCacheHit:    must(meter.Int64Counter("cache_hits_total", otelmetric.WithDescription("Total number of cache hits"))),
		requestsCacheMiss:   must(meter.Int64Counter("cache_misses_total", otelmetric.WithDescription("Total number of cache misses"))),
		requestsBlocked:     must(meter.Int64Counter("requests_blocked_total", otelmetric.WithDescription("Total number of blocked requests"))),
		requestsRateLimited: must(meter.Int64Counter("requests_rate_limited_total", otelmetric.WithDescription("Total number of rate limited requests"))),
		errorsTotal:         must(meter.Int64Counter("errors_total", otelmetric.WithDescription("Total number of errors"))),
		fallbackTotal:       must(meter.Int64Counter("fallback_total", otelmetric.WithDescription("Total number of HTTP fallbacks"))),
		requestDuration:     must(meter.Float64Histogram("request_duration_seconds", secondsBucket, otelmetric.WithDescription("Request duration in seconds"))),
		upstreamDuration:    must(meter.Float64Histogram("upstream_duration_seconds", secondsBucket, otelmetric.WithDescription("Upstream call duration in seconds"))),
		requestSize:         must(meter.Int64Histogram("request_size_bytes", bytesSizeBucket, otelmetric.WithDescription("Request size in bytes"))),
		responseSize:        must(meter.Int64Histogram("response_size_bytes", bytesSizeBucket, otelmetric.WithDescription("Response size in bytes"))),
		activeConnections:   must(meter.Int64UpDownCounter("active_connections", otelmetric.WithDescription("Number of active WebSocket connections"))),
		wsPoolSize:          must(meter.Int64UpDownCounter("ws_pool_size", otelmetric.WithDescription("Number of connections in WebSocket pool"))),
		malformedResponses:  must(meter.Int64Counter("malformed_responses_total", otelmetric.WithDescription("Total number of malformed responses from upstream that were skipped"))),
		upstreamErrors:      must(meter.Int64Counter("upstream_errors_total", otelmetric.WithDescription("Total number of error responses from upstream"))),
		droppedMessages:     must(meter.Int64Counter("dropped_messages_total", otelmetric.WithDescription("Total number of messages dropped due to full write channel"))),
	}
)

func init() {
	exporter, err := prometheus.New(prometheus.WithNamespace("waterway"))
	if err != nil {
		logger.Error("failed to init prometheus exporter", "err", err)
		panic(err)
	}
	otel.SetMeterProvider(sdkmetric.NewMeterProvider(sdkmetric.WithReader(exporter)))
}

// recordRequest records a completed request
func recordRequest(ctx context.Context, method, transport, status string, duration time.Duration) {
	attrs := otelmetric.WithAttributes(
		attribute.String("method", method),
		attribute.String("transport", transport),
		attribute.String("status", status),
	)
	metrics.requestsTotal.Add(ctx, 1, attrs)
	metrics.requestDuration.Record(ctx, duration.Seconds(), attrs)
}

// must panics if err is non-nil, otherwise returns v.
func must[V any](v V, err error) V {
	if err != nil {
		panic(err)
	}
	return v
}
