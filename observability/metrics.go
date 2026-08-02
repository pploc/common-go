package observability

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics records bounded server request attributes. Applications inject a
// MeterProvider through their chosen OpenTelemetry configuration.
type Metrics struct {
	requests metric.Int64Counter
	duration metric.Float64Histogram
	panics   metric.Int64Counter
}

// NewMetrics creates the metrics instruments from meter.
func NewMetrics(meter metric.Meter) (*Metrics, error) {
	requests, err := meter.Int64Counter("common_go.grpc.server.requests")
	if err != nil {
		return nil, err
	}
	duration, err := meter.Float64Histogram("common_go.grpc.server.duration", metric.WithUnit("s"))
	if err != nil {
		return nil, err
	}
	panics, err := meter.Int64Counter("common_go.grpc.server.panics")
	if err != nil {
		return nil, err
	}
	return &Metrics{requests: requests, duration: duration, panics: panics}, nil
}

// RecordRequest records a completed request using bounded method and status
// attributes only.
func (m *Metrics) RecordRequest(ctx context.Context, method, status string, duration time.Duration) {
	if m == nil {
		return
	}
	attrs := metric.WithAttributes(attribute.String("rpc.method", method), attribute.String("rpc.grpc.status_code", status))
	m.requests.Add(ctx, 1, attrs)
	m.duration.Record(ctx, duration.Seconds(), attrs)
}

// RecordPanic records a recovered handler panic without panic details.
func (m *Metrics) RecordPanic(ctx context.Context, method string) {
	if m == nil {
		return
	}
	m.panics.Add(ctx, 1, metric.WithAttributes(attribute.String("rpc.method", method)))
}
