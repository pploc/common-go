package kafka

import (
	"context"
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/pploc/common-go/observability"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

const (
	HeaderEventType   = "event-type"
	HeaderSource      = "source"
	HeaderTimestamp   = "timestamp"
	HeaderEventID     = "event-id"
	HeaderTraceParent = "traceparent"
	HeaderTraceState  = "tracestate"
	HeaderTraceID     = "x-trace-id"
)

var requiredHeaders = map[string]struct{}{
	HeaderEventType: {}, HeaderSource: {}, HeaderTimestamp: {}, HeaderEventID: {}, HeaderTraceParent: {},
}

// CanonicalHeaders constructs immutable contract-owned headers. It injects W3C
// context first, creating an outbound root only when there is no valid parent,
// and emits x-trace-id only as compatibility correlation. Caller-supplied
// headers cannot override canonical names, including case variants.
func CanonicalHeaders(ctx context.Context, message proto.Message, source, eventID string, now time.Time, extra []Header) ([]Header, error) {
	if message == nil || !message.ProtoReflect().IsValid() {
		return nil, fmt.Errorf("kafka: a concrete protobuf message is required")
	}
	if strings.TrimSpace(source) == "" || strings.TrimSpace(eventID) == "" {
		return nil, fmt.Errorf("kafka: source and event ID are required")
	}
	_, hadW3CParent := observability.ValidSpanContext(ctx)
	if !hadW3CParent {
		root, err := outboundRootContext()
		if err != nil {
			return nil, err
		}
		ctx = trace.ContextWithSpanContext(ctx, root)
	}
	carrier := propagation.HeaderCarrier{}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	traceparent := carrier.Get(HeaderTraceParent)
	if traceparent == "" {
		// The application may not configure a global propagator; the W3C contract
		// remains mandatory for Kafka records.
		traceparent = trace.SpanContextFromContext(ctx).TraceID().String()
		spanID := trace.SpanContextFromContext(ctx).SpanID().String()
		traceparent = "00-" + traceparent + "-" + spanID + "-01"
	}
	headers := []Header{
		{Key: HeaderEventType, Value: []byte(message.ProtoReflect().Descriptor().FullName())},
		{Key: HeaderSource, Value: []byte(strings.TrimSpace(source))},
		{Key: HeaderTimestamp, Value: []byte(strconv.FormatInt(now.UTC().UnixMilli(), 10))},
		{Key: HeaderEventID, Value: []byte(strings.TrimSpace(eventID))},
		{Key: HeaderTraceParent, Value: []byte(traceparent)},
	}
	if tracestate := carrier.Get(HeaderTraceState); tracestate != "" {
		headers = append(headers, Header{Key: HeaderTraceState, Value: []byte(tracestate)})
	}
	if !hadW3CParent {
		if correlationID, ok := observability.CorrelationID(ctx); ok {
			headers = append(headers, Header{Key: HeaderTraceID, Value: []byte(correlationID)})
		}
	}
	for _, header := range extra {
		key := strings.ToLower(strings.TrimSpace(header.Key))
		if _, reserved := requiredHeaders[key]; reserved || key == HeaderTraceState || key == HeaderTraceID {
			return nil, fmt.Errorf("kafka: caller may not override canonical header %q", header.Key)
		}
		headers = append(headers, Header{Key: header.Key, Value: append([]byte(nil), header.Value...)})
	}
	return headers, nil
}

func outboundRootContext() (trace.SpanContext, error) {
	var traceID trace.TraceID
	var spanID trace.SpanID
	if _, err := rand.Read(traceID[:]); err != nil {
		return trace.SpanContext{}, fmt.Errorf("kafka: generate trace ID: %w", err)
	}
	if _, err := rand.Read(spanID[:]); err != nil {
		return trace.SpanContext{}, fmt.Errorf("kafka: generate span ID: %w", err)
	}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: trace.FlagsSampled}), nil
}

// ValidateCanonicalHeaders verifies required metadata is present exactly once.
func ValidateCanonicalHeaders(headers []Header) error {
	seen := make(map[string]struct{}, len(headers))
	for _, header := range headers {
		key := strings.ToLower(header.Key)
		if _, required := requiredHeaders[key]; !required {
			continue
		}
		if len(header.Value) == 0 {
			return fmt.Errorf("kafka: required header %q is empty", key)
		}
		if _, exists := seen[key]; exists {
			return fmt.Errorf("kafka: required header %q is duplicated", key)
		}
		seen[key] = struct{}{}
	}
	for key := range requiredHeaders {
		if _, exists := seen[key]; !exists {
			return fmt.Errorf("kafka: required header %q is missing", key)
		}
	}
	return nil
}
