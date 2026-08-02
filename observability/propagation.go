// Package observability provides opt-in OpenTelemetry helpers.
package observability

import (
	"context"

	"github.com/pploc/common-go/auth"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

type correlationContextKey struct{}

// ExtractIncoming extracts W3C trace context from incoming gRPC metadata. The
// legacy x-trace-id is retained only as correlation data when no valid W3C span
// context was extracted; it never creates an OpenTelemetry span context.
func ExtractIncoming(ctx context.Context) context.Context {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return ctx
	}
	carrier := propagation.HeaderCarrier{}
	for _, key := range []string{"traceparent", "tracestate"} {
		if values := md.Get(key); len(values) > 0 {
			carrier.Set(key, values[0])
		}
	}
	ctx = otel.GetTextMapPropagator().Extract(ctx, carrier)
	if trace.SpanContextFromContext(ctx).IsValid() {
		return ctx
	}
	if values := md.Get(auth.HeaderTraceID); len(values) == 1 && values[0] != "" {
		return context.WithValue(ctx, correlationContextKey{}, values[0])
	}
	return ctx
}

// CorrelationID returns the compatibility trace identifier only when no valid
// W3C span context was available during extraction.
func CorrelationID(ctx context.Context) (string, bool) {
	id, ok := ctx.Value(correlationContextKey{}).(string)
	return id, ok
}
