package observability

import (
	"context"
	"testing"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/metadata"
)

func TestExtractIncomingPrefersW3C(t *testing.T) {
	previous := otel.GetTextMapPropagator()
	otel.SetTextMapPropagator(propagation.TraceContext{})
	t.Cleanup(func() { otel.SetTextMapPropagator(previous) })
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs(
		"traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01",
		"x-trace-id", "fallback",
	))
	ctx = ExtractIncoming(ctx)
	if !trace.SpanContextFromContext(ctx).IsValid() {
		t.Fatal("expected W3C span context")
	}
	if _, ok := CorrelationID(ctx); ok {
		t.Fatal("fallback correlation must not override W3C context")
	}
}

func TestExtractIncomingUsesFallbackOnlyWithoutW3C(t *testing.T) {
	ctx := metadata.NewIncomingContext(context.Background(), metadata.Pairs("x-trace-id", "fallback"))
	ctx = ExtractIncoming(ctx)
	if id, ok := CorrelationID(ctx); !ok || id != "fallback" {
		t.Fatalf("unexpected correlation ID: %q, %t", id, ok)
	}
}
