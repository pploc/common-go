package observability

import (
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"google.golang.org/grpc"
)

// ServerStatsHandler returns the official OpenTelemetry gRPC server stats
// handler. Install it on grpc.NewServer with grpc.StatsHandler.
func ServerStatsHandler(options ...otelgrpc.Option) grpc.ServerOption {
	return grpc.StatsHandler(otelgrpc.NewServerHandler(options...))
}
