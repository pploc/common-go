package interceptor

import (
	"github.com/pploc/common-go/grpc/middleware"
	"github.com/pploc/common-go/observability"
	"google.golang.org/grpc"
)

// ServerOptions returns the Phase 1 server wiring order: recovery, W3C
// propagation, logging, metrics, error conversion, authentication, then
// authorization. Error conversion is nested inside logging and metrics so both
// observe final gRPC statuses. Install the returned StatsHandler option with the
// returned unary and stream interceptors.
func ServerOptions(authOptions AuthOptions, policy middleware.Policy, metrics *observability.Metrics) []grpc.ServerOption {
	if len(authOptions.PublicMethods) > 0 && policy != nil {
		policy = middleware.PublicMethods(authOptions.PublicMethods, policy)
	}
	return []grpc.ServerOption{
		observability.ServerStatsHandler(),
		grpc.ChainUnaryInterceptor(
			RecoveryUnary(metrics),
			PropagationUnary(),
			MetricsUnary(metrics),
			ErrorUnary(),
			AuthUnary(authOptions),
			AuthorizationUnary(policy),
			LoggingUnary(),
		),
		grpc.ChainStreamInterceptor(
			RecoveryStream(metrics),
			PropagationStream(),
			MetricsStream(metrics),
			ErrorStream(),
			AuthStream(authOptions),
			AuthorizationStream(policy),
			LoggingStream(),
		),
	}
}
