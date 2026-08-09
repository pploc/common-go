package interceptor

import (
	"buf.build/go/protovalidate"
	"github.com/pploc/common-go/grpc/middleware"
	"github.com/pploc/common-go/observability"
	"google.golang.org/grpc"
)

// ServerOptions returns the canonical server wiring. Outer-to-inner order is recovery,
// W3C propagation, metrics, logging, error conversion, authentication, authorization,
// then Protovalidate. Logging and metrics observe final gRPC statuses.
func ServerOptions(authOptions AuthOptions, policy middleware.Policy, metrics *observability.Metrics, validator protovalidate.Validator) []grpc.ServerOption {
	if validator == nil {
		panic("interceptor: protovalidate validator is required")
	}
	if len(authOptions.PublicMethods) > 0 && policy != nil {
		policy = middleware.PublicMethods(authOptions.PublicMethods, policy)
	}
	return []grpc.ServerOption{
		observability.ServerStatsHandler(),
		grpc.ChainUnaryInterceptor(
			RecoveryUnary(metrics),
			PropagationUnary(),
			MetricsUnary(metrics),
			LoggingUnary(),
			ErrorUnary(),
			AuthUnary(authOptions),
			AuthorizationUnary(policy),
			ValidationUnary(validator),
		),
		grpc.ChainStreamInterceptor(
			RecoveryStream(metrics),
			PropagationStream(),
			MetricsStream(metrics),
			LoggingStream(),
			ErrorStream(),
			AuthStream(authOptions),
			AuthorizationStream(policy),
			ValidationStream(validator),
		),
	}
}
