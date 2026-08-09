package interceptor

import (
	"fmt"

	"buf.build/go/protovalidate"
	middleware "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/protovalidate"
	"google.golang.org/grpc"
)

// ValidationUnary validates inbound protobuf requests with Protovalidate.
func ValidationUnary(validator protovalidate.Validator) grpc.UnaryServerInterceptor {
	if validator == nil {
		panic("interceptor: protovalidate validator is required")
	}
	return middleware.UnaryServerInterceptor(validator)
}

// ValidationStream validates inbound streaming protobuf requests with Protovalidate.
func ValidationStream(validator protovalidate.Validator) grpc.StreamServerInterceptor {
	if validator == nil {
		panic("interceptor: protovalidate validator is required")
	}
	return middleware.StreamServerInterceptor(validator)
}

// NewValidator constructs the shared Protovalidate instance used by gRPC and Kafka.
func NewValidator() (protovalidate.Validator, error) {
	validator, err := protovalidate.New()
	if err != nil {
		return nil, fmt.Errorf("create protovalidate validator: %w", err)
	}
	return validator, nil
}
