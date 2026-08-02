package auth

import (
	"context"
	"fmt"

	"google.golang.org/grpc/metadata"
)

// FromIncomingContext extracts validated claims from incoming gRPC metadata.
func FromIncomingContext(ctx context.Context, options Options) (Claims, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return Claims{}, fmt.Errorf("auth: incoming metadata is missing")
	}
	return ParseMetadata(md, options)
}
