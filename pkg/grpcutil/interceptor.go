package grpcutil

import (
	"context"

	"google.golang.org/grpc"
)

type UnaryServerInterceptor interface {
	Intercept(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error)
}

type StreamServerInterceptor interface {
	InterceptStream(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error
}
