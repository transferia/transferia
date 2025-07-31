package yc

import (
	"context"

	"google.golang.org/grpc"
)

type Resolver interface {
	ID() string
	Err() error

	Run(context.Context, *SDK, ...grpc.CallOption) error
}
