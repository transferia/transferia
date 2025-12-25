package grpc

import (
	"context"
	"io"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// MockServerStream is a mock implementation of grpc.ServerStream for testing.
type MockServerStream[T any] struct {
	grpc.ServerStream
	ctx      context.Context
	sentMsgs []any
	sentVals []T
}

func NewMockServerStream[T any](ctx context.Context) *MockServerStream[T] {
	if ctx == nil {
		ctx = context.Background()
	}
	return &MockServerStream[T]{
		ctx:      ctx,
		sentMsgs: make([]any, 0),
	}
}

func (m *MockServerStream[T]) Send(val T) error {
	m.sentVals = append(m.sentVals, val)
	return nil
}

func (m *MockServerStream[T]) GetSentVals() []T {
	return m.sentVals
}

func (m *MockServerStream[T]) Context() context.Context {
	return m.ctx
}

func (m *MockServerStream[T]) SendMsg(msg any) error {
	return nil
}

func (m *MockServerStream[T]) RecvMsg(msg any) error {
	return io.EOF
}

func (m *MockServerStream[T]) SetHeader(metadata.MD) error {
	return nil
}

func (m *MockServerStream[T]) SendHeader(metadata.MD) error {
	return nil
}

func (m *MockServerStream[T]) SetTrailer(metadata.MD) {}
