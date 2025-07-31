package grpcutil

import (
	"context"
	"crypto/tls"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
)

type authorizationHeader string

func (h authorizationHeader) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"authorization": string(h)}, nil
}

func (h authorizationHeader) RequireTransportSecurity() bool {
	return false
}

type ConnectionOptions struct {
	Plaintext       bool
	AuthHeaderValue string
}

func (c *ConnectionOptions) DialOptions() []grpc.DialOption {
	options := []grpc.DialOption{grpc.WithBlock()}

	if c.Plaintext {
		options = append(options, grpc.WithTransportCredentials(insecure.NewCredentials()))
	} else {
		options = append(options, grpc.WithTransportCredentials(credentials.NewTLS(&tls.Config{})))
	}

	if c.AuthHeaderValue != "" {
		options = append(options, grpc.WithPerRPCCredentials(authorizationHeader(c.AuthHeaderValue)))
	}

	return options
}
