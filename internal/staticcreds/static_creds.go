package staticcreds

import (
	"context"

	"github.com/golang/protobuf/ptypes/timestamp"
)

// TokenCredentials is an interface that contains options used to authorize a
// client.
type TokenCredentials interface {
	Token(context.Context) (string, error)
}

type StaticCreds struct {
	AuthToken string
}

func (c *StaticCreds) ExpiresAt() *timestamp.Timestamp {
	return nil
}

func (c *StaticCreds) Token(context.Context) (string, error) {
	return c.AuthToken, nil
}

func New(token string) TokenCredentials {
	return &StaticCreds{AuthToken: token}
}
