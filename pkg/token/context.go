package token

import (
	"context"

	"github.com/transferia/transferia/pkg/contextutil"
)

const (
	ErrUnableToGetToken = "unable to get token"
)

var (
	tokenCtxKey = contextutil.NewContextKey()
)

func WithToken(ctx context.Context, token string) context.Context {
	return context.WithValue(ctx, tokenCtxKey, token)
}

func FromContext(ctx context.Context) (string, bool) {
	value, ok := ctx.Value(tokenCtxKey).(string)
	return value, ok
}
