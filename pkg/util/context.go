package util

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
)

type ContextKey string

var WrappedKey = ContextKey("wrapped")

type Values struct {
	ts time.Time
}

func (v Values) GetTS() time.Time {
	return v.ts
}

func ContextWithTimestamp(ctx context.Context, ts time.Time) context.Context {
	v := Values{
		ts: ts,
	}
	return context.WithValue(ctx, WrappedKey, v)
}

func GetTimestampFromContext(ctx context.Context) (time.Time, error) {
	var ts time.Time
	v := ctx.Value(WrappedKey)
	if v == nil {
		return ts, xerrors.Errorf("no wrapped values in context")
	}
	return v.(Values).GetTS(), nil
}

func GetTimestampFromContextOrNow(ctx context.Context) time.Time {
	st, err := GetTimestampFromContext(ctx)
	if err != nil || st.IsZero() {
		st = time.Now()
	}
	return st
}
