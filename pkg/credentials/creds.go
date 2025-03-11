package credentials

import (
	"context"

	"github.com/golang/protobuf/ptypes/timestamp"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/library/go/core/log"
)

type Credentials interface {
	Token(context.Context) (string, error)
	ExpiresAt() *timestamp.Timestamp
}

var NewServiceAccountCreds = func(logger log.Logger, serviceAccountID string) (Credentials, error) {
	return nil, xerrors.New("not implemented")
}

var NewIamCreds = func(logger log.Logger) (Credentials, error) {
	return nil, xerrors.Errorf("not implemented")
}
