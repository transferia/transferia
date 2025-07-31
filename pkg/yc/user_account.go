package yc

import (
	"context"

	iampb "github.com/transferia/transferia/cloud/bitbucket/public-api/yandex/cloud/iam/v1"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"google.golang.org/grpc"
)

type UserAccountServiceClient struct {
	getConn func(ctx context.Context) (*grpc.ClientConn, error)
}

func NewUserAccountServiceClient(getConn func(ctx context.Context) (*grpc.ClientConn, error)) *UserAccountServiceClient {
	return &UserAccountServiceClient{getConn: getConn}
}

func (c *UserAccountServiceClient) GetLogin(ctx context.Context, userAccountID string, opts ...grpc.CallOption) (string, error) {
	conn, err := c.getConn(ctx)
	if err != nil {
		return "", xerrors.Errorf("unable to get connection: %w", err)
	}
	request := &iampb.GetUserAccountRequest{UserAccountId: userAccountID}
	client := iampb.NewUserAccountServiceClient(conn)
	userAccount, err := client.Get(ctx, request, opts...)
	if err != nil {
		return "", xerrors.Errorf("unable to get user account: %w", err)
	}
	switch x := userAccount.UserAccount.(type) {
	case *iampb.UserAccount_YandexPassportUserAccount:
		return x.YandexPassportUserAccount.Login, nil
	default:
		return "", xerrors.Errorf("unsupported account type: %T", x)
	}
}
