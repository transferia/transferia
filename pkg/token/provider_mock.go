package token

import "context"

type providerMock struct {
	createToken func(ctx context.Context, oauthToken string) (string, error)
}

func NewProviderMock(createToken func(ctx context.Context, oauthToken string) (string, error)) Provider {
	return &providerMock{createToken: createToken}
}

func (p *providerMock) Close() error {
	return nil
}

func (p *providerMock) CreateToken(ctx context.Context, oauthToken, clientIP string) (string, error) {
	return p.createToken(ctx, oauthToken)
}
