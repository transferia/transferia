package confluent

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract"
)

var _ abstract.Expirer = (*YSRConnectionParams)(nil)

type YSRConnectionParams struct {
	URL                   string
	Username              string
	Password              string
	CredentialsExpiration time.Time
}

func (y *YSRConnectionParams) ExpiresAt() time.Time {
	return y.CredentialsExpiration
}
