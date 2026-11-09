//go:build !cgo || !oracle
// +build !cgo !oracle

package common

import (
	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/oracle"
)

func CreateConnection(config *oracle.OracleSource) (*sqlx.DB, error) {
	return nil, xerrors.New("Oracle provider is not presented in this build")
}
