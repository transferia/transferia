//go:build cgo && oracle
// +build cgo,oracle

package common

import (
	_ "github.com/godror/godror"
	"github.com/jmoiron/sqlx"
	"github.com/transferia/transferia/pkg/providers/oracle"
)

//nolint:descriptiveerrors
func CreateConnection(config *oracle.OracleSource) (*sqlx.DB, error) {
	if connectionStr, err := GetConnectionString(config); err != nil {
		return nil, err
	} else {
		return sqlx.Open("godror", connectionStr)
	}
}
