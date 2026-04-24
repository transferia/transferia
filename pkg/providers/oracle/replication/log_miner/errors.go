//go:build cgo && oracle
// +build cgo,oracle

package log_miner

import (
	"strings"

	"github.com/godror/godror"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

func IsContainsError(err error, skipCodes []string) bool {
	oraErr := new(godror.OraErr)
	if xerrors.As(err, &oraErr) {
		for _, errorCode := range skipErrorCodes {
			if strings.Contains(oraErr.Message(), errorCode) {
				return true
			}
		}
	}
	return false
}
