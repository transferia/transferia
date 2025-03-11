package mysql

import (
	"github.com/go-sql-driver/mysql"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
)

var (
	CodeSyntax   = coded.Register("mysql", "incorrect_syntax")
	CodeDeadlock = coded.Register("mysql", "deadlock")
)

func IsErrorCode(err error, errNumber uint16) bool {
	var mErr = new(mysql.MySQLError)
	if !xerrors.As(err, &mErr) {
		return false
	}
	return mErr.Number == errNumber
}

func IsErrorCodes(err error, codes map[int]bool) bool {
	var mErr = new(mysql.MySQLError)
	if !xerrors.As(err, &mErr) {
		return false
	}
	return codes[int(mErr.Number)]
}
