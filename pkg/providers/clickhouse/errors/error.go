package errors

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/errors/coded"
)

// full list of error codes here - https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
var NonRetryableCode = map[int32]bool{
	62:  true,
	349: true,
}

var UpdateToastsError = coded.Register("ch", "update_toast_error")

func IsClickhouseError(err error) bool {
	var exception *clickhouse.Exception
	return xerrors.As(err, &exception)
}

func IsFatalClickhouseError(err error) bool {
	exception := new(clickhouse.Exception)
	if !xerrors.As(err, &exception) {
		return false
	}
	return NonRetryableCode[exception.Code]
}
