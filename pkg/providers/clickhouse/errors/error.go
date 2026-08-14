package errors

import (
	"strings"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

const (
	tooManyPartsCode         = 252
	tooManyPartitionsMessage = "Too many partitions for single INSERT block"
)

// full list of error codes here - https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
var NonRetryableCode = map[int32]bool{
	62:  true,
	349: true,
}

func IsClickhouseError(err error) bool {
	var exception *clickhouse_go.Exception
	return xerrors.As(err, &exception)
}

func IsFatalClickhouseError(err error) bool {
	exception := new(clickhouse_go.Exception)
	if !xerrors.As(err, &exception) {
		return false
	}
	return NonRetryableCode[exception.Code]
}

// IsTooManyPartitionsError matches the "Too many partitions for single INSERT block" error,
// which is deterministic for a given batch: retrying without raising max_partitions_per_insert_block never helps.
// Code 252 (TOO_MANY_PARTS) is shared with the "Too many parts" merge backlog error,
// which is transient backpressure, hence the additional message check.
func IsTooManyPartitionsError(err error) bool {
	exception := new(clickhouse_go.Exception)
	if !xerrors.As(err, &exception) {
		return false
	}
	return exception.Code == tooManyPartsCode && strings.Contains(exception.Message, tooManyPartitionsMessage)
}
