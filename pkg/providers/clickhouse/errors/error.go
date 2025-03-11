package errors

import (
	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/errors/coded"
)

var (
	// full list of error codes here - https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/ErrorCodes.cpp
	RetryableCode = map[int32]bool{
		16:  true, // NO_SUCH_COLUMN_IN_TABLE
		21:  true, // CANNOT_READ_ALL_DATA_FROM_TAB_SEPARATED_INPUT
		22:  true, // CANNOT_PARSE_ALL_VALUE_FROM_TAB_SEPARATED_INPUT
		23:  true, // CANNOT_READ_FROM_ISTREAM
		24:  true, // CANNOT_WRITE_TO_OSTREAM
		30:  true, // CANNOT_READ_SIZE_OF_COMPRESSED_CHUNK
		31:  true, // CANNOT_READ_COMPRESSED_CHUNK
		32:  true, // ATTEMPT_TO_READ_AFTER_EOF
		33:  true, // CANNOT_READ_ALL_DATA
		95:  true, // CANNOT_READ_FROM_SOCKET
		96:  true, // CANNOT_WRITE_TO_SOCKET
		97:  true, // CANNOT_READ_ALL_DATA_FROM_CHUNKED_INPUT
		98:  true, // CANNOT_WRITE_TO_EMPTY_BLOCK_OUTPUT_STREAM
		101: true, // UNEXPECTED_PACKET_FROM_CLIENT
		102: true, // UNEXPECTED_PACKET_FROM_SERVER
		114: true, // CANNOT_CLOCK_GETTIME
		159: true, // TIMEOUT_EXCEEDED
		160: true, // TOO_SLOW
		164: true, // READONLY
		170: true, // BAD_GET
		198: true, // DNS_ERROR
		201: true, // QUOTA_EXPIRED
		202: true, // TOO_MANY_SIMULTANEOUS_QUERIES
		203: true, // NO_FREE_CONNECTION
		204: true, // CANNOT_FSYNC
		209: true, // SOCKET_TIMEOUT
		210: true, // NETWORK_ERROR
		212: true, // UNKNOWN_LOAD_BALANCING
		214: true, // CANNOT_STATVFS
		225: true, // NO_ZOOKEEPER
		236: true, // ABORTED
		241: true, // MEMORY_LIMIT_EXCEEDED
		242: true, // TABLE_IS_READ_ONLY
		243: true, // NOT_ENOUGH_SPACE
		244: true, // UNEXPECTED_ZOOKEEPER_ERROR
		252: true, // TOO_MANY_PARTS
		274: true, // AIO_READ_ERROR
		275: true, // AIO_WRITE_ERROR
		286: true, // UNSATISFIED_QUORUM_FOR_PREVIOUS_WRITE
		290: true, // LIMIT_EXCEEDED
		319: true, // UNKNOWN_STATUS_OF_INSERT
		394: true, // QUERY_WAS_CANCELLED
		412: true, // NETLINK_ERROR
		415: true, // ALL_REPLICAS_LOST
		416: true, // REPLICA_STATUS_CHANGED
		492: true, // ACCESS_ENTITY_NOT_FOUND
		499: true, // S3_ERROR
		999: true, // KEEPER_EXCEPTION
	}
)

var (
	UpdateToastsError = coded.Register("ch", "update_toast_error")
)

func IsClickhouseError(err error) bool {
	var exception *clickhouse.Exception
	return xerrors.As(err, &exception)
}

func IsFatalClickhouseError(err error) bool {
	var blockErr = new(proto.BlockError)
	if xerrors.As(err, &blockErr) && blockErr.Err != nil {
		_, dateOverflow := blockErr.Err.(*column.DateOverflowError)
		return dateOverflow
	}
	var exception = new(clickhouse.Exception)
	if !xerrors.As(err, &exception) {
		return false
	}
	return !RetryableCode[exception.Code]
}
