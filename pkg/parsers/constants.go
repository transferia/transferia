package parsers

import (
	"github.com/transferria/transferria/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

var (
	ErrParserColumns = []string{"_partition", "_offset", "_error", "data"}
	ErrParserSchema  = abstract.NewTableSchema([]abstract.ColSchema{
		{ColumnName: "_partition", DataType: string(ytschema.TypeString), PrimaryKey: true},
		{ColumnName: "_offset", DataType: string(ytschema.TypeUint64), PrimaryKey: true},
		{ColumnName: "_error", DataType: string(ytschema.TypeBytes)},
		{ColumnName: "data", DataType: string(ytschema.TypeBytes)},
	})
)

const (
	SyntheticTimestampCol  = "_timestamp"
	SyntheticPartitionCol  = "_partition"
	SyntheticOffsetCol     = "_offset"
	SyntheticIdxCol        = "_idx"
	SystemLbCtimeCol       = "_lb_ctime"
	SystemLbWtimeCol       = "_lb_wtime"
	SystemLbExtraColPrefix = "_lb_extra_"
)
