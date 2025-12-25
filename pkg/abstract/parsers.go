package abstract

type TimestampCol struct {
	Col    string
	Format string
}

type TableSplitter struct {
	Columns []string
}

type LfLineSplitter string

const (
	LfLineSplitterNewLine       LfLineSplitter = "\n"
	LfLineSplitterDoNotSplit    LfLineSplitter = "do-not-split"
	LfLineSplitterProtoseq      LfLineSplitter = "protoseq"
	LfLineSplitterOtelLogsProto LfLineSplitter = "otel-logs-proto"
)
