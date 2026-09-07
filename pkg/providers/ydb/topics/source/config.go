package topicsource

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract/model"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
)

const defaultMaxMemory = 100 * 1024 * 1024

type Config struct {
	Connection topiccommon.ConnectionConfig

	Topics      []string
	Consumer    string
	ReaderOpts  ReaderOptions
	Transformer *model.DataTransformOptions

	IsYDBTopicSink             bool
	AllowTTLRewind             bool
	ParseQueueParallelism      int
	UseFullTopicNameForParsing bool
}

type ReaderOptions struct {
	ReadOnlyLocal bool

	MaxMemory           int
	MaxReadSize         uint32
	MaxReadMessageCount uint32
	MaxTimeLag          time.Duration
	MinReadInterval     time.Duration
}

func NewDefaultReaderOptions() ReaderOptions {
	return ReaderOptions{
		ReadOnlyLocal:       true,
		MaxMemory:           0,
		MaxReadSize:         0,
		MaxReadMessageCount: 0,
		MaxTimeLag:          0,
		MinReadInterval:     0,
	}
}

func (o ReaderOptions) MaxMemoryOrDefault() int {
	if o.MaxMemory == 0 {
		return defaultMaxMemory
	}
	return o.MaxMemory
}
