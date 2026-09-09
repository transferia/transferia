package topicsource

import (
	"time"

	"github.com/transferia/transferia/pkg/abstract/model"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
)

const (
	defaultMaxMemory    = 100 * 1024 * 1024
	defaultMaxBatchSize = 100 * 1024 * 1024
)

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
	MaxMemory int

	PQv1     PQv1ReaderOptions
	TopicAPI TopicAPIReaderOptions
}

type PQv1ReaderOptions struct {
	ReadOnlyLocal bool

	MaxReadSize         uint32
	MaxReadMessageCount uint32
	MaxTimeLag          time.Duration
	MinReadInterval     time.Duration
}

type TopicAPIReaderOptions struct {
	MaxBatchSize         uint32
	MaxBatchMessageCount int
}

func (o ReaderOptions) MaxMemoryOrDefault() int {
	if o.MaxMemory == 0 {
		return defaultMaxMemory
	}
	return o.MaxMemory
}

func (o TopicAPIReaderOptions) MaxBatchSizeOrDefault() uint32 {
	if o.MaxBatchSize == 0 {
		return defaultMaxBatchSize
	}
	return o.MaxBatchSize
}

func NewDefaultReaderOptions() ReaderOptions {
	return ReaderOptions{
		MaxMemory: 0,
		PQv1: PQv1ReaderOptions{
			ReadOnlyLocal:       true,
			MaxReadSize:         0,
			MaxReadMessageCount: 0,
			MaxTimeLag:          0,
			MinReadInterval:     0,
		},
		TopicAPI: TopicAPIReaderOptions{
			MaxBatchSize:         0,
			MaxBatchMessageCount: 0,
		},
	}
}
