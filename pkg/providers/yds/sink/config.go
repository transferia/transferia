package sink

import (
	"github.com/transferia/transferia/pkg/abstract/model"
	ydb_topics_sink "github.com/transferia/transferia/pkg/providers/ydb/topics/sink"
)

type sinkConfig interface {
	model.Destination

	prepareConfig() error
	topicSinkConfig() (*ydb_topics_sink.Config, error)
}
