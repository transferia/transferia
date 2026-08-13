package source

import (
	"github.com/transferia/transferia/pkg/abstract/model"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
)

type sourceConfig interface {
	model.Source

	topicSourceConfig(defaultConsumer string) *topicsource.Config
}
