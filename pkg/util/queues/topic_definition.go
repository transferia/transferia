package util

import (
	"fmt"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
)

type TopicDefinition struct {
	FullPath   string
	PrefixPath string
}

func (d *TopicDefinition) bothEmpty() bool {
	return d.FullPath == "" && d.PrefixPath == ""
}

func (d *TopicDefinition) bothNotEmpty() bool {
	return d.FullPath != "" && d.PrefixPath != ""
}

func (d *TopicDefinition) isValid() error {
	if d.bothEmpty() {
		return xerrors.Errorf("there are empty TopicName & TopicPrefix - should be specified only one of them. TopicName: %s, TopicPrefix: %s", d.FullPath, d.PrefixPath)
	}
	if d.bothNotEmpty() {
		return xerrors.Errorf("there are empty TopicName & TopicPrefix - should be specified only one of them. TopicName: %s, TopicPrefix: %s", d.FullPath, d.PrefixPath)
	}
	return nil
}

func NewTopicDefinition(topicName, topicPrefix string) (*TopicDefinition, error) {
	topic := &TopicDefinition{
		FullPath:   topicName,
		PrefixPath: topicPrefix,
	}
	if err := topic.isValid(); err != nil {
		return nil, xerrors.Errorf("unable to create TopicDefinition: %w", err)
	}
	return topic, nil
}

func GetTopicName(topic, topicPrefix string, tableID abstract.TablePartID) string {
	if topic != "" {
		return topic
	} else {
		return fmt.Sprintf("%s.%s.%s", topicPrefix, tableID.Namespace, tableID.Name)
	}
}
