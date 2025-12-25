package packer

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
	debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"
)

func makeSubjectName(
	tableID abstract.TableID,
	topic string, // full topic name of prefix
	writeIntoOneTopic bool,
	isKeyProcessor bool,
	subjectNameStrategy string,
) string {
	var topicName string
	recordName := fmt.Sprintf("%s.%s.%s", topic, tableID.Namespace, tableID.Name)

	if writeIntoOneTopic {
		topicName = topic
	} else {
		topicName = recordName
	}

	if isKeyProcessor {
		recordName += ".Key"
	} else {
		recordName += ".Envelope"
	}
	switch subjectNameStrategy {
	case debeziumparameters.SubjectTopicRecordNameStrategy:
		return topicName + "-" + recordName
	case debeziumparameters.SubjectRecordNameStrategy:
		return recordName
	case debeziumparameters.SubjectTopicNameStrategy:
		fallthrough
	default:
		if isKeyProcessor {
			topicName += "-key"
		} else {
			topicName += "-value"
		}
		return topicName
	}
}

func tableSchemaKey(in *abstract.ChangeItem) string {
	builder := strings.Builder{}
	builder.WriteString(in.TableID().String())
	builder.WriteString("|")
	for _, el := range in.TableSchema.Columns() {
		builder.WriteString(el.ColumnName)
		builder.WriteString(":")
		builder.WriteString(el.DataType)
		builder.WriteString(":")
		builder.WriteString(el.OriginalType)
		builder.WriteString(":")
		builder.WriteString(strconv.FormatBool(el.PrimaryKey))
		builder.WriteString("|")
	}
	return builder.String()
}
