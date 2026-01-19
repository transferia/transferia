package confluentschemaregistry

import (
	"github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry/table_name_policy"
)

type ParserConfigConfluentSchemaRegistryLb struct {
	SchemaRegistryURL string
	SkipAuth          bool
	Username          string
	Password          string
	TLSFile           string
	NamespaceID       string // when specified, all other connection settings are ignored

	IsGenerateUpdates bool
	TableNamePolicy   table_name_policy.TableNamePolicy
}

func (c *ParserConfigConfluentSchemaRegistryLb) IsNewParserConfig() {}

func (c *ParserConfigConfluentSchemaRegistryLb) IsAppendOnly() bool {
	return !c.IsGenerateUpdates
}

func (c *ParserConfigConfluentSchemaRegistryLb) Validate() error {
	return nil
}

func (c *ParserConfigConfluentSchemaRegistryLb) YSRNamespaceID() string {
	return c.NamespaceID
}
