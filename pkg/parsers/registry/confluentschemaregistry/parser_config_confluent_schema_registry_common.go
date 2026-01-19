package confluentschemaregistry

import (
	"github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry/table_name_policy"
)

type ParserConfigConfluentSchemaRegistryCommon struct {
	SchemaRegistryURL string
	SkipAuth          bool
	Username          string
	Password          string
	TLSFile           string
	NamespaceID       string // when specified, all other connection settings are ignored

	IsGenerateUpdates bool
	TableNamePolicy   table_name_policy.TableNamePolicy
}

func (c *ParserConfigConfluentSchemaRegistryCommon) IsNewParserConfig() {}

func (c *ParserConfigConfluentSchemaRegistryCommon) IsAppendOnly() bool {
	return !c.IsGenerateUpdates
}

func (c *ParserConfigConfluentSchemaRegistryCommon) Validate() error {
	return nil
}

func (c *ParserConfigConfluentSchemaRegistryCommon) YSRNamespaceID() string {
	return c.NamespaceID
}
