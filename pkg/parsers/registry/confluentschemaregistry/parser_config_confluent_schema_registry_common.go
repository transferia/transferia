package confluentschemaregistry

type ParserConfigConfluentSchemaRegistryCommon struct {
	SchemaRegistryURL string
	SkipAuth          bool
	Username          string
	Password          string
	TLSFile           string

	IsGenerateUpdates bool
}

func (c *ParserConfigConfluentSchemaRegistryCommon) IsNewParserConfig() {}

func (c *ParserConfigConfluentSchemaRegistryCommon) IsAppendOnly() bool {
	return !c.IsGenerateUpdates
}

func (c *ParserConfigConfluentSchemaRegistryCommon) Validate() error {
	return nil
}
