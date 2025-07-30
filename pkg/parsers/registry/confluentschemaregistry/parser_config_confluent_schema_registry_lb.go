package confluentschemaregistry

type ParserConfigConfluentSchemaRegistryLb struct {
	SchemaRegistryURL string
	SkipAuth          bool
	Username          string
	Password          string
	TLSFile           string

	IsGenerateUpdates bool
}

func (c *ParserConfigConfluentSchemaRegistryLb) IsNewParserConfig() {}

func (c *ParserConfigConfluentSchemaRegistryLb) IsAppendOnly() bool {
	return !c.IsGenerateUpdates
}

func (c *ParserConfigConfluentSchemaRegistryLb) Validate() error {
	return nil
}
