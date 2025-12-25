package debezium

type ParserConfigDebeziumLb struct {
	SchemaRegistryURL string
	SkipAuth          bool
	Username          string
	Password          string
	TLSFile           string
	NamespaceID       string // when specified, all other connection settings are ignored
}

func (c *ParserConfigDebeziumLb) IsNewParserConfig() {}

func (c *ParserConfigDebeziumLb) IsAppendOnly() bool {
	return false
}

func (c *ParserConfigDebeziumLb) Validate() error {
	return nil
}

func (c *ParserConfigDebeziumLb) YSRNamespaceID() string {
	return c.NamespaceID
}
