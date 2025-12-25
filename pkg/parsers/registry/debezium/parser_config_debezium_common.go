package debezium

type ParserConfigDebeziumCommon struct {
	SchemaRegistryURL string
	SkipAuth          bool
	Username          string
	Password          string
	TLSFile           string
	NamespaceID       string // when specified, all other connection settings are ignored
}

func (c *ParserConfigDebeziumCommon) IsNewParserConfig() {}

func (c *ParserConfigDebeziumCommon) IsAppendOnly() bool {
	return false
}

func (c *ParserConfigDebeziumCommon) Validate() error {
	return nil
}

func (c *ParserConfigDebeziumCommon) YSRNamespaceID() string {
	return c.NamespaceID
}
