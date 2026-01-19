package native

type ParserConfigNativeLb struct{}

func (c *ParserConfigNativeLb) IsNewParserConfig() {}

func (c *ParserConfigNativeLb) IsAppendOnly() bool {
	return false
}

func (c *ParserConfigNativeLb) Validate() error {
	return nil
}
