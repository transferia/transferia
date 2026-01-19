package blank

type ParserConfigBlankLb struct{}

func (c *ParserConfigBlankLb) IsNewParserConfig() {}

func (c *ParserConfigBlankLb) IsAppendOnly() bool {
	return false
}

func (c *ParserConfigBlankLb) Validate() error {
	return nil
}
