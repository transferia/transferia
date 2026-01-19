package raw2table

type ParserConfigRawToTableLb struct{}

func (c *ParserConfigRawToTableLb) IsNewParserConfig() {}

func (c *ParserConfigRawToTableLb) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigRawToTableLb) Validate() error {
	return nil
}
