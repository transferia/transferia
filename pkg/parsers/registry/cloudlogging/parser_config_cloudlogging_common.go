package cloudlogging

type ParserConfigCloudLoggingCommon struct{}

func (c *ParserConfigCloudLoggingCommon) IsNewParserConfig() {}

func (c *ParserConfigCloudLoggingCommon) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigCloudLoggingCommon) Validate() error {
	return nil
}
