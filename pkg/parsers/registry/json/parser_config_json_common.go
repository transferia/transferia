package json

import (
	"github.com/transferia/transferia/pkg/abstract"
)

type ParserConfigJSONCommon struct {
	// common parameters - for lb/kafka/yds/eventhub
	Fields             []abstract.ColSchema
	SchemaResourceName string // for the case, when logfeller-schema-id. Only for internal installation!

	NullKeysAllowed      bool // (title: "Использовать значение NULL в ключевых столбцах", "Разрешить NULL в ключевых колонках")
	AddRest              bool // (title: "Добавить неразмеченные столбцы", usage: "Поля, отсутствующие в схеме, попадут в колонку _rest")
	UnescapeStringValues bool

	// private option
	AddDedupeKeys     bool
	UseNumbersInAny   bool
	UnpackBytesBase64 bool
}

func (c *ParserConfigJSONCommon) IsNewParserConfig() {}

func (c *ParserConfigJSONCommon) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigJSONCommon) Validate() error {
	return nil
}
