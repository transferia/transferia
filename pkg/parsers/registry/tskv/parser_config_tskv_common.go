package tskv

import (
	"github.com/transferia/transferia/pkg/abstract"
)

type ParserConfigTSKVCommon struct {
	// common parameters - for lb/kafka/yds/eventhub
	Fields             []abstract.ColSchema
	SchemaResourceName string // for the case, when logfeller-schema-id. Only for internal installation!

	NullKeysAllowed      bool // (title: "Использовать значение NULL в ключевых столбцах", "Разрешить NULL в ключевых колонках")
	AddRest              bool // (title: "Добавить неразмеченные столбцы", usage: "Поля, отсутствующие в схеме, попадут в колонку _rest")
	UnescapeStringValues bool
}

func (c *ParserConfigTSKVCommon) IsNewParserConfig() {}

func (c *ParserConfigTSKVCommon) IsAppendOnly() bool {
	return true
}

func (c *ParserConfigTSKVCommon) Validate() error {
	return nil
}
