package reader

import "github.com/transferia/transferia/pkg/abstract"

func DataTypes(columns abstract.TableColumns) []string {
	result := make([]string, len(columns))
	for i, column := range columns {
		result[i] = column.DataType
	}
	return result
}
