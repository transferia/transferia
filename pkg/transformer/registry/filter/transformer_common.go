package filter

import (
	"fmt"

	"github.com/transferia/transferia/pkg/abstract"
)

func TableFqtnVariants(tID abstract.TableID) []string {
	var fullName string
	if tID.Namespace == "" {
		fullName = tID.Name
	} else {
		fullName = fmt.Sprintf("%s.%s", tID.Namespace, tID.Name)
	}

	return []string{
		fullName,
		tID.Fqtn(),
	}
}

func MatchAnyTableNameVariant(filter Filter, tableID abstract.TableID) bool {
	if filter.Empty() {
		return true
	}
	for _, name := range TableFqtnVariants(tableID) {
		if filter.Match(name) {
			return true
		}
	}
	return false
}

func copyOldKeys(original *abstract.OldKeysType) abstract.OldKeysType {
	c := abstract.OldKeysType{
		KeyNames:  make([]string, len(original.KeyNames)),
		KeyTypes:  make([]string, len(original.KeyTypes)),
		KeyValues: make([]interface{}, len(original.KeyValues)),
	}
	copy(c.KeyNames, original.KeyNames)
	copy(c.KeyTypes, original.KeyTypes)
	copy(c.KeyValues, original.KeyValues)
	return c
}
