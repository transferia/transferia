package typesystem

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/transferia/transferia/pkg/abstract"
)

func Doc(typ abstract.ProviderType, title string) string {
	return fmt.Sprintf(
		`## Type System Definition for %s

%s

%s`,
		title,
		BuildSourceDoc(typ, title),
		BuildTargetDoc(typ, title),
	)
}

func BuildSourceDoc(typ abstract.ProviderType, title string) string {
	rules := RuleFor(typ)
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(`
### %[1]s Source Type Mapping

| %[1]s TYPES | TRANSFER TYPE |
| --- | ----------- |
`, title))
	for _, typ := range SupportedTypes() {
		var row []string

		var types []string
		for orTyp, dtTyp := range rules.Source {
			if dtTyp == typ {
				types = append(types, orTyp)
			}
		}
		sort.Strings(types)
		if len(types) > 0 {
			row = []string{strings.Join(types, "<br/>"), string(typ)}
		} else {
			row = []string{"—", string(typ)}
		}
		buf.WriteString(fmt.Sprintf("|%s|\n", strings.Join(row, "|")))
	}
	return buf.String()
}

func BuildTargetDoc(typ abstract.ProviderType, title string) string {
	rules := RuleFor(typ)
	if rules.Target == nil {
		return fmt.Sprintf(`### %s Target Type Mapping Not Specified
`, title)
	}
	buf := &bytes.Buffer{}
	buf.WriteString(fmt.Sprintf(`
### %[1]s Target Type Mapping

| TRANSFER TYPE | %[1]s TYPES |
| --- | ----------- |
`, title))
	for _, typ := range SupportedTypes() {
		var row []string
		if targetType, ok := rules.Target[typ]; ok && len(targetType) > 0 {
			row = []string{string(typ), targetType}

		} else {
			row = []string{string(typ), "—"}
		}
		buf.WriteString(fmt.Sprintf("|%s|\n", strings.Join(row, "|")))
	}
	return buf.String()
}
