package postgres

import _ "embed"

var (
	//go:embed dump/array_types.sql
	array []byte
	//go:embed dump/date_types.sql
	date []byte
	//go:embed dump/geom_types.sql
	geom []byte
	//go:embed dump/numeric_types.sql
	numeric []byte
	//go:embed dump/text_types.sql
	text []byte
	//go:embed dump/user_types.sql
	user []byte
	//go:embed dump/wtf_types.sql
	wtf []byte
)

var TableSQLs = map[string]string{
	"public.array_types":   string(array),
	"public.date_types":    string(date),
	"public.geom_types":    string(geom),
	"public.numeric_types": string(numeric),
	"public.text_types":    string(text),
	"public.user_types":    string(user),
	"public.wtf_types":     string(wtf),
}
