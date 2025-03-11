package lib

import (
	"github.com/transferria/transferria/pkg/abstract"
	"go.ytsaurus.tech/yt/go/schema"
)

type logfellerSchema struct { //nolint:golint,unused
	Name     string `yson:"name"`
	Required bool   `yson:"required"`
	Type     string `yson:"type"`
}

func asColSchema(raw []logfellerSchema) []abstract.ColSchema { //nolint
	res := make([]abstract.ColSchema, len(raw))
	for i, v := range raw {
		res[i] = abstract.NewColSchema(v.Name, schema.Type(v.Type), false)
		res[i].PrimaryKey = v.Required
		if v.Name == "_logfeller_timestamp" {
			res[i].PrimaryKey = true
		}
	}
	return res
}
