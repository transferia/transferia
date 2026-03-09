package typefitting

import (
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/chrecipe"
)

var (
	//nolint:exhaustivestruct
	source = model.MockSource{}
	target = *chrecipe.MustTarget(chrecipe.WithDatabase("test"), chrecipe.WithInitFile("init.sql"))
)

func init() {
	source.WithDefaults()
	target.WithDefaults()
}
