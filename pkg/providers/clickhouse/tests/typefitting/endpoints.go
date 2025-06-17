//go:build !disable_clickhouse_provider

package typefitting

import (
	"github.com/transferia/transferia/pkg/abstract/model"
	chrecipe "github.com/transferia/transferia/pkg/providers/clickhouse/recipe"
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
