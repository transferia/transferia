package tablequery

import (
	"context"

	"github.com/transferia/transferia/pkg/abstract"
)

type TableQueryable interface {
	LoadQueryTable(ctx context.Context, table TableQuery, pusher abstract.Pusher) error
}
