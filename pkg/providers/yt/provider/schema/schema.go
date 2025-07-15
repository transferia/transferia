package schema

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	basetypes "github.com/transferia/transferia/pkg/base/types"
	"github.com/transferia/transferia/pkg/providers/yt/provider/table"
	"github.com/transferia/transferia/pkg/providers/yt/provider/types"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yt"
)

func AddRowIdxColumn(tbl table.YtTable, colName string) {
	cl := schema.Column{
		Name:        colName,
		Type:        schema.TypeInt64,
		Required:    true,
		ComplexType: nil,
		SortOrder:   schema.SortAscending,
	}
	tbl.AddColumn(table.NewColumn(cl.Name, basetypes.NewInt64Type(), cl.Type, cl, false))
}

func Load(ctx context.Context, ytc yt.Client, txID yt.TxID, nodeID yt.NodeID, origName string) (table.YtTable, error) {
	var sch schema.Schema
	if err := ytc.GetNode(ctx, nodeID.YPath().Attr("schema"), &sch, &yt.GetNodeOptions{
		TransactionOptions: &yt.TransactionOptions{TransactionID: txID},
	}); err != nil {
		return nil, xerrors.Errorf("unable to get table %s (%s) schema: %w", origName, nodeID.String(), err)
	}

	if len(sch.Columns) == 0 {
		return nil, xerrors.Errorf("tables with empty schema are not supported (table=%s/%s)", origName, nodeID.String())
	}

	t := table.NewTable(origName)
	for _, cl := range sch.Columns {
		ytType, isOptional := types.UnwrapOptional(cl.ComplexType)
		typ, err := types.Resolve(ytType)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve yt type to base type: %w", err)
		}
		t.AddColumn(table.NewColumn(cl.Name, typ, ytType, cl, isOptional))
	}

	return t, nil
}
