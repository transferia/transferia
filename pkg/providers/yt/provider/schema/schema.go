package schema

import (
	"context"
	"slices"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yt_table "github.com/transferia/transferia/pkg/providers/yt/provider/table"
	yt_provider_types "github.com/transferia/transferia/pkg/providers/yt/provider/types"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/yt"
)

func AddRowIdxColumn(tbl yt_table.YtTable, colName string) {
	cl := ytschema.Column{
		Name:        colName,
		Type:        ytschema.TypeInt64,
		Required:    true,
		ComplexType: nil,
		SortOrder:   ytschema.SortAscending,
	}
	tbl.AddColumn(yt_table.NewColumn(cl.Name, ytschema.TypeInt64, cl.Type, cl, false))
}

// FromRawSchema is the pure builder that wraps an already-fetched YT schema
// into a yt_table.YtTable. Load is the fetch+wrap path (by node id inside a
// snapshot TX); callers that already have the schema through a path lookup
// — e.g. the preflight TableList that seeds SnapshotLoader.schemaCache
// before BeginSnapshot — can call FromRawSchema directly and stay on the
// same code path as everyone else (including subsequent AddRowIdxColumn).
func FromRawSchema(origName string, sch ytschema.Schema, includeCols []string) (yt_table.YtTable, error) {
	if len(sch.Columns) == 0 {
		return nil, xerrors.Errorf("tables with empty schema are not supported (table=%s)", origName)
	}

	t := yt_table.NewTable(origName)
	for _, cl := range sch.Columns {
		if len(includeCols) > 0 && !slices.Contains(includeCols, cl.Name) {
			continue
		}
		ytType, isOptional := yt_provider_types.UnwrapOptional(cl.ComplexType)
		primType, err := yt_provider_types.Resolve(ytType)
		if err != nil {
			return nil, xerrors.Errorf("unable to resolve yt type to base type: %w", err)
		}
		if ytType == ytschema.TypeNull {
			// A null-typed column can only ever contain nulls, even when YT marks it required.
			isOptional = true
		}
		t.AddColumn(yt_table.NewColumn(cl.Name, primType, ytType, cl, isOptional))
	}

	return t, nil
}

func Load(ctx context.Context, ytc yt.Client, txID yt.TxID, nodeID yt.NodeID, origName string, includeCols []string) (yt_table.YtTable, error) {
	var sch ytschema.Schema
	if err := ytc.GetNode(ctx, nodeID.YPath().Attr("schema"), &sch, &yt.GetNodeOptions{
		TransactionOptions: &yt.TransactionOptions{TransactionID: txID},
	}); err != nil {
		return nil, xerrors.Errorf("unable to get table %s (%s) schema: %w", origName, nodeID.String(), err)
	}
	return FromRawSchema(origName, sch, includeCols)
}
