package tasks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

func TestSnapshotLoader_doUploadTablesV2(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table1",
		"schema1.table2",
		"schema2.*",
	}}

	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := snapshotLoader.doUploadTablesV2(ctx, nil, NewLocalTablePartProvider().TablePartProvider())
	require.NoError(t, err)
}
