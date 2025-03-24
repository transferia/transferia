package tasks

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	mockstorage "github.com/transferia/transferia/tests/helpers/mock_storage"
)

func TestCheckIncludeDirectives_DataObjects_NoError(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{
		"schema1.table1",
		"schema2.*",
	}}
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table2",
		"schema3.*",
	}} // must be ignored
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "schema1"},
		{Name: "table1", Schema: "schema2"},
	}
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.CheckIncludeDirectives(tables)
	require.NoError(t, err)
}

func TestCheckIncludeDirectives_DataObjects_Error(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{
		"schema1.table1",
		"schema1.table2",
		"schema2.*",
	}}
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table3",
		"schema3.*",
	}} // must be ignored
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "schema1"},
	}
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.CheckIncludeDirectives(tables)
	require.Error(t, err)
	require.Equal(t, "some tables from include list are missing in the source database: [schema1.table2 schema2.*]", err.Error())
}

func TestCheckIncludeDirectives_DataObjects_FqtnVariants(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.DataObjects = &model.DataObjects{IncludeObjects: []string{
		"schema1.table1",
		"\"schema1\".table1",
		"schema1.\"table1\"",
		"\"schema1\".\"table1\"",
		"schema2.*",
		"\"schema2\".*",
	}}
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "schema1"},
		{Name: "table1", Schema: "schema2"},
	}
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.CheckIncludeDirectives(tables)
	require.NoError(t, err)
}

func TestCheckIncludeDirectives_Src_NoError(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table1",
		"schema2.*",
	}}
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "schema1"},
		{Name: "table1", Schema: "schema2"},
	}
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.CheckIncludeDirectives(tables)
	require.NoError(t, err)
}

func TestCheckIncludeDirectives_Src_Error(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table1",
		"schema1.table2",
		"schema2.*",
	}}
	tables := []abstract.TableDescription{
		{Name: "table1", Schema: "schema1"},
	}
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.CheckIncludeDirectives(tables)
	require.Error(t, err)
	require.Equal(t, "some tables from include list are missing in the source database: [schema1.table2 schema2.*]", err.Error())
}

func TestDoUploadTables_CtxCancelledNoErr(t *testing.T) {
	transfer := new(model.Transfer)
	transfer.Src = &postgres.PgSource{DBTables: []string{
		"schema1.table1",
		"schema1.table2",
		"schema2.*",
	}}

	storage := &mockstorage.MockStorage{}
	snapshotLoader := NewSnapshotLoader(&FakeControlplane{}, "test-operation", transfer, solomon.NewRegistry(nil))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := snapshotLoader.DoUploadTables(ctx, storage, snapshotLoader.GetLocalTablePartProvider())
	require.NoError(t, err)
}
