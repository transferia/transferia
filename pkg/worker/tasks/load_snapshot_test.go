package tasks

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/util/jsonx"
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
	err := snapshotLoader.DoUploadTables(ctx, storage, NewLocalTablePartProvider().TablePartProvider())
	require.NoError(t, err)
}

func TestLocalTablePartProvider(t *testing.T) {
	ctx := context.Background()
	descs := []*abstract.TableDescription{
		{Schema: "schema-1", Name: "table-1", Filter: "a<5"},
		{Schema: "schema-1", Name: "table-1", Filter: "a>5"},
		{Schema: "schema-2", Name: "table-2"},
	}
	parts := []*model.OperationTablePart{}
	for _, desc := range descs {
		parts = append(parts, model.NewOperationTablePartFromDescription("dtjtest", desc))
	}

	t.Run("empty synchronous", func(t *testing.T) {
		provider := NewLocalTablePartProvider()
		require.NotPanics(t, provider.Close)
		require.NotPanics(t, provider.Close) // Check that many Closes won't panic.
		require.Error(t, provider.AppendParts(ctx, parts))
		checkPartProvider(t, []*model.OperationTablePart{nil}, provider.TablePartProvider())
	})

	t.Run("synchronous", func(t *testing.T) {
		provider := NewLocalTablePartProvider(parts...)
		require.NotPanics(t, provider.Close)
		require.NotPanics(t, provider.Close) // Check that many Closes won't panic.
		require.Error(t, provider.AppendParts(ctx, parts))
		checkPartProvider(t, append(parts, nil), provider.TablePartProvider())
	})

	t.Run("asynchronous", func(t *testing.T) {
		provider := NewAsyncLocalTablePartProvider()

		require.NoError(t, provider.AppendParts(ctx, parts[:2]))
		checkPartProvider(t, parts[:2], provider.TablePartProvider())

		require.NoError(t, provider.AppendParts(ctx, []*model.OperationTablePart{parts[2]}))
		checkPartProvider(t, []*model.OperationTablePart{parts[2]}, provider.TablePartProvider())

		// Check that cancellation of context won't cause deadlock.
		provider.parts = make(chan *model.OperationTablePart, 1) // Use 1 to make channel filled.
		ctx, cancel := context.WithCancel(ctx)
		waitCh := make(chan struct{})
		go func() {
			defer close(waitCh)
			// Will be cancelled (bcs len(parts) > cap(chan)), so err is not nil.
			require.Error(t, provider.AppendParts(ctx, parts))
		}()
		time.Sleep(50 * time.Millisecond)
		cancel()
		<-waitCh

		require.NotPanics(t, provider.Close)
		require.NotPanics(t, provider.Close) // Check that many Closes won't panic.
		require.Error(t, provider.AppendParts(ctx, parts))
	})
}

func checkPartProvider(t *testing.T, expected []*model.OperationTablePart, provider TablePartProvider) {
	for _, part := range expected {
		actual, err := provider(context.Background())
		require.Equal(t, part, actual)
		require.NoError(t, err)
	}
}

func TestAddKeyToJson(t *testing.T) {
	testCases := []struct {
		input    string
		expected map[string]any
	}{
		{
			input:    ``,
			expected: map[string]any{"key": json.Number("123")},
		},
		{
			input:    `{"key-1":"text"}`,
			expected: map[string]any{"key-1": "text", "key": json.Number("123")},
		},
	}

	for i, testCase := range testCases {
		res, err := addKeyToJson(testCase.input, "key", 123)
		require.NoError(t, err)
		var actual map[string]any
		require.NoError(t, jsonx.Unmarshal(res, &actual))
		require.Equal(t, testCase.expected, actual, fmt.Sprintf("test-case-%d", i))
	}
}
