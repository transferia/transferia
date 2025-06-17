//go:build !disable_ydb_provider

package ydb

import (
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/worker/tasks"
)

type mockSink struct {
	PushCallback func([]abstract.ChangeItem)
}

func (s *mockSink) Close() error {
	return nil
}

func (s *mockSink) Push(input []abstract.ChangeItem) error {
	s.PushCallback(input)
	return nil
}

func TestYDBCleanupPaths(t *testing.T) {
	type ydbTableType struct {
		paths []string
		abs   abstract.TableID
		rel   abstract.TableID
	}

	for caseName, ydbTable := range map[string]ydbTableType{
		"root case":              {paths: []string{"abc"}, abs: *abstract.NewTableID("", "/abc"), rel: *abstract.NewTableID("", "abc")},
		"top-level dir case":     {paths: []string{"dir1"}, abs: *abstract.NewTableID("", "/dir1/abc"), rel: *abstract.NewTableID("", "dir1/abc")},
		"non-top-level dir case": {paths: []string{"dir1/dir2"}, abs: *abstract.NewTableID("", "/dir1/dir2/abc"), rel: *abstract.NewTableID("", "dir2/abc")},
		"multi-level dir case 1": {paths: []string{"dir1/dir2/dir3"}, abs: *abstract.NewTableID("", "/dir1/dir2/dir3/abc"), rel: *abstract.NewTableID("", "dir3/abc")},
		"multi-level dir case 2": {paths: []string{"dir1/dir2"}, abs: *abstract.NewTableID("", "/dir1/dir2/dir3/abc"), rel: *abstract.NewTableID("", "dir2/dir3/abc")},
	} {
		t.Run(fmt.Sprintf("%s (without full path)", caseName), func(t *testing.T) {
			testCaseYDBCleanupPaths(t, false, ydbTable.paths, ydbTable.abs, ydbTable.rel)
		})
		expectedTransformedTableID := abstract.TableID{
			Namespace: ydbTable.abs.Namespace,
			Name:      strings.TrimPrefix(ydbTable.abs.Name, "/"),
		}
		t.Run(fmt.Sprintf("%s (with full path)", caseName), func(t *testing.T) {
			testCaseYDBCleanupPaths(t, true, ydbTable.paths, ydbTable.abs, expectedTransformedTableID)
		})

	}
}

func testCaseYDBCleanupPaths(
	t *testing.T,
	useFullPath bool,
	paths []string,
	tableID abstract.TableID,
	expectedTransformedTableID abstract.TableID,
) {
	src := new(YdbSource)
	src.UseFullPaths = useFullPath
	src.Tables = paths

	tables := abstract.TableMap{
		tableID: {},
	}

	sinker := new(mockSink)
	dst := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sinker },
		Cleanup:       model.Drop,
	}

	sinker.PushCallback = func(items []abstract.ChangeItem) {
		require.Len(t, items, 1, "Expecting cleanup batch of size 1")
		item := items[0]
		require.Equal(t, item.Kind, abstract.DropTableKind, "should receive only drop table kind")
		actualTableID := item.TableID()
		require.Equal(t, expectedTransformedTableID, actualTableID)
	}

	transferID := "dttlohpidr"

	transfer := &model.Transfer{
		ID:   transferID,
		Type: abstract.TransferTypeSnapshotOnly,
		Src:  src,
		Dst:  dst,
	}
	transfer.FillDependentFields()

	emptyRegistry := solomon.NewRegistry(nil).WithTags(map[string]string{"ts": time.Now().String()})

	snapshotLoader := tasks.NewSnapshotLoader(coordinator.NewFakeClient(), "test-operation", transfer, emptyRegistry)
	err := snapshotLoader.CleanupSinker(ConvertTableMapToYDBRelPath(src.ToStorageParams(), tables))
	require.NoError(t, err)
}
