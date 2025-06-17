//go:build !disable_ydb_provider

package ydb

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/worker/tasks"
)

func TestYdbSource_Include(t *testing.T) {
	type source struct {
		Tables []string
	}
	type args struct {
		tID abstract.TableID
	}
	tests := []struct {
		name   string
		source source
		args   args
		want   bool
	}{
		{
			name: "match: empty tables list in source",
			args: args{
				tID: abstract.TableID{Name: "test", Namespace: ""},
			},
			source: source{Tables: make([]string, 0)},
			want:   true,
		},
		{
			name: "match: tables list includes target",
			args: args{
				tID: abstract.TableID{Name: "/test", Namespace: ""},
			},
			source: source{Tables: []string{"a", "b", "test"}},
			want:   true,
		},
		{
			name: "mismatch: tables list not includes target",
			args: args{
				tID: abstract.TableID{Name: "test", Namespace: ""},
			},
			source: source{Tables: []string{"a", "b", "c"}},
			want:   false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &YdbSource{Tables: tt.source.Tables}
			if got := s.Include(tt.args.tID); got != tt.want {
				t.Errorf("Include() = %v, want %v", got, tt.want)
			}
		})
	}
}

func checkYDBTestCase(t *testing.T, useFullPaths bool, tables []string, existingTable, expectedTable string) {
	src := &YdbSource{
		UseFullPaths: useFullPaths,
		Tables:       tables,
	}
	storageParams := src.ToStorageParams()
	existingTableID := abstract.TableID{Namespace: "", Name: existingTable}
	require.Equal(t, expectedTable, MakeYDBRelPath(storageParams.UseFullPaths, storageParams.Tables, existingTableID.Name))
	require.NotNil(t, src.FulfilledIncludes(existingTableID))
}

func TestMakeYDBRelPath(t *testing.T) {
	//-----
	// root

	t.Run("root case with use_full_paths", func(t *testing.T) {
		checkYDBTestCase(t, true, nil, "/abc", "abc")
	})
	t.Run("root case without use_full_paths", func(t *testing.T) {
		checkYDBTestCase(t, false, nil, "/abc", "abc")
	})

	//------
	// table

	t.Run("table case with use_full_paths without leading slash", func(t *testing.T) {
		checkYDBTestCase(t, true, []string{"abc"}, "/abc", "abc")
	})
	t.Run("table case with use_full_paths with leading slash", func(t *testing.T) {
		checkYDBTestCase(t, true, []string{"/abc"}, "/abc", "abc")
	})
	t.Run("table case without use_full_paths without leading slash", func(t *testing.T) {
		checkYDBTestCase(t, false, []string{"abc"}, "/abc", "abc")
	})
	t.Run("table case without use_full_paths with leading slash", func(t *testing.T) {
		checkYDBTestCase(t, false, []string{"/abc"}, "/abc", "abc")
	})

	//----------------
	// dir (top-level)

	t.Run("dir case with use_full_paths with leading slash", func(t *testing.T) {
		checkYDBTestCase(t, true, []string{"/dir1"}, "/dir1/abc", "dir1/abc")
	})
	t.Run("dir case with use_full_paths without leading slash", func(t *testing.T) {
		checkYDBTestCase(t, true, []string{"dir1"}, "/dir1/abc", "dir1/abc")
	})
	t.Run("dir case without use_full_paths with leading slash", func(t *testing.T) {
		checkYDBTestCase(t, false, []string{"/dir1"}, "/dir1/abc", "dir1/abc")
	})
	t.Run("dir case without use_full_paths without leading slash", func(t *testing.T) {
		checkYDBTestCase(t, false, []string{"dir1"}, "/dir1/abc", "dir1/abc")
	})

	// tailing slash

	t.Run("dir case with use_full_paths with leading slash", func(t *testing.T) {
		checkYDBTestCase(t, true, []string{"/dir1/"}, "/dir1/abc", "dir1/abc")
	})
	t.Run("dir case with use_full_paths without leading slash", func(t *testing.T) {
		checkYDBTestCase(t, true, []string{"dir1/"}, "/dir1/abc", "dir1/abc")
	})
	t.Run("dir case without use_full_paths with leading slash", func(t *testing.T) {
		checkYDBTestCase(t, false, []string{"/dir1/"}, "/dir1/abc", "dir1/abc")
	})
	t.Run("dir case without use_full_paths without leading slash", func(t *testing.T) {
		checkYDBTestCase(t, false, []string{"dir1/"}, "/dir1/abc", "dir1/abc")
	})

	//--------------------
	// dir (not-top-level)

	t.Run("dir case with use_full_paths with leading slash", func(t *testing.T) {
		checkYDBTestCase(t, true, []string{"/dir1/dir2"}, "/dir1/dir2/abc", "dir1/dir2/abc")
	})
	t.Run("dir case with use_full_paths without leading slash", func(t *testing.T) {
		checkYDBTestCase(t, true, []string{"dir1/dir2"}, "/dir1/dir2/abc", "dir1/dir2/abc")
	})
	t.Run("dir case without use_full_paths with leading slash", func(t *testing.T) {
		checkYDBTestCase(t, false, []string{"/dir1/dir2"}, "/dir1/dir2/abc", "dir2/abc")
	})
	t.Run("dir case without use_full_paths without leading slash", func(t *testing.T) {
		checkYDBTestCase(t, false, []string{"dir1/dir2"}, "/dir1/dir2/abc", "dir2/abc")
	})

	// tailing slash

	t.Run("dir case with use_full_paths with leading slash", func(t *testing.T) {
		checkYDBTestCase(t, true, []string{"/dir1/dir2/"}, "/dir1/dir2/abc", "dir1/dir2/abc")
	})
	t.Run("dir case with use_full_paths without leading slash", func(t *testing.T) {
		checkYDBTestCase(t, true, []string{"dir1/dir2/"}, "/dir1/dir2/abc", "dir1/dir2/abc")
	})
	t.Run("dir case without use_full_paths with leading slash", func(t *testing.T) {
		checkYDBTestCase(t, false, []string{"/dir1/dir2/"}, "/dir1/dir2/abc", "dir2/abc")
	})
	t.Run("dir case without use_full_paths without leading slash", func(t *testing.T) {
		checkYDBTestCase(t, false, []string{"dir1/dir2/"}, "/dir1/dir2/abc", "dir2/abc")
	})
}

func TestCheckIncludeDirectives_Src_YDBSpecific(t *testing.T) {
	// the real table descriptors generated by YDB source now not having leading slash in full path by convention
	tables := []abstract.TableDescription{
		{Name: "table1/full/path", Schema: ""},
		{Name: "table2/full/path", Schema: ""},
	}
	// although, user input still can have one leading slash
	transfer := new(model.Transfer)
	transfer.Src = &YdbSource{Tables: []string{
		// this is a canonical table description: full path with slashes, no leading slash
		"table1/full/path",
		// note, that nowadays we just omit leading slash when checking table compatibility,
		// thus this addition by user of table with leading slash should not lead to error
		"/table2/full/path",
	}}
	snapshotLoader := tasks.NewSnapshotLoader(&coordinator.CoordinatorNoOp{}, "test-operation", transfer, solomon.NewRegistry(nil))
	err := snapshotLoader.CheckIncludeDirectives(tables)
	require.NoError(t, err)
}
