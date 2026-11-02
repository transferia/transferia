package provider

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	yt_provider "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/cypressmeta"
	"go.uber.org/zap"
	"go.uber.org/zap/zaptest/observer"
	logzap "go.ytsaurus.tech/library/go/core/log/zap"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type schemaFilterTx struct {
	yt.Tx
	t           *testing.T
	schema      ytschema.Schema
	lockErr     error
	schemaErr   error
	schemaReads int
}

func (tx *schemaFilterTx) LockNode(_ context.Context, _ ypath.YPath, mode yt.LockMode, _ *yt.LockNodeOptions) (yt.LockResult, error) {
	require.Equal(tx.t, yt.LockSnapshot, mode)
	return yt.LockResult{NodeID: yt.NodeID{}}, tx.lockErr
}

func (tx *schemaFilterTx) GetNode(_ context.Context, path ypath.YPath, result any, _ *yt.GetNodeOptions) error {
	// Read through the locked node identity, not through the mutable table path.
	require.Equal(tx.t, (yt.NodeID{}).YPath().Attr("schema"), path)
	tx.schemaReads++
	if tx.schemaErr != nil {
		return tx.schemaErr
	}
	*result.(*ytschema.Schema) = tx.schema
	return nil
}

func TestSkipTablesWithUnsupportedSchema(t *testing.T) {
	strict, nonStrict := true, false
	columns := []ytschema.Column{{Name: "value", ComplexType: ytschema.TypeString}}
	for _, tc := range []struct {
		name      string
		schema    ytschema.Schema
		wantCount int
		reason    string
	}{
		{name: "skip unspecified schema", schema: ytschema.Schema{}, wantCount: 0, reason: "empty schema"},
		{name: "skip empty schema", schema: ytschema.Schema{Strict: &nonStrict}, wantCount: 0, reason: "empty schema"},
		{name: "skip strict empty schema", schema: ytschema.Schema{Strict: &strict}, wantCount: 0, reason: "empty schema"},
		{name: "skip non-strict columns", schema: ytschema.Schema{Strict: &nonStrict, Columns: columns}, wantCount: 0, reason: "non-strict schema"},
		{name: "keep strict columns", schema: ytschema.Schema{Strict: &strict, Columns: columns}, wantCount: 1},
		{name: "implicit strict schema", schema: ytschema.Schema{Columns: columns}, wantCount: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tx := &schemaFilterTx{t: t, schema: tc.schema}
			core, observed := observer.New(zap.WarnLevel)
			src := &source{cfg: &yt_provider.YtSource{RowIdxColumnName: "row_idx"}, tx: tx, logger: logzap.NewWithCore(core)}
			node := cypressmeta.NewYtNodeMeta("c", "//root", "table", 100, 1000, 0, yt.NodeTable, false, nil, nil)
			got, err := src.lockAndFilterTables(context.Background(), cypressmeta.YtNodes{node})
			require.NoError(t, err)
			require.Len(t, got, tc.wantCount)
			require.NotNil(t, got, "empty results must be cached without relisting")
			require.Equal(t, 1, tx.schemaReads)
			if tc.reason == "" {
				require.Empty(t, observed.All())
			} else {
				entries := observed.All()
				require.Len(t, entries, 1)
				require.Equal(t, "//root/table", entries[0].ContextMap()["table"])
				require.Equal(t, tc.reason, entries[0].ContextMap()["reason"])
			}
		})
	}
}

func TestSkipTablesWithUnsupportedSchemaPropagatesErrors(t *testing.T) {
	failure := errors.New("access denied")
	for _, stage := range []string{"lock", "schema"} {
		t.Run(stage, func(t *testing.T) {
			tx := &schemaFilterTx{t: t}
			if stage == "lock" {
				tx.lockErr = failure
			} else {
				tx.schemaErr = failure
			}
			src := &source{cfg: &yt_provider.YtSource{}, tx: tx, logger: logger.Log}
			node := cypressmeta.NewYtNodeMeta("c", "//root", "table", 100, 1000, 0, yt.NodeTable, false, nil, nil)
			_, err := src.lockAndFilterTables(context.Background(), cypressmeta.YtNodes{node})
			require.ErrorIs(t, err, failure)
		})
	}
}

func TestSkippedTableSatisfiesIncludeFromSnapshot(t *testing.T) {
	tx := &schemaFilterTx{t: t}
	// The live client is deliberately nil: after discovery the live path may
	// disappear, and validating this include must not query it again.
	src := &source{cfg: &yt_provider.YtSource{}, tx: tx, logger: logger.Log}
	node := cypressmeta.NewYtNodeMeta("c", "//root", "table", 100, 1000, 0, yt.NodeTable, false, nil, nil)
	nodes, err := src.lockAndFilterTables(context.Background(), cypressmeta.YtNodes{node})
	require.NoError(t, err)
	filtered, err := src.filterByIncludes(context.Background(), nodes, []string{"//root", "//root/table"})
	require.NoError(t, err)
	require.Empty(t, filtered)
}

type missingIncludeClient struct {
	yt.Client
	err error
}

func (c *missingIncludeClient) GetNode(_ context.Context, _ ypath.YPath, _ any, _ *yt.GetNodeOptions) error {
	return c.err
}

func TestSkippedTableDoesNotHideMissingInclude(t *testing.T) {
	failure := errors.New("no such node")
	src := &source{
		yt:           &missingIncludeClient{err: failure},
		skippedPaths: map[string]struct{}{"//root/skipped": {}},
	}
	_, err := src.filterByIncludes(context.Background(), nil, []string{"//root/missing"})
	require.ErrorIs(t, err, failure)
}

func TestSkipTablesIncludeOverlapAndOrder(t *testing.T) {
	first := cypressmeta.NewYtNodeMeta("c", "//root", "sub/first", 1, 10, 0, yt.NodeTable, false, nil, nil)
	second := cypressmeta.NewYtNodeMeta("c", "//root", "sub/second", 1, 10, 0, yt.NodeTable, false, nil, nil)
	duplicate := cypressmeta.NewYtNodeMeta("c", "//root/sub", "first", 1, 10, 0, yt.NodeTable, false, nil, nil)
	outside := cypressmeta.NewYtNodeMeta("c", "//root-other", "table", 1, 10, 0, yt.NodeTable, false, nil, nil)
	// No live client: every overlapping include must be marked from the
	// snapshot nodes, including the exact table path.
	src := &source{}
	got, err := src.filterByIncludes(context.Background(), cypressmeta.YtNodes{second, first, duplicate, outside},
		[]string{"//root", "//root/sub", "//root/sub/first"})
	require.NoError(t, err)
	require.Len(t, got, 2)
	require.Same(t, second, got[0])
	require.Same(t, first, got[1])
}

func TestSkipTablesIncludePathBoundary(t *testing.T) {
	failure := errors.New("no such node")
	for _, state := range []string{"retained", "skipped"} {
		t.Run(state, func(t *testing.T) {
			src := &source{yt: &missingIncludeClient{err: failure}}
			var nodes cypressmeta.YtNodes
			if state == "skipped" {
				src.skippedPaths = map[string]struct{}{"//root-other/table": {}}
			} else {
				nodes = cypressmeta.YtNodes{cypressmeta.NewYtNodeMeta("c", "//root-other", "table", 1, 10, 0, yt.NodeTable, false, nil, nil)}
			}
			_, err := src.filterByIncludes(context.Background(), nodes, []string{"//root"})
			require.ErrorIs(t, err, failure)
		})
	}
}
