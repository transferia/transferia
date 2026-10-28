package target

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	yt_copy_events "github.com/transferia/transferia/pkg/providers/yt/copy/events"
	"github.com/transferia/transferia/pkg/providers/yt/cypressmeta"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yson"
	"go.ytsaurus.tech/yt/go/yt"
)

type copyTestClient struct {
	yt.Client
	t              *testing.T
	operationSpec  map[string]any
	outputPath     ypath.YPath
	copiedRowCount int64
	lostRowCount   *int64
	getError       error
	setError       error
	moved          bool
	snapshot       *copySnapshotTx
}

var _ ytMimimalClient = (*copyTestClient)(nil)

func (c *copyTestClient) CreateNode(_ context.Context, path ypath.YPath, _ yt.NodeType, _ *yt.CreateNodeOptions) (yt.NodeID, error) {
	c.outputPath = path
	return yt.NodeID{}, nil
}

func (c *copyTestClient) StartOperation(_ context.Context, opType yt.OperationType, operationSpec any, _ *yt.StartOperationOptions) (yt.OperationID, error) {
	require.Equal(c.t, yt.OperationRemoteCopy, opType)
	if c.snapshot != nil {
		require.True(c.t, c.snapshot.locked)
		require.True(c.t, c.snapshot.rowCountRead)
		require.False(c.t, c.snapshot.aborted, "source snapshot must remain alive until copy completes")
	}
	data, err := yson.Marshal(operationSpec)
	require.NoError(c.t, err)
	require.NoError(c.t, yson.Unmarshal(data, &c.operationSpec))
	return yt.OperationID{}, nil
}

func (c *copyTestClient) GetNode(_ context.Context, path ypath.YPath, result any, _ *yt.GetNodeOptions) error {
	require.Equal(c.t, c.outputPath.YPath().Attr("row_count").String(), path.YPath().String())
	require.False(c.t, c.snapshot.aborted, "source snapshot must remain alive until destination row count is read")
	if c.getError != nil {
		return c.getError
	}
	*result.(*int64) = c.copiedRowCount
	return nil
}

func (c *copyTestClient) SetNode(_ context.Context, path ypath.YPath, value any, _ *yt.SetNodeOptions) error {
	require.Equal(c.t, c.outputPath.YPath().Attr(LostRowCountAttr).String(), path.YPath().String())
	if c.setError != nil {
		return c.setError
	}
	count := value.(int64)
	c.lostRowCount = &count
	return nil
}

func (c *copyTestClient) NodeExists(context.Context, ypath.YPath, *yt.NodeExistsOptions) (bool, error) {
	return false, nil
}

func (c *copyTestClient) MoveNode(_ context.Context, src, dst ypath.YPath, _ *yt.MoveNodeOptions) (yt.NodeID, error) {
	require.Equal(c.t, c.outputPath, src)
	require.Equal(c.t, "//target/table", dst.YPath().String())
	require.NotNil(c.t, c.lostRowCount, "lost row count must be set before publishing the replacement")
	c.moved = true
	return yt.NodeID{}, nil
}

type copyStatusClient struct {
	yt.Client
	snapshot   *copySnapshotTx
	beginError error
}

var _ yt.Client = (*copyStatusClient)(nil)

func (c *copyStatusClient) GetOperation(context.Context, yt.OperationID, *yt.GetOperationOptions) (*yt.OperationStatus, error) {
	return &yt.OperationStatus{State: yt.StateCompleted}, nil
}

func (c *copyStatusClient) BeginTx(context.Context, *yt.StartTxOptions) (yt.Tx, error) {
	if c.beginError != nil {
		return nil, c.beginError
	}
	return c.snapshot, nil
}

type copySnapshotTx struct {
	yt.Tx
	t            *testing.T
	rowCount     int64
	lockError    error
	getError     error
	locked       bool
	rowCountRead bool
	aborted      bool
}

var _ yt.Tx = (*copySnapshotTx)(nil)

func (tx *copySnapshotTx) ID() yt.TxID {
	return yt.TxID{1}
}

func (tx *copySnapshotTx) LockNode(_ context.Context, path ypath.YPath, mode yt.LockMode, _ *yt.LockNodeOptions) (yt.LockResult, error) {
	require.Equal(tx.t, "//source/table", path.YPath().String())
	require.Equal(tx.t, yt.LockSnapshot, mode)
	if tx.lockError != nil {
		return yt.LockResult{}, tx.lockError
	}
	tx.locked = true
	return yt.LockResult{NodeID: yt.NodeID{2}}, nil
}

func (tx *copySnapshotTx) GetNode(_ context.Context, path ypath.YPath, result any, _ *yt.GetNodeOptions) error {
	require.True(tx.t, tx.locked, "source row count must be read after snapshot lock")
	require.Equal(tx.t, (yt.NodeID{2}).YPath().Attr("row_count").String(), path.YPath().String())
	if tx.getError != nil {
		return tx.getError
	}
	*result.(*int64) = tx.rowCount
	tx.rowCountRead = true
	return nil
}

func (tx *copySnapshotTx) Abort() error {
	tx.aborted = true
	return nil
}

func TestRunCopySkipUnavailableChunks(t *testing.T) {
	for _, tc := range []struct {
		name        string
		enabled     bool
		nodeType    yt.NodeType
		sourceRows  int64
		listedRows  int64
		copiedRows  int64
		cleanup     model.CleanupType
		getError    error
		setError    error
		beginError  error
		lockError   error
		sourceError error
	}{
		{name: "disabled", nodeType: yt.NodeTable, sourceRows: 100, copiedRows: 100},
		{name: "lost rows", enabled: true, nodeType: yt.NodeTable, sourceRows: 100, copiedRows: 80},
		{name: "no loss", enabled: true, nodeType: yt.NodeTable, sourceRows: 100, copiedRows: 100},
		{name: "source appended since listing", enabled: true, nodeType: yt.NodeTable, listedRows: 100, sourceRows: 120, copiedRows: 120},
		{name: "source shrank since listing", enabled: true, nodeType: yt.NodeTable, listedRows: 100, sourceRows: 80, copiedRows: 80},
		{name: "loss after source changed", enabled: true, nodeType: yt.NodeTable, listedRows: 100, sourceRows: 120, copiedRows: 90},
		{name: "all rows lost", enabled: true, nodeType: yt.NodeTable, sourceRows: 100},
		{name: "empty table", enabled: true, nodeType: yt.NodeTable},
		{name: "file", enabled: true, nodeType: yt.NodeFile},
		{name: "replace", enabled: true, nodeType: yt.NodeTable, sourceRows: 100, copiedRows: 80, cleanup: model.Replace},
		{name: "get row count fails", enabled: true, nodeType: yt.NodeTable, cleanup: model.Replace, getError: errors.New("row count unavailable")},
		{name: "set lost row count fails", enabled: true, nodeType: yt.NodeTable, cleanup: model.Replace, setError: errors.New("attribute write failed")},
		{name: "source transaction fails", enabled: true, nodeType: yt.NodeTable, beginError: errors.New("transaction failed")},
		{name: "source lock fails", enabled: true, nodeType: yt.NodeTable, lockError: errors.New("lock failed")},
		{name: "source row count fails", enabled: true, nodeType: yt.NodeTable, sourceError: errors.New("source row count failed")},
	} {
		t.Run(tc.name, func(t *testing.T) {
			snapshot := &copySnapshotTx{t: t, rowCount: tc.sourceRows, lockError: tc.lockError, getError: tc.sourceError}
			client := &copyTestClient{t: t, copiedRowCount: tc.copiedRows, getError: tc.getError, setError: tc.setError}
			if tc.enabled && tc.nodeType == yt.NodeTable {
				client.snapshot = snapshot
			}
			cfg := &provider_yt.YtCopyDestination{Prefix: "//target", SkipUnavailableChunks: tc.enabled, Cleanup: tc.cleanup}
			cfg.WithDefaults()
			target := &YtCopyTarget{cfg: cfg, yt: &copyStatusClient{}, sourceYT: &copyStatusClient{snapshot: snapshot, beginError: tc.beginError}, logger: logger.Log, transferID: "test-transfer"}
			listedRows := tc.sourceRows
			if tc.listedRows != 0 {
				listedRows = tc.listedRows
			}
			batch := yt_copy_events.NewEventBatch(cypressmeta.YtNodes{
				cypressmeta.NewYtNodeMeta("source-cluster", "//source", "table", listedRows, 0, 0, tc.nodeType, false, nil, nil),
			})
			require.True(t, batch.Next())
			evt, err := batch.Event()
			require.NoError(t, err)
			err = target.runCopy(copyTask{evt: evt.(yt_copy_events.NodeEvent), yt: client})
			require.Equal(t, tc.enabled && tc.nodeType == yt.NodeTable && tc.beginError == nil, snapshot.aborted)
			expectedError := errors.Join(tc.getError, tc.setError, tc.beginError, tc.lockError, tc.sourceError)
			if expectedError != nil {
				require.Error(t, err)
				for _, cause := range []error{tc.getError, tc.setError, tc.beginError, tc.lockError, tc.sourceError} {
					if cause != nil {
						require.ErrorIs(t, err, cause)
					}
				}
				require.Nil(t, client.lostRowCount)
				require.False(t, client.moved)
				if tc.beginError != nil || tc.lockError != nil || tc.sourceError != nil {
					require.Nil(t, client.operationSpec, "copy must not start without a source snapshot and its row count")
				}
				return
			}
			require.NoError(t, err)
			require.Equal(t, "source-cluster", client.operationSpec["cluster_name"])
			outputPath, ok := client.operationSpec["output_table_path"].(*yson.ValueWithAttrs)
			require.True(t, ok)
			require.Equal(t, client.outputPath.YPath().String(), outputPath.Value)
			if tc.enabled {
				require.Equal(t, "skip", client.operationSpec["unavailable_chunk_strategy"])
				require.Equal(t, "skip", client.operationSpec["unavailable_chunk_tactics"])
			} else {
				require.NotContains(t, client.operationSpec, "unavailable_chunk_strategy")
				require.NotContains(t, client.operationSpec, "unavailable_chunk_tactics")
			}
			if tc.enabled && tc.nodeType == yt.NodeTable {
				inputPath, ok := client.operationSpec["input_table_paths"].([]any)[0].(*yson.ValueWithAttrs)
				require.True(t, ok)
				require.Equal(t, (yt.NodeID{2}).YPath().String(), inputPath.Value)
				require.Equal(t, snapshot.ID().String(), inputPath.Attrs["transaction_id"])
				require.NotNil(t, client.lostRowCount)
				require.Equal(t, tc.sourceRows-tc.copiedRows, *client.lostRowCount)
			} else {
				require.Nil(t, client.lostRowCount)
			}
			require.Equal(t, tc.cleanup == model.Replace, client.moved)
		})
	}
}
