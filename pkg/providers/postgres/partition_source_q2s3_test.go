package postgres

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	postgres_sequencer "github.com/transferia/transferia/pkg/providers/postgres/sequencer"
)

func TestOffsetsNotPushed(t *testing.T) {
	all := []abstract.ChangeItem{
		{LSN: 10}, {LSN: 10}, {LSN: 20}, {LSN: 30},
	}
	pushed := []abstract.ChangeItem{
		{LSN: 10}, {LSN: 30},
	}
	require.Equal(t, []uint64{10, 20}, offsetsNotPushed(all, pushed))
}

func TestQueueToS3AckAdvancesMaxLsn(t *testing.T) {
	publisher := &replication{
		config:    &PgSource{SlotID: "slot-1"},
		mutex:     new(sync.Mutex),
		stopCh:    make(chan struct{}),
		sequencer: postgres_sequencer.NewSequencer(),
	}
	p := &queueToS3Replication{replication: publisher}
	require.NoError(t, publisher.sequencer.StartProcessing([]abstract.ChangeItem{
		{ID: 1, LSN: 10}, {ID: 1, LSN: 11},
	}))
	require.NoError(t, publisher.sequencer.StartProcessing([]abstract.ChangeItem{
		{ID: 2, LSN: 20},
	}))

	require.NoError(t, p.ack(abstract.QueueResult{Offsets: []uint64{10, 11}}))
	require.Equal(t, uint64(11), publisher.maxLsn)

	require.NoError(t, p.ack(abstract.QueueResult{Offsets: []uint64{20}}))
	// last in-flight tx is not committed by design
	require.Equal(t, uint64(11), publisher.maxLsn)
}

func TestParseForQueueToS3FillsQueueMessageMeta(t *testing.T) {
	publisher := &replication{
		config:    &PgSource{SlotID: "slot-1"},
		mutex:     new(sync.Mutex),
		stopCh:    make(chan struct{}),
		sequencer: postgres_sequencer.NewSequencer(),
		// Pre-populated cache: WithIncludeFilter will not consult objectsFilter/config.
		includeCache: map[abstract.TableID]bool{
			{Namespace: "public", Name: "t"}:                 true,
			{Namespace: "public", Name: TableConsumerKeeper}: true,
		},
	}
	p := &queueToS3Replication{replication: publisher}

	items := []abstract.ChangeItem{
		{ID: 1, LSN: 100, Counter: 0, Schema: "public", Table: "t"},
		{ID: 1, LSN: 100, Counter: 1, Schema: "public", Table: TableConsumerKeeper},
		{ID: 1, LSN: 101, Counter: 2, Schema: "public", Table: "t"},
	}
	require.NoError(t, publisher.sequencer.StartProcessing(items))

	parsed, err := p.parse(items)
	require.NoError(t, err)
	require.Len(t, parsed, 2)

	require.Equal(t, "slot-1", parsed[0].QueueMessageMeta.TopicName)
	require.Equal(t, 0, parsed[0].QueueMessageMeta.PartitionNum)
	require.Equal(t, uint64(100), parsed[0].QueueMessageMeta.Offset)
	require.Equal(t, 0, parsed[0].QueueMessageMeta.Index)

	require.Equal(t, "slot-1", parsed[1].QueueMessageMeta.TopicName)
	require.Equal(t, uint64(101), parsed[1].QueueMessageMeta.Offset)
	require.Equal(t, 2, parsed[1].QueueMessageMeta.Index)
}
