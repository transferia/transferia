package queues

import (
	"fmt"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/parsers"
	"go.ytsaurus.tech/library/go/core/log"
)

type LbOffsetsSourceValidator struct {
	logger                log.Logger
	partitionToLastOffset map[string]uint64
}

func (v *LbOffsetsSourceValidator) CheckLbOffsets(batches []parsers.MessageBatch) error {
	for _, b := range batches {
		partition := fmt.Sprintf("%v@%v", b.Topic, b.Partition)
		if len(b.Messages) == 0 {
			v.logger.Warnf("partition has 0 messages: %v partition", partition)
		}

		firstOffset := b.Messages[0].Offset
		lastOffset := b.Messages[len(b.Messages)-1].Offset

		if int(lastOffset-firstOffset+1) != len(b.Messages) {
			v.partitionToLastOffset[partition] = lastOffset // for the case when (AllowTTLRewind == false), to not to spam logs
			return xerrors.Errorf("b.Messages has gaps in offsets. lastOffset: %v, firstOffset: %v, len(b.Messages): %v", lastOffset, firstOffset, len(b.Messages))
		}

		if v.partitionToLastOffset[partition] == 0 { // first read topic by this consumer
			v.partitionToLastOffset[partition] = lastOffset
			continue
		}

		if firstOffset != v.partitionToLastOffset[partition]+1 {
			prevLastOffset := v.partitionToLastOffset[partition]
			v.partitionToLastOffset[partition] = lastOffset // for the case when (AllowTTLRewind == false), to not to spam logs
			return xerrors.Errorf("found rewind into the session. Last offset: %v, New offset: %v, partition: %v", prevLastOffset, firstOffset, partition)
		}

		v.partitionToLastOffset[partition] = lastOffset
	}
	return nil
}

func (v *LbOffsetsSourceValidator) InitOffsetForPartition(topic string, partition uint32, consumerOffsetAfterLastCommitted uint64) {
	partitionStr := fmt.Sprintf("%v@%v", topic, partition)

	if consumerOffsetAfterLastCommitted == 0 { // first read topic by this consumer
		v.partitionToLastOffset[partitionStr] = 0
	} else {
		v.partitionToLastOffset[partitionStr] = consumerOffsetAfterLastCommitted - 1
	}
}

func NewLbOffsetsSourceValidator(logger log.Logger) *LbOffsetsSourceValidator {
	return &LbOffsetsSourceValidator{
		logger:                logger,
		partitionToLastOffset: make(map[string]uint64),
	}
}
