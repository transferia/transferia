package sequencer

import (
	"fmt"
	"strconv"
	"strings"
	"sync"

	"github.com/transferria/transferria/library/go/core/xerrors"
)

type QueueMessage struct {
	Topic     string
	Partition int
	Offset    int64
}

type partitionToOffsets struct {
	topic             string
	partition         int
	offsets           []int64
	maxAppendedOffset int64
}

func (p *partitionToOffsets) appendOffset(newOffset int64) error {
	if len(p.offsets) > 0 {
		lastOffset := p.offsets[len(p.offsets)-1]
		if lastOffset >= newOffset {
			return xerrors.Errorf("invariant is broken: offsets should go in increasing order, lastOffset:%d, newOffset:%d", lastOffset, newOffset)
		}
	}
	p.offsets = append(p.offsets, newOffset)
	p.maxAppendedOffset = newOffset
	return nil
}

func (p *partitionToOffsets) removeOffsets(in map[int64]bool) error {
	newOffsets := make([]int64, 0, len(p.offsets))
	for _, el := range p.offsets {
		if !in[el] {
			newOffsets = append(newOffsets, el)
		}
	}

	if len(newOffsets)+len(in) != len(p.offsets) {
		return xerrors.New("invariant is broken: you should push only taken offsets")
	}

	p.offsets = newOffsets
	return nil
}

func (p *partitionToOffsets) committedTo() int64 {
	if len(p.offsets) == 0 {
		return p.maxAppendedOffset
	} else {
		return p.offsets[0] - 1
	}
}

func (p *partitionToOffsets) toStringRanges() string {
	result := ""
	if len(p.offsets) == 0 {
		return ""
	}
	prevRangeBegin := p.offsets[0]
	prevRangeEl := p.offsets[0]
	rangeLen := 0
	for offsetIndex, currOffset := range p.offsets[1:] {
		if prevRangeEl+1 == currOffset {
			prevRangeEl = currOffset
			rangeLen++
			continue
		}
		if rangeLen == 0 {
			result += fmt.Sprintf("%d,", prevRangeBegin)
			prevRangeBegin = currOffset
			prevRangeEl = currOffset
			continue
		} else {
			result += fmt.Sprintf("%d-%d,", prevRangeBegin, p.offsets[offsetIndex])
			prevRangeBegin = currOffset
			prevRangeEl = currOffset
			rangeLen = 0
			continue
		}
	}
	if rangeLen == 0 {
		result += fmt.Sprintf("%d", prevRangeBegin)
	} else {
		result += fmt.Sprintf("%d-%d", prevRangeBegin, p.offsets[len(p.offsets)-1])
	}
	return result
}

func makePartitionName(topic string, partition int) string {
	return fmt.Sprintf("%s@%d", topic, partition)
}

func partitionNameToTopic(partitionName string) string {
	index := strings.LastIndex(partitionName, "@")
	return partitionName[0:index]
}

func partitionNameToPartition(partitionName string) int {
	index := strings.LastIndex(partitionName, "@")
	result, _ := strconv.Atoi(partitionName[index+1:])
	return result
}

type Sequencer struct {
	mutex      sync.Mutex
	processing map[string]*partitionToOffsets
}

func (s *Sequencer) StartProcessing(messages []QueueMessage) error {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	for _, message := range messages {
		partitionName := makePartitionName(message.Topic, message.Partition)
		if _, ok := s.processing[partitionName]; !ok {
			s.processing[partitionName] = &partitionToOffsets{
				topic:             message.Topic,
				partition:         int(message.Partition),
				offsets:           make([]int64, 0),
				maxAppendedOffset: 0,
			}
		}
		err := s.processing[partitionName].appendOffset(message.Offset)
		if err != nil {
			return xerrors.Errorf("unable to append offset to sequencer, err: %w", err)
		}
	}
	return nil
}

func (s *Sequencer) committedTo() map[string]int64 {
	result := make(map[string]int64)
	for name, obj := range s.processing {
		result[name] = obj.committedTo()
	}
	return result
}

func (s *Sequencer) Pushed(messages []QueueMessage) ([]QueueMessage, error) {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	prevCommittedTo := s.committedTo()

	partitionToPushedMsgs := make(map[string]map[int64]bool)
	for _, message := range messages {
		partitionName := makePartitionName(message.Topic, message.Partition)
		if _, ok := s.processing[partitionName]; !ok {
			return nil, xerrors.Errorf("invariant is broken: you should push only taken partitions, partitionName: %s", partitionName)
		}
		if _, ok := partitionToPushedMsgs[partitionName]; !ok {
			partitionToPushedMsgs[partitionName] = make(map[int64]bool)
		}
		partitionToPushedMsgs[partitionName][message.Offset] = true
	}

	for partitionName, offsets := range partitionToPushedMsgs {
		err := s.processing[partitionName].removeOffsets(offsets)
		if err != nil {
			return nil, xerrors.Errorf("unable to remove offsets from sequences, err: %w", err)
		}
	}

	newCommittedTo := s.committedTo()

	result := make([]QueueMessage, 0)
	for partitionName, prevCommitted := range prevCommittedTo {
		newCommitted := newCommittedTo[partitionName]
		if newCommitted != prevCommitted {
			result = append(result, QueueMessage{
				Topic:     partitionNameToTopic(partitionName),
				Partition: partitionNameToPartition(partitionName),
				Offset:    newCommitted,
			})
		}
	}
	return result, nil
}

func (s *Sequencer) ToStringRanges() string {
	result := ""
	for _, partitionObj := range s.processing {
		result += fmt.Sprintf("%d:%s;", partitionObj.partition, partitionObj.toStringRanges())
	}
	if result == "" {
		return result
	}
	return result[0 : len(result)-1]
}

func (s *Sequencer) ToStringRangesWithTopic() string {
	result := ""
	for _, partitionObj := range s.processing {
		result += fmt.Sprintf("%s@%d:%s;", partitionObj.topic, partitionObj.partition, partitionObj.toStringRanges())
	}
	return result[0 : len(result)-1]
}

func NewSequencer() *Sequencer {
	return &Sequencer{
		mutex:      sync.Mutex{},
		processing: make(map[string]*partitionToOffsets),
	}
}
