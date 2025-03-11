package queue

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/debezium/packer"
	"github.com/transferia/transferia/pkg/util/pool"
)

//---

type task struct {
	serializer     serializerOneThread
	sessionPackers packer.SessionPackers
	changeItems    []abstract.ChangeItem
	results        map[abstract.TablePartID][]SerializedMessage
	err            error
}

func (t *task) Serialize() {
	results, err := t.serializer.SerializeOneThread(t.changeItems, t.sessionPackers)
	if err != nil {
		t.err = err
	}
	t.results = results
}

func newTask(serializer serializerOneThread, sessionPackers packer.SessionPackers, changeItems []abstract.ChangeItem) *task {
	return &task{
		serializer:     serializer,
		sessionPackers: sessionPackers,
		changeItems:    changeItems,
		results:        make(map[abstract.TablePartID][]SerializedMessage),
		err:            nil,
	}
}

//---

func Split(serializer serializerOneThread, sessionPackers packer.SessionPackers, items []abstract.ChangeItem, chunkSize int) []*task {
	tasks := make([]*task, 0, (len(items)/chunkSize)+1)
	for i := 0; i < len(items); i += chunkSize {
		end := i + chunkSize

		if end > len(items) {
			end = len(items)
		}

		tasks = append(tasks, newTask(serializer, sessionPackers, items[i:end]))
	}
	return tasks
}

func MergeBack(in []*task) map[abstract.TablePartID][]SerializedMessage {
	result := make(map[abstract.TablePartID][]SerializedMessage)
	for _, currTask := range in {
		for k, v := range currTask.results {
			_, ok := result[k]
			if !ok {
				result[k] = make([]SerializedMessage, 0, len(v))
			}
			result[k] = append(result[k], v...)
		}
	}
	return result
}

func expand(result map[abstract.TablePartID][]SerializedMessage, tablePartID abstract.TablePartID, maxMessageSize int) {
	result[tablePartID] = append(result[tablePartID], SerializedMessage{
		Key:   nil,
		Value: make([]byte, 0, maxMessageSize),
	})
}

func expandArrIfNeeded(result map[abstract.TablePartID][]SerializedMessage, tablePartID abstract.TablePartID, newMsg *SerializedMessage, maxMessageSize int) {
	if len(result[tablePartID]) == 0 {
		expand(result, tablePartID, maxMessageSize)
		return
	}
	index := len(result[tablePartID]) - 1
	if len(result[tablePartID][index].Value)+1+len(newMsg.Value) > maxMessageSize {
		expand(result, tablePartID, maxMessageSize)
		return
	}
}

func MergeWithMaxMessageSize(in []*task, maxMessageSize int) map[abstract.TablePartID][]SerializedMessage {
	result := make(map[abstract.TablePartID][]SerializedMessage)
	for _, currTask := range in {
		for tablePartID, v := range currTask.results {
			_, ok := result[tablePartID]
			if !ok {
				result[tablePartID] = make([]SerializedMessage, 0, len(v))
			}
			for _, currMessage := range v {
				expandArrIfNeeded(result, tablePartID, &currMessage, maxMessageSize)
				index := len(result[tablePartID]) - 1
				result[tablePartID][index].Value = append(result[tablePartID][index].Value, currMessage.Value...)
			}
		}
	}
	return result
}

//---

func MultithreadingSerialize(serializer serializerOneThread, sessionPackers packer.SessionPackers, input []abstract.ChangeItem, threadsNum, chunkSize, maxMessageSize int) (map[abstract.TablePartID][]SerializedMessage, error) {
	tasks := Split(serializer, sessionPackers, input, chunkSize)

	currWork := func(in interface{}) {
		currTask := tasks[in.(int)]
		currTask.Serialize()
	}

	currPool := pool.NewDefaultPool(currWork, uint64(threadsNum))
	_ = currPool.Run()
	for currTask := range tasks {
		_ = currPool.Add(currTask)
	}
	_ = currPool.Close()

	for _, currTask := range tasks {
		if currTask.err != nil {
			return nil, xerrors.Errorf("one of tasks returned error, err: %w", currTask.err)
		}
	}

	if maxMessageSize == 0 {
		return MergeBack(tasks), nil
	} else {
		return MergeWithMaxMessageSize(tasks, maxMessageSize), nil
	}
}
