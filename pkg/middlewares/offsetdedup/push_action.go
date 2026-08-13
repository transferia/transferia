package offsetdedup

import "github.com/transferia/transferia/pkg/abstract"

// pushAction describes exactly one outcome of preparing a push: either write
// items to the sink or return a result directly to the source.
type pushAction struct {
	itemsToWrite    []abstract.ChangeItem
	immediateResult abstract.AsyncPushResult
}

func newPushAction(itemsToWrite []abstract.ChangeItem, immediateResult abstract.AsyncPushResult) pushAction {
	if (len(itemsToWrite) != 0) == (immediateResult != nil) {
		panic("push action must either write items or return an immediate result")
	}
	return pushAction{
		itemsToWrite:    itemsToWrite,
		immediateResult: immediateResult,
	}
}
