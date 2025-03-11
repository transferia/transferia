package queue

import (
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

func logErrorWithChangeItem(logger log.Logger, msg string, err error, changeItem *abstract.ChangeItem) {
	changeItemJSON := changeItem.ToJSONString()
	logger.Error(msg, log.Error(err), log.Any("change_item", util.DefaultSample(changeItemJSON)))
}
