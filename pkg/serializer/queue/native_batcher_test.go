package queue

import (
	"testing"

	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
)

func TestNativeBatcher(t *testing.T) {
	commonTest(t, func(batchingSettings model.Batching) Serializer {
		result, _ := NewNativeSerializer(batchingSettings, true)
		return result
	}, func(in abstract.ChangeItem) int {
		return len(in.ToJSONString())
	}, func(elementsSize, elementsNum int) int64 {
		commasNum := int64(elementsNum - 1)
		bracketsCount := int64(2)
		return bracketsCount + commasNum + int64(elementsSize*elementsNum)
	})
}
