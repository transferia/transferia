//go:build !disable_s3_provider

package sink

type ObjectRange struct {
	From uint64 `json:"from"`
	To   uint64 `json:"to"`
}

func (o *ObjectRange) isEqual(object ObjectRange) bool {
	return o.From == object.From && o.To == object.To
}

func (o *ObjectRange) isSubset(object ObjectRange) bool {
	return o.From >= object.From && o.To <= object.To
}

func NewObjectRange(from, to uint64) ObjectRange {
	return ObjectRange{
		From: from,
		To:   to,
	}
}
