package proto

import (
	"time"

	"github.com/transferia/transferia/pkg/parsers"
)

func constructMessage(curTime time.Time, buff []byte, key []byte) parsers.Message {
	return parsers.Message{
		Offset:     0,
		SeqNo:      0,
		Key:        key,
		CreateTime: curTime,
		WriteTime:  curTime,
		Value:      buff,
		Headers:    nil,
	}
}
