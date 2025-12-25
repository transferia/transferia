package proto

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConstructMessage(t *testing.T) {
	timeNow := time.Now()
	for _, test := range []struct {
		time time.Time
		key  []byte
		buff []byte
	}{
		{
			time: timeNow,
			key:  []byte("test1"),
			buff: []byte("test2"),
		},
		{
			time: timeNow.Add(-time.Hour),
			key:  []byte("test1"),
			buff: []byte("test2"),
		},
		{
			time: timeNow.Add(-time.Second),
			key:  nil,
			buff: []byte("test3"),
		},
		{
			time: timeNow.Add(-time.Second),
			key:  []byte("test1"),
			buff: nil,
		},
		{
			time: time.Time{},
			key:  nil,
			buff: nil,
		},
	} {
		message := constructMessage(test.time, test.buff, test.key)
		require.Equal(t, message.Value, test.buff)
		require.Equal(t, message.Key, test.key)
		require.Equal(t, message.CreateTime, test.time)
		require.Equal(t, message.WriteTime, test.time)
		require.Nil(t, message.Headers)
		require.Empty(t, message.Offset)
		require.Empty(t, message.SeqNo)
	}
}
