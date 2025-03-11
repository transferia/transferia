package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	logger "github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/parsers"
)

func TestDo(t *testing.T) {
	byteData := []byte{}
	byteData = append(byteData, 0xC0, 0xC1, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9)

	stringAndByteData := []byte{}
	for row := range []any{"Hi", "Bye", "Hello"} {
		stringAndByteData = append(stringAndByteData, byte(row))
	}
	stringAndByteData = append(stringAndByteData, 0xC0, 0xC1, 0xF5, 0xF6, 0xF7, 0xF8, 0xF9)

	tu := time.Unix(0, int64(time.Now().Unix()))

	testCases := []struct {
		testCaseName   string
		inputData      []byte
		kafkaConfig    *kafkaConfig
		srcID          []byte
		expectedResult []any
	}{
		{
			"MessageWithNonUTF8Chars",
			[]byte("Hello World!"),
			&kafkaConfig{
				isAddTimestamp: true,
				isAddHeaders:   false,
				isAddKey:       false,
				isKeyString:    true,
				isValueString:  true,
				tableName:      "test_test",
				isTopicAsName:  true,
			},
			nil,
			[]any{
				"test_test",
				uint32(1),
				uint32(200),
				tu,
				"Hello World!",
			},
		},
		{
			"MessageWitValidUTF8Chars",
			byteData,
			&kafkaConfig{
				isAddTimestamp: true,
				isAddHeaders:   false,
				isAddKey:       false,
				isKeyString:    false,
				isValueString:  false,
				tableName:      "test_test",
				isTopicAsName:  true,
			},
			nil,
			[]any{
				"test_test",
				uint32(1),
				uint32(200),
				tu,
				byteData,
			},
		},
		{
			"MessageWitUTF8AndValidNonUTF8Chars",
			stringAndByteData,
			&kafkaConfig{
				isAddTimestamp: true,
				isAddHeaders:   false,
				isAddKey:       false,
				isKeyString:    false,
				isValueString:  false,
				tableName:      "test_test",
				isTopicAsName:  true,
			},
			nil,
			[]any{
				"test_test",
				uint32(1),
				uint32(200),
				tu,
				stringAndByteData,
			},
		},
		{
			"MessageWithNonUTF8CharsAndInvalidUTF8Chars",
			stringAndByteData,
			&kafkaConfig{
				isAddTimestamp: true,
				isAddHeaders:   false,
				isAddKey:       true,
				isKeyString:    true,
				isValueString:  true,
				tableName:      "test_dlq",
				isTopicAsName:  true,
			},
			nil,
			[]any{
				"test_test",
				uint32(1),
				uint32(200),
				tu,
				map[string]string(nil),
				string(stringAndByteData),
			},
		},
		{
			"MessageWithInvalidSourceID",
			[]byte("Hello World!"),
			&kafkaConfig{
				isAddTimestamp: true,
				isAddHeaders:   false,
				isAddKey:       true,
				isKeyString:    true,
				isValueString:  false,
				tableName:      "test_dlq",
				isTopicAsName:  true,
			},
			byteData,
			[]any{
				"test_test",
				uint32(1),
				uint32(200),
				tu,
				map[string]string(nil),
				"Hello World!",
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.testCaseName, func(t *testing.T) {
			ptr := NewRawToTable(
				logger.NewConsoleLogger(),
				tc.kafkaConfig.isAddTimestamp,
				tc.kafkaConfig.isAddHeaders,
				tc.kafkaConfig.isAddKey,
				tc.kafkaConfig.isKeyString,
				tc.kafkaConfig.isValueString,
				tc.kafkaConfig.isTopicAsName,
				"test",
			)
			result := ptr.Do(parsers.Message{
				Offset:     200,
				SeqNo:      4965502264652803543,
				Key:        tc.srcID,
				CreateTime: tu,
				WriteTime:  tu,
				Value:      tc.inputData,
				Headers:    nil,
			}, abstract.Partition{
				Cluster:   "",
				Partition: 1,
				Topic:     "test_test",
			})
			require.Equal(
				t,
				tc.expectedResult,
				result[0].ColumnValues,
			)
			require.Equal(t, tc.kafkaConfig.tableName, result[0].Table)
		})
	}
}
