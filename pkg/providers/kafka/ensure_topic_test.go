package kafka

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// MockRequestor implements kmsg.Requestor for testing
type MockRequestor struct {
	response *kmsg.MetadataResponse
	err      error
}

func (m *MockRequestor) Request(_ context.Context, _ kmsg.Request) (kmsg.Response, error) {
	if m.err != nil {
		return nil, m.err
	}
	return m.response, nil
}

func TestEnsureTopicExists(t *testing.T) {
	t.Run("should return error when requestor fails", func(t *testing.T) {
		// Arrange
		expectedErr := xerrors.New("connection failed")
		requestor := &MockRequestor{err: expectedErr}
		topics := []string{"test-topic"}

		// Act
		err := ensureTopicExists(requestor, time.Second, topics)

		// Assert
		require.Error(t, err)
		require.ErrorIs(t, err, expectedErr)
		require.False(t, abstract.IsFatal(err))
	})

	t.Run("should return fatal error when topic not found", func(t *testing.T) {
		// Arrange
		topicName := "non-existent-topic"
		response := &kmsg.MetadataResponse{
			Topics: []kmsg.MetadataResponseTopic{
				{
					Topic:      kmsg.StringPtr(topicName),
					ErrorCode:  kerr.UnknownTopicOrPartition.Code,
					Partitions: []kmsg.MetadataResponseTopicPartition{},
				},
			},
		}
		requestor := &MockRequestor{response: response}
		topics := []string{topicName}

		// Act
		err := ensureTopicExists(requestor, 3*time.Second, topics)

		// Assert
		require.Error(t, err)
		require.True(t, abstract.IsFatal(err))

		// Check error code
		var unwrapErr interface {
			Unwrap() error
		}
		require.ErrorAs(t, err, &unwrapErr)

		var codedErr interface {
			Code() coded.Code
		}
		require.ErrorAs(t, unwrapErr.Unwrap(), &codedErr)
		require.Equal(t, codes.MissingData, codedErr.Code())
	})

	t.Run("should not return error when topic has partitions despite error code", func(t *testing.T) {
		// Arrange
		topicName := "topic-with-partitions"
		response := &kmsg.MetadataResponse{
			Topics: []kmsg.MetadataResponseTopic{
				{
					Topic:     kmsg.StringPtr(topicName),
					ErrorCode: kerr.UnknownTopicOrPartition.Code,
					Partitions: []kmsg.MetadataResponseTopicPartition{
						{Partition: 0, Leader: 0},
					},
				},
			},
		}
		requestor := &MockRequestor{response: response}
		topics := []string{topicName}

		// Act
		err := ensureTopicExists(requestor, time.Second, topics)

		// Assert
		require.NoError(t, err)
	})

	t.Run("should return fatal error when multiple topics not found", func(t *testing.T) {
		// Arrange
		missingTopics := []string{"topic1", "topic2"}
		response := &kmsg.MetadataResponse{
			Topics: []kmsg.MetadataResponseTopic{
				{
					Topic:      kmsg.StringPtr("topic1"),
					ErrorCode:  kerr.UnknownTopicOrPartition.Code,
					Partitions: []kmsg.MetadataResponseTopicPartition{},
				},
				{
					Topic:      kmsg.StringPtr("topic2"),
					ErrorCode:  kerr.UnknownTopicOrPartition.Code,
					Partitions: []kmsg.MetadataResponseTopicPartition{},
				},
				{
					Topic:     kmsg.StringPtr("existing-topic"),
					ErrorCode: 0, // No error
					Partitions: []kmsg.MetadataResponseTopicPartition{
						{Partition: 0, Leader: 0},
					},
				},
			},
		}
		requestor := &MockRequestor{response: response}
		topics := []string{"topic1", "topic2", "existing-topic"}

		// Act
		err := ensureTopicExists(requestor, time.Second, topics)

		// Assert
		require.Error(t, err)
		require.True(t, abstract.IsFatal(err))
		// Check that error message contains missing topics
		errorMsg := err.Error()
		for _, topic := range missingTopics {
			require.Contains(t, errorMsg, topic)
		}
	})

	t.Run("should succeed when all topics exist", func(t *testing.T) {
		// Arrange
		topics := []string{"topic1", "topic2", "topic3"}
		response := &kmsg.MetadataResponse{
			Topics: []kmsg.MetadataResponseTopic{
				{
					Topic:     kmsg.StringPtr("topic1"),
					ErrorCode: 0,
					Partitions: []kmsg.MetadataResponseTopicPartition{
						{Partition: 0, Leader: 0},
					},
				},
				{
					Topic:     kmsg.StringPtr("topic2"),
					ErrorCode: 0,
					Partitions: []kmsg.MetadataResponseTopicPartition{
						{Partition: 0, Leader: 0},
					},
				},
				{
					Topic:     kmsg.StringPtr("topic3"),
					ErrorCode: 0,
					Partitions: []kmsg.MetadataResponseTopicPartition{
						{Partition: 0, Leader: 0},
					},
				},
			},
		}
		requestor := &MockRequestor{response: response}

		// Act
		err := ensureTopicExists(requestor, time.Second, topics)

		// Assert
		require.NoError(t, err)
	})

	t.Run("should handle nil topic pointer gracefully", func(t *testing.T) {
		// Arrange
		response := &kmsg.MetadataResponse{
			Topics: []kmsg.MetadataResponseTopic{
				{
					Topic:      nil, // Nil topic pointer
					ErrorCode:  kerr.UnknownTopicOrPartition.Code,
					Partitions: []kmsg.MetadataResponseTopicPartition{},
				},
			},
		}
		requestor := &MockRequestor{response: response}
		topics := []string{"some-topic"}

		// Act
		err := ensureTopicExists(requestor, time.Second, topics)

		// Assert
		require.Error(t, err)
		require.True(t, abstract.IsFatal(err))
	})

	t.Run("should respect context timeout", func(t *testing.T) {
		// Arrange
		requestor := &MockRequestor{
			err: context.DeadlineExceeded,
		}
		topics := []string{"test-topic"}

		// Act
		err := ensureTopicExists(requestor, time.Second, topics)

		// Assert
		require.Error(t, err)
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})
}

func TestUnderlyingFunctionFail(t *testing.T) {
	// should return error when underlying function fails

	// This test is simplified since we cannot mock the ensureTopicExists function directly
	// We'll test the integration by ensuring the function properly wraps errors
	expectedErr := xerrors.New("connection failed")
	requestor := &MockRequestor{err: expectedErr}

	// We cannot test the retry logic directly without mocking, but we can test error propagation
	// The actual retry logic would require more complex mocking setup
	err := ensureTopicExists(requestor, time.Second, []string{"test-topic"})

	require.Error(t, err)
	require.ErrorIs(t, err, expectedErr)
}
