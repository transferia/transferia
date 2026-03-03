package reader

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

type mockAdminClient struct {
	group   string
	offsets kadm.OffsetResponses
}

func (c *mockAdminClient) FetchOffsets(_ context.Context, group string) (kadm.OffsetResponses, error) {
	if c.group != group {
		return nil, xerrors.New("group does not match")
	}
	return c.offsets, nil
}

func (c *mockAdminClient) CommitOffsets(_ context.Context, _ string, _ kadm.Offsets) (kadm.OffsetResponses, error) {
	return nil, nil
}

func (c *mockAdminClient) ListTopics(_ context.Context, _ ...string) (kadm.TopicDetails, error) {
	return nil, nil
}

func newMockClient(group string, offsets kadm.OffsetResponses) *mockAdminClient {
	return &mockAdminClient{
		group:   group,
		offsets: offsets,
	}
}

func TestFetchPartitionOffset(t *testing.T) {
	const testGroup = "test_consumer_group"
	const testTopic = "test_topic"
	const testPartition = 7

	t.Run("GroupDoesNotExist", func(t *testing.T) {
		testAdminClient := newMockClient("some_another_group", map[string]map[int32]kadm.OffsetResponse{
			testTopic: {testPartition: kadm.OffsetResponse{}},
		})

		resOffset, err := fetchPartitionNextOffset(testGroup, testPartition, testTopic, testAdminClient)
		require.Error(t, err)
		require.Equal(t, kgo.Offset{}, resOffset)
	})

	t.Run("TopicDoesNotExist", func(t *testing.T) {
		testAdminClient := newMockClient(testGroup, map[string]map[int32]kadm.OffsetResponse{
			"some_another_topic": {testPartition: kadm.OffsetResponse{}},
		})

		resOffset, err := fetchPartitionNextOffset(testGroup, testPartition, testTopic, testAdminClient)
		require.NoError(t, err)
		require.Equal(t, kgo.NewOffset().At(-2), resOffset)
	})

	t.Run("PartitionDoesNotExist", func(t *testing.T) {
		testAdminClient := newMockClient(testGroup, map[string]map[int32]kadm.OffsetResponse{
			testTopic: {1: kadm.OffsetResponse{}},
		})

		resOffset, err := fetchPartitionNextOffset(testGroup, testPartition, testTopic, testAdminClient)
		require.NoError(t, err)
		require.Equal(t, kgo.NewOffset().At(-2), resOffset)
	})

	t.Run("Errorkadm.OffsetResponse", func(t *testing.T) {
		testAdminClient := newMockClient(testGroup, map[string]map[int32]kadm.OffsetResponse{
			"some_another_topic": {testPartition: kadm.OffsetResponse{Err: xerrors.New("some error")}},
		})

		resOffset, err := fetchPartitionNextOffset(testGroup, testPartition, testTopic, testAdminClient)
		require.NoError(t, err)
		require.Equal(t, kgo.NewOffset().At(-2), resOffset)
	})

	t.Run("ErrorInRequiredkadm.OffsetResponse", func(t *testing.T) {
		testAdminClient := newMockClient(testGroup, map[string]map[int32]kadm.OffsetResponse{
			testTopic: {testPartition: kadm.OffsetResponse{Err: xerrors.New("some error")}},
		})

		resOffset, err := fetchPartitionNextOffset(testGroup, testPartition, testTopic, testAdminClient)
		require.Error(t, err)
		require.Equal(t, kgo.Offset{}, resOffset)
	})

	t.Run("ZeroOffset", func(t *testing.T) {
		testAdminClient := newMockClient(testGroup, map[string]map[int32]kadm.OffsetResponse{
			testTopic: {testPartition: kadm.OffsetResponse{
				Offset: kadm.Offset{
					Topic:       testTopic,
					Partition:   testPartition,
					At:          0,
					LeaderEpoch: -1,
					Metadata:    "",
				},
			}},
		})

		resOffset, err := fetchPartitionNextOffset(testGroup, testPartition, testTopic, testAdminClient)
		require.NoError(t, err)
		require.Equal(t, kgo.NewOffset().At(1), resOffset)
	})

	t.Run("NotZeroOffset", func(t *testing.T) {
		testAdminClient := newMockClient(testGroup, map[string]map[int32]kadm.OffsetResponse{
			testTopic: {testPartition: kadm.OffsetResponse{
				Offset: kadm.Offset{
					Topic:       testTopic,
					Partition:   testPartition,
					At:          1500,
					LeaderEpoch: -1,
					Metadata:    "",
				},
			}},
		})

		resOffset, err := fetchPartitionNextOffset(testGroup, testPartition, testTopic, testAdminClient)
		require.NoError(t, err)
		require.Equal(t, kgo.NewOffset().At(1501), resOffset)
	})
}
