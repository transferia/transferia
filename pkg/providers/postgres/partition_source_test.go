package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/stats"
)

func TestNewPartitionSourceWrongPartition(t *testing.T) {
	src := &PgSource{SlotID: "expected-slot"}
	_, err := NewPartitionSource(
		src,
		"transfer-1",
		nil,
		abstract.Partition{Topic: "other-slot", Partition: 0},
		logger.Log,
		stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		coordinator.NewFakeClient(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected postgres queue-to-s3 partition")

	_, err = NewPartitionSource(
		src,
		"transfer-1",
		nil,
		abstract.Partition{Topic: "expected-slot", Partition: 1},
		logger.Log,
		stats.NewSourceStats(solomon.NewRegistry(solomon.NewRegistryOpts())),
		coordinator.NewFakeClient(),
	)
	require.Error(t, err)
	require.Contains(t, err.Error(), "unexpected postgres queue-to-s3 partition")
}
