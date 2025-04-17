package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
)

func TestPgDumpStepsAnyStepIsTrue(t *testing.T) {
	steps := PgDumpSteps{}
	require.False(t, steps.AnyStepIsTrue())

	sequenceSet := false
	steps.SequenceSet = &sequenceSet
	require.False(t, steps.AnyStepIsTrue())

	steps.Type = true
	require.True(t, steps.AnyStepIsTrue())
}

func TestIncludeEmptyTable(t *testing.T) {
	src := PgSource{
		DBTables:       []string{"myspace.*"},
		ExcludedTables: []string{"myspace.mytable"},
	}
	src.WithDefaults()
	require.NoError(t, src.Validate())
	require.True(t, src.Include(*abstract.NewTableID("myspace", "")))
}

func TestIsPreferReplica(t *testing.T) {
	tests := []struct {
		name     string
		source   PgSource
		transfer *model.Transfer
		want     bool
	}{
		{
			name: "should return true for heterogeneous SNAPSHOT_ONLY transfer without DBLog",
			source: PgSource{
				IsHomo:       false,
				DBLogEnabled: false,
			},
			transfer: transferWithType(abstract.TransferTypeSnapshotOnly),
			want:     true,
		},
		{
			name: "should return false for homogeneous transfer",
			source: PgSource{
				IsHomo:       true,
				DBLogEnabled: false,
			},
			transfer: transferWithType(abstract.TransferTypeSnapshotOnly),
			want:     false,
		},
		{
			name: "should return false for nil transfer",
			source: PgSource{
				IsHomo:       false,
				DBLogEnabled: false,
			},
			transfer: nil,
			want:     false,
		},
		{
			name: "should return false for INCREMENT_ONLY transfer",
			source: PgSource{
				IsHomo:       false,
				DBLogEnabled: false,
			},
			transfer: transferWithType(abstract.TransferTypeIncrementOnly),
			want:     false,
		},
		{
			name: "should return false when DBLog is enabled",
			source: PgSource{
				IsHomo:       false,
				DBLogEnabled: true,
			},
			transfer: transferWithType(abstract.TransferTypeSnapshotOnly),
			want:     false,
		},
		{
			name: "should return true for non-INCREMENT_ONLY transfer",
			source: PgSource{
				IsHomo:       false,
				DBLogEnabled: false,
			},
			transfer: transferWithType(abstract.TransferTypeSnapshotOnly),
			want:     true,
		},
		{
			name: "should return false for heterogeneous snapshot and increment transfer with DBLog",
			source: PgSource{
				IsHomo:       false,
				DBLogEnabled: true,
			},
			transfer: transferWithType(abstract.TransferTypeSnapshotAndIncrement),
			want:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := tt.source.isPreferReplica(tt.transfer)
			require.Equal(t, tt.want, got)
		})
	}
}

func transferWithType(transferType abstract.TransferType) *model.Transfer {
	transfer := new(model.Transfer)
	transfer.Type = transferType

	return transfer
}
