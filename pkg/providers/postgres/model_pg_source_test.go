package postgres

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/ptr"
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

func TestPgDumpDefaults(t *testing.T) {
	src := PgSource{
		ClusterID: "my_cluster",
	}
	src.WithEssentialDefaults()
	require.NoError(t, src.Validate())
	require.Nil(t, src.PreSteps)
	require.Nil(t, src.PostSteps)

	src.WithDefaults()
	require.NoError(t, src.Validate())
	require.NotNil(t, src.PreSteps)
	require.NotNil(t, src.PostSteps)
}

func TestValidatePgDumpSteps_SequenceSetRequiresSequence(t *testing.T) {
	t.Run("PreSteps: SequenceSet without Sequence fails", func(t *testing.T) {
		src := PgSource{
			ClusterID: "c",
			PreSteps:  &PgDumpSteps{Sequence: false, SequenceSet: ptr.T(true)},
			PostSteps: DefaultPgDumpPostSteps(),
		}
		src.WithEssentialDefaults()
		require.Error(t, src.Validate())
	})
	t.Run("PostSteps: SequenceSet without Sequence fails", func(t *testing.T) {
		src := PgSource{
			ClusterID: "c",
			PreSteps:  DefaultPgDumpPreSteps(),
			PostSteps: &PgDumpSteps{Sequence: false, SequenceSet: ptr.T(true), Constraint: true, FkConstraint: true, Index: true, Trigger: true},
		}
		src.WithEssentialDefaults()
		require.Error(t, src.Validate())
	})
	t.Run("SequenceSet with Sequence in same step passes", func(t *testing.T) {
		src := PgSource{
			ClusterID: "c",
			PreSteps:  &PgDumpSteps{Sequence: true, SequenceSet: ptr.T(true)},
			PostSteps: DefaultPgDumpPostSteps(),
		}
		src.WithEssentialDefaults()
		require.NoError(t, src.Validate())
	})
}

func TestValidatePgDumpSteps_SequenceOwnedByRequiresSequence(t *testing.T) {
	t.Run("SequenceOwnedBy without Sequence in any phase fails", func(t *testing.T) {
		src := PgSource{
			ClusterID: "c",
			PreSteps:  &PgDumpSteps{Sequence: false, SequenceOwnedBy: true},
			PostSteps: DefaultPgDumpPostSteps(),
		}
		src.WithEssentialDefaults()
		require.Error(t, src.Validate())
	})
	t.Run("Sequence in PreSteps and SequenceOwnedBy in PostSteps is valid", func(t *testing.T) {
		src := PgSource{
			ClusterID: "c",
			PreSteps:  DefaultPgDumpPreSteps(),
			PostSteps: &PgDumpSteps{Sequence: false, SequenceOwnedBy: true, Constraint: true, FkConstraint: true, Index: true, Trigger: true},
		}
		src.WithEssentialDefaults()
		require.NoError(t, src.Validate())
	})
	t.Run("SequenceOwnedBy in PreSteps without Sequence in PreSteps fails", func(t *testing.T) {
		src := PgSource{
			ClusterID: "c",
			PreSteps:  &PgDumpSteps{Sequence: false, SequenceOwnedBy: true},
			PostSteps: &PgDumpSteps{Sequence: true, Constraint: true, FkConstraint: true, Index: true, Trigger: true},
		}
		src.WithEssentialDefaults()
		err := src.Validate()
		require.Error(t, err)
		require.Contains(t, err.Error(), "PreSteps")
	})
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
