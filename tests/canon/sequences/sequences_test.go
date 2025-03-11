package sequences

import (
	"context"
	_ "embed"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
	"github.com/transferria/transferria/pkg/abstract/model"
	pgcommon "github.com/transferria/transferria/pkg/providers/postgres"
	"github.com/transferria/transferria/tests/canon"
	"github.com/transferria/transferria/tests/canon/validator"
	"github.com/transferria/transferria/tests/helpers"
)

var (
	//go:embed dump/00_insert_update_delete.sql
	insertUpdateDelete []byte
	//go:embed dump/01_updatepk.sql
	updatePK []byte
	//go:embed dump/02_insert_update_insert.sql
	insertUpdateInsert []byte
	//go:embed dump/init.insert_update_delete.sql
	initInsertUpdateDelete []byte
)

func TestCanonizeSequences(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga
	Source := &pgcommon.PgSource{
		ClusterID: os.Getenv("PG_CLUSTER_ID"),
		Hosts:     []string{"localhost"},
		User:      os.Getenv("PG_LOCAL_USER"),
		Password:  model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
		Database:  os.Getenv("PG_LOCAL_DATABASE"),
		Port:      helpers.GetIntFromEnv("PG_LOCAL_PORT"),
		SlotID:    "test_slot_id",
	}
	Source.WithDefaults()
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	conn, err := pgcommon.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)

	canonizationCase := func(initSQL string, replicationSQL string) func(t *testing.T) {
		return func(t *testing.T) {
			_, err = conn.Exec(context.Background(), initSQL)
			require.NoError(t, err)

			sequencer, produceSequenceDump := validator.Sequencer(t, DropNonRowKindsExceptRowMiddleware(), validator.RemoveVariableFieldsRowMiddleware, SynthesizeCommitTimeRowMiddleware())

			transfer := helpers.MakeTransfer(
				helpers.TransferID,
				Source,
				&model.MockDestination{
					SinkerFactory: validator.New(model.IsStrictSource(Source), sequencer),
					Cleanup:       model.Drop,
				},
				abstract.TransferTypeSnapshotAndIncrement,
			)

			defer produceSequenceDump()
			worker := helpers.Activate(t, transfer)
			defer worker.Close(t)

			_, err = conn.Exec(context.Background(), replicationSQL)
			require.NoError(t, err)
			time.Sleep(time.Second * 10)
		}
	}

	t.Run(canon.SequenceTestCases[0], canonizationCase(string(initInsertUpdateDelete), string(insertUpdateDelete)))
	t.Run(canon.SequenceTestCases[1], canonizationCase(string(initInsertUpdateDelete), string(updatePK)))
	t.Run(canon.SequenceTestCases[2], canonizationCase(string(initInsertUpdateDelete), string(insertUpdateInsert)))
	// new cases should be added here. The name of the cases MUST be placed in canon.SequenceTestCases so that the change in the set of cases is automatically propagated to all sinks under test
}

// DropNonRowKindsExceptRowMiddleware drops all non-row items except the ones whose kind is among the given kinds
func DropNonRowKindsExceptRowMiddleware(preserveKinds ...abstract.Kind) func([]abstract.ChangeItem) []abstract.ChangeItem {
	preserveKindsMap := make(map[abstract.Kind]bool)
	for _, k := range preserveKinds {
		preserveKindsMap[k] = true
	}
	return func(items []abstract.ChangeItem) []abstract.ChangeItem {
		result := make([]abstract.ChangeItem, 0)
		for i := range items {
			if !items[i].IsRowEvent() && !preserveKindsMap[items[i].Kind] {
				continue
			}
			result = append(result, items[i])
		}
		return result
	}
}

// SynthesizeCommitTimeRowMiddleware sets synthetic sequential CommitTime for all items that it processes
func SynthesizeCommitTimeRowMiddleware() func([]abstract.ChangeItem) []abstract.ChangeItem {
	return func(items []abstract.ChangeItem) []abstract.ChangeItem {
		for i := range items {
			items[i].CommitTime = uint64(i + 1)
		}
		return items
	}
}
