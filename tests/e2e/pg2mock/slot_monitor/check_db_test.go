package main

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	pgcommon "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var Source = pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("init_source"))

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	Source.WithDefaults()
	Source.SlotByteLagLimit = 100
}

//---------------------------------------------------------------------------------------------------------------------
// mockSinker

type mockSinker struct {
	pushCallback func([]abstract.ChangeItem)
}

func (s *mockSinker) Close() error {
	return nil
}

func (s *mockSinker) Push(input []abstract.ChangeItem) error {
	s.pushCallback(input)
	return nil
}

//---------------------------------------------------------------------------------------------------------------------

func TestSnapshot(t *testing.T) {
	defer func() {
		require.NoError(t, helpers.CheckConnections(
			helpers.LabeledPort{Label: "PG source", Port: Source.Port},
		))
	}()

	// build transfer

	sinker := new(mockSinker)
	transfer := helpers.MakeTransfer(
		helpers.TransferID,
		Source,
		&model.MockDestination{SinkerFactory: func() abstract.Sinker {
			return sinker
		}},
		abstract.TransferTypeSnapshotAndIncrement,
	)
	inputs := make(chan []abstract.ChangeItem, 100)
	sinker.pushCallback = func(input []abstract.ChangeItem) {
		time.Sleep(6 * time.Second)
		inputs <- input
	}

	// activate

	worker, err := helpers.ActivateErr(transfer)
	if err != nil {
		if strings.Contains(err.Error(), "lag for replication slot") {
			return // everything is ok
		}
	}

	// insert data

	srcConn, err := pgcommon.MakeConnPoolFromSrc(Source, logger.Log)
	require.NoError(t, err)
	defer srcConn.Close()

	queries := []string{
		`INSERT INTO __test1 (id, value) VALUES ( 0, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa');`,
		`INSERT INTO __test1 (id, value) VALUES ( 1, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaab');`,
		`INSERT INTO __test1 (id, value) VALUES ( 2, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaac');`,
		`INSERT INTO __test1 (id, value) VALUES ( 3, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaad');`,
		`INSERT INTO __test1 (id, value) VALUES ( 4, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaae');`,
		`INSERT INTO __test1 (id, value) VALUES ( 5, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaf');`,
		`INSERT INTO __test1 (id, value) VALUES ( 6, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaag');`,
		`INSERT INTO __test1 (id, value) VALUES ( 7, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaah');`,
		`INSERT INTO __test1 (id, value) VALUES ( 8, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaai');`,
		`INSERT INTO __test1 (id, value) VALUES ( 9, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaj');`,
		`INSERT INTO __test1 (id, value) VALUES (10, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaak');`,
		`INSERT INTO __test1 (id, value) VALUES (11, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaal');`,
		`INSERT INTO __test1 (id, value) VALUES (12, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaam');`,
		`INSERT INTO __test1 (id, value) VALUES (13, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaan');`,
		`INSERT INTO __test1 (id, value) VALUES (14, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaao');`,
		`INSERT INTO __test1 (id, value) VALUES (15, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaap');`,
		`INSERT INTO __test1 (id, value) VALUES (16, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaq');`,
		`INSERT INTO __test1 (id, value) VALUES (17, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaar');`,
		`INSERT INTO __test1 (id, value) VALUES (18, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaas');`,
		`INSERT INTO __test1 (id, value) VALUES (19, 'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaat');`,
	}

	for _, currQuery := range queries {
		_, err = srcConn.Exec(context.Background(), currQuery)
		require.NoError(t, err)
	}

	// check

	err = worker.CloseWithErr()
	require.Error(t, err)
	require.Contains(t, err.Error(), "lag for replication slot")
}
