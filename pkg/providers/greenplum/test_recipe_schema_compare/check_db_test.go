package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	provider_greenplum "github.com/transferia/transferia/pkg/providers/greenplum"
	provider_postgres "github.com/transferia/transferia/pkg/providers/postgres"
	"github.com/transferia/transferia/pkg/providers/postgres/pgrecipe"
	"github.com/transferia/transferia/tests/helpers"
)

var (
	pgSource = pgrecipe.RecipeSource(pgrecipe.WithPrefix(""), pgrecipe.WithInitDir("init_source"))
	gpSource = provider_greenplum.GpSource{
		Connection: provider_greenplum.GpConnection{
			OnPremises: &provider_greenplum.GpCluster{
				Coordinator: &provider_greenplum.GpHAP{
					Primary: &provider_greenplum.GpHP{
						Host: "localhost",
						Port: helpers.GetIntFromEnv("PG_LOCAL_PORT"),
					},
				},
				Segments: []*provider_greenplum.GpHAP{
					{Primary: new(provider_greenplum.GpHP)},
					{Primary: new(provider_greenplum.GpHP)},
				},
			},
			Database: os.Getenv("PG_LOCAL_DATABASE"),
			User:     os.Getenv("PG_LOCAL_USER"),
			AuthProps: provider_greenplum.PgAuthProps{
				Password: model.SecretString(os.Getenv("PG_LOCAL_PASSWORD")),
			},
		},
	}
)

func init() {
	_ = os.Setenv("YC", "1") // to not go to vanga
	pgSource.WithDefaults()
	gpSource.WithDefaults()
}

//---------------------------------------------------------------------------------------------------------------------

func TestSnapshot(t *testing.T) {
	defer require.NoError(t, helpers.CheckConnections(
		helpers.LabeledPort{Label: "PG source", Port: pgSource.Port},
	))

	//------------------------------------------------------------------------------
	// pg

	var pgColumns abstract.TableColumns

	pgStorage, err := provider_postgres.NewStorage(pgSource.ToStorageParams(nil))
	require.NoError(t, err)
	pgTableMap, err := pgStorage.TableList(nil)
	require.NoError(t, err)
	for _, v := range pgTableMap {
		pgColumns = v.Schema.Columns()
		pgTableMapArr, err := json.Marshal(pgColumns)
		require.NoError(t, err)
		pgTableMapStr := string(pgTableMapArr)
		fmt.Println(pgTableMapStr)
	}

	//------------------------------------------------------------------------------
	// gp

	var gpColumns abstract.TableColumns

	checkConnectionFunc := func(ctx context.Context, pgs *provider_postgres.Storage, expectedSP provider_greenplum.GPSegPointer) error {
		return nil
	}

	newFlavourFunc := func(in *provider_greenplum.Storage, _ *provider_postgres.Storage) provider_postgres.DBFlavour {
		return provider_greenplum.NewGreenplumFlavourImpl(
			in.WorkersCount() == 1,
			false,
			func(bool, bool, func(bool) string) string {
				return provider_postgres.NewPostgreSQLFlavour().PgClassFilter()
			},
			func(bool) string {
				return provider_postgres.NewPostgreSQLFlavour().PgClassRelsOnlyFilter()
			},
		)
	}

	gpStorage := provider_greenplum.NewStorageImpl(&gpSource, solomon.NewRegistry(nil), checkConnectionFunc, newFlavourFunc)
	gpTableMap, err := gpStorage.TableList(nil)
	require.NoError(t, err)
	for _, v := range gpTableMap {
		gpColumns = v.Schema.Columns()
		gpTableMapArr, err := json.Marshal(gpColumns)
		require.NoError(t, err)
		gpTableMapStr := string(gpTableMapArr)
		fmt.Println(gpTableMapStr)
	}

	//------------------------------------------------------------------------------

	require.Equal(t, pgColumns, gpColumns)
	for i := 0; i < len(pgColumns); i++ {
		require.Equal(t, pgColumns[i], gpColumns[i])
	}
}
