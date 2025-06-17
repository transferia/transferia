//go:build !disable_bigquery_provider

package bigquery

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/changeitem"
)

func TestSimpleTable(t *testing.T) {
	creds, ok := os.LookupEnv("GCP_CREDS")
	if !ok {
		t.Skip()
	}
	snkr, err := NewSink(
		&BigQueryDestination{
			ProjectID: "mdb-dp-preprod",
			Dataset:   "transfer_sinker_demo",
			Creds:     creds,
		},
		logger.Log,
		solomon.NewRegistry(solomon.NewRegistryOpts()),
	)
	items := generateRawMessages("test", 0, 0, 200000)
	require.NoError(t, err)
	require.NoError(t, snkr.Push([]abstract.ChangeItem{{Kind: changeitem.DropTableKind, Schema: items[0].Schema, Table: items[0].Table}}))
	require.NoError(t, snkr.Push(abstract.MakeInitTableLoad(abstract.LogPosition{ID: items[0].ID, LSN: items[0].LSN, TxID: items[0].TxID}, abstract.TableDescription{Name: items[0].Table, Schema: items[0].Schema}, time.Now(), items[0].TableSchema)))
	require.NoError(t, snkr.Push(items))
	require.NoError(t, snkr.Push(abstract.MakeDoneTableLoad(abstract.LogPosition{ID: items[0].ID, LSN: items[0].LSN, TxID: items[0].TxID}, abstract.TableDescription{Name: items[0].Table, Schema: items[0].Schema}, time.Now(), items[0].TableSchema)))
}

func generateRawMessages(table string, part, from, to int) []abstract.ChangeItem {
	ciTime := time.Date(2022, time.Month(10), 19, 0, 0, 0, 0, time.UTC)
	var res []abstract.ChangeItem
	for i := from; i < to; i++ {
		res = append(res, abstract.MakeRawMessage(
			table,
			ciTime,
			"test-topic",
			part,
			int64(i),
			[]byte(fmt.Sprintf("test_part_%v_value_%v", part, i)),
		))
	}
	return res
}
