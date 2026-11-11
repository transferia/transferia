package yt

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/yt_client"
	eo "github.com/transferia/transferia/tests/e2e/ch_exactly_once_snapshot"
	_ "github.com/transferia/transferia/tests/helpers/registration/yt"
	ytschema "go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func TestExactlyOnceSnapshot(test *testing.T) {
	client, err := yt_client.NewYtClientWrapper(yt_client.HTTP, nil, &yt.Config{Proxy: os.Getenv("YT_PROXY")})
	require.NoError(test, err)
	const sourcePath = "//home/eo_snapshot"
	const expectedRows = 60000
	for _, tableName := range []string{"rows", "empty"} {
		schema := ytschema.Schema{Columns: []ytschema.Column{{Name: "id", ComplexType: ytschema.TypeInt64, SortOrder: ytschema.SortAscending}, {Name: "payload", ComplexType: ytschema.TypeString}}}
		tableWriter, err := yt.WriteTable(context.Background(), client, ypath.Path(sourcePath+"/"+tableName), yt.WithCreateOptions(yt.WithSchema(schema), yt.WithRecursive()))
		require.NoError(test, err)
		if tableName == "rows" {
			for rowIndex := 1; rowIndex <= expectedRows; rowIndex++ {
				require.NoError(test, tableWriter.Write(map[string]interface{}{"id": int64(rowIndex), "payload": fmt.Sprintf("row-%d", rowIndex)}))
			}
		}
		require.NoError(test, tableWriter.Commit())
	}
	eo.ForDatabases(test, func(test *testing.T) {
		for _, workers := range []int{1, 2} {
			test.Run(fmt.Sprintf("workers%d", workers), func(test *testing.T) {
				source := &provider_yt.YtSource{Cluster: os.Getenv("YT_PROXY"), YtProxy: os.Getenv("YT_PROXY"), Paths: []string{sourcePath}, RowIdxColumnName: "row_idx", DesiredPartSizeBytes: 1024}
				target := eo.NewTarget(test, "")
				eo.Run(test, source, target, workers)
				eo.CheckRows(test, target, "rows", expectedRows)
				eo.CheckRows(test, target, "empty", 0)
				eo.CheckKeyPayload(test, target, "rows", expectedRows)
			})
		}
	})
}
