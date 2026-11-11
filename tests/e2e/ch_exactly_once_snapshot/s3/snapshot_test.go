package s3

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"testing"

	"github.com/parquet-go/parquet-go"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	"github.com/transferia/transferia/pkg/providers/s3/s3recipe"
	eo "github.com/transferia/transferia/tests/e2e/ch_exactly_once_snapshot"
	_ "github.com/transferia/transferia/tests/helpers/registration/s3"
)

func TestExactlyOnceSnapshot(test *testing.T) {
	eo.ForDatabases(test, func(test *testing.T) {
		for _, formatName := range []string{"csv", "csv_gzip", "jsonline", "dynamojson", "line", "parquet", "nginx"} {
			for _, workers := range []int{1, 2} {
				test.Run(fmt.Sprintf("%s/workers%d", formatName, workers), func(test *testing.T) {
					format := model.ParsingFormatCSV
					switch formatName {
					case "jsonline":
						format = model.ParsingFormatJSONLine
					case "dynamojson":
						format = model.ParsingFormatJSON
					case "line":
						format = model.ParsingFormatLine
					case "parquet":
						format = model.ParsingFormatPARQUET
					case "nginx":
						format = model.ParsingFormatNginx
					}
					source := s3recipe.PrepareCfg(test, fmt.Sprintf("eo-%s-%d", formatName, workers), format)
					source.TableNamespace = "public"
					source.TableName = "rows"
					source.PathPrefix = "rows/"
					if formatName == "nginx" {
						source.Format.NginxSetting = &s3_model.NginxSetting{Format: `"$id" "$payload"`, BlockSize: 4096}
					}
					if formatName != "line" && formatName != "parquet" {
						source.OutputSchema = []abstract.ColSchema{{ColumnName: "id", DataType: "int64", PrimaryKey: true}, {ColumnName: "payload", DataType: "utf8"}}
						if formatName == "dynamojson" {
							source.Format.JSONLSetting = &s3_model.JSONLSetting{BlockSize: 1024 * 1024}
							source.OutputSchema[0].Path = "Item.id.N"
							source.OutputSchema[1].Path = "Item.payload.S"
						}
					}
					for objectIndex := 0; objectIndex < 4; objectIndex++ {
						var content bytes.Buffer
						parquetRows := make([]parquetRow, 0, 64)
						for rowIndex := 1; rowIndex <= 64; rowIndex++ {
							id := objectIndex*64 + rowIndex
							switch formatName {
							case "csv", "csv_gzip":
								fmt.Fprintf(&content, "%d,row-%d\n", id, id)
							case "nginx":
								fmt.Fprintf(&content, "\"%d\" \"row-%d\"\n", id, id)
							case "jsonline":
								fmt.Fprintf(&content, "{\"id\":%d,\"payload\":\"row-%d\"}\n", id, id)
							case "dynamojson":
								fmt.Fprintf(&content, "{\"Item\":{\"id\":{\"N\":\"%d\"},\"payload\":{\"S\":\"row-%d\"}}}\n", id, id)
							case "line":
								fmt.Fprintf(&content, "row-%03d\n", id)
							case "parquet":
								parquetRows = append(parquetRows, parquetRow{ID: int64(id), Payload: fmt.Sprintf("row-%d", id)})
							}
						}
						extension := ".data"
						if formatName == "parquet" {
							encoder := parquet.NewGenericWriter[parquetRow](&content)
							_, err := encoder.Write(parquetRows)
							require.NoError(test, err)
							require.NoError(test, encoder.Close())
							extension = ".parquet"
						}
						if formatName == "csv_gzip" {
							var compressed bytes.Buffer
							encoder := gzip.NewWriter(&compressed)
							_, err := encoder.Write(content.Bytes())
							require.NoError(test, err)
							require.NoError(test, encoder.Close())
							content = compressed
							extension = ".csv.gz"
						}
						s3recipe.UploadOneFromMemory(test, source, fmt.Sprintf("rows/%d%s", objectIndex, extension), content.Bytes())
					}
					target := eo.NewTarget(test, "")
					eo.Run(test, source, target, workers)
					eo.CheckRows(test, target, "rows", 256)
					if formatName != "line" {
						eo.CheckKeyPayload(test, target, "rows", 256)
					} else {
						connection := eo.OpenTarget(test, target)
						rows, err := connection.Query("SELECT row FROM `" + target.Database + "`.rows ORDER BY row")
						require.NoError(test, err)
						defer rows.Close()
						rowIndex := 0
						for rows.Next() {
							var value string
							require.NoError(test, rows.Scan(&value))
							rowIndex++
							require.Equal(test, fmt.Sprintf("row-%03d", rowIndex), value)
						}
						require.NoError(test, rows.Err())
						require.Equal(test, 256, rowIndex)
					}
				})
			}
		}
	})
}
