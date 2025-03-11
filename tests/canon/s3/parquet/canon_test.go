package parquet

import (
	_ "embed"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/tests/canon/validator"
	"github.com/transferia/transferia/tests/helpers"
)

func TestUnsopportedData(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga
	absPath, err := filepath.Abs("unsupported_data")
	require.NoError(t, err)
	files, err := os.ReadDir(absPath)
	require.NoError(t, err)
	src := s3.PrepareCfg(t, "canon-parquet-bad", "")
	testCasePath := "data"
	src.PathPrefix = testCasePath
	s3.CreateBucket(t, src)
	s3.PrepareTestCase(t, src, "data")
	for _, file := range files {
		t.Run(file.Name(), func(t *testing.T) {
			src.TableNamespace = "s3_source_parquet"
			src.TableName = file.Name()
			src.InputFormat = model.ParsingFormatPARQUET
			src.PathPattern = "data/" + file.Name()
			src.WithDefaults()

			transfer := helpers.MakeTransfer(
				helpers.TransferID,
				src,
				&model.MockDestination{
					SinkerFactory: validator.New(model.IsStrictSource(src)),
					Cleanup:       model.Drop,
				},
				abstract.TransferTypeSnapshotOnly,
			)
			_, err = helpers.ActivateErr(transfer)
			require.Error(t, err)
		})
	}
}

// rowsCutter will limit number of rows pushed to child sink
type rowsCutter struct {
	sink   abstract.Sinker
	pushed bool
}

func (r *rowsCutter) Close() error {
	if !r.pushed {
		return xerrors.New("where is my data Lebovsky?")
	}
	return r.sink.Close()
}

func (r *rowsCutter) Push(items []abstract.ChangeItem) error {
	var filteredRows []abstract.ChangeItem
	for _, row := range items {
		if row.IsRowEvent() {
			filteredRows = append(filteredRows, row)
		}
	}
	if len(filteredRows) == 0 {
		return nil
	}
	r.pushed = true
	if len(filteredRows) > 3 {
		return r.sink.Push(filteredRows[:3]) // funny cat face :3
	}
	return r.sink.Push(filteredRows)
}

func TestCanonSource(t *testing.T) {
	t.Setenv("YC", "1") // to not go to vanga
	absPath, err := filepath.Abs("data")
	require.NoError(t, err)
	files, err := os.ReadDir(absPath)
	require.NoError(t, err)
	src := s3.PrepareCfg(t, "canon-parquet", "")
	testCasePath := "data"
	src.PathPrefix = testCasePath
	s3.CreateBucket(t, src)
	s3.PrepareTestCase(t, src, "data")

	for _, file := range files {
		t.Run(file.Name(), func(t *testing.T) {
			src.TableNamespace = "s3_source_parquet"
			src.TableName = file.Name()
			src.InputFormat = model.ParsingFormatPARQUET
			src.PathPattern = "data/" + file.Name()
			src.WithDefaults()

			transfer := helpers.MakeTransfer(
				helpers.TransferID,
				src,
				&model.MockDestination{
					SinkerFactory: func() abstract.Sinker {
						return &rowsCutter{
							sink: validator.New(
								model.IsStrictSource(src),
								validator.InitDone(t),
								validator.ValuesTypeChecker,
								validator.Canonizator(t),
								validator.TypesystemChecker(s3.ProviderType, func(colSchema abstract.ColSchema) string {
									clearType := strings.ReplaceAll(colSchema.OriginalType, "optional", "")
									re := regexp.MustCompile(`\(.*\)$`) // Matches the last parenthesis and its contents
									return re.ReplaceAllString(clearType, "")
								}),
							)(),
						}
					},
					Cleanup: model.Drop,
				},
				abstract.TransferTypeSnapshotOnly,
			)
			worker := helpers.Activate(t, transfer)
			defer worker.Close(t)
		})
	}
}
