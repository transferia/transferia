package s3

import (
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/tests/helpers/delivery"
	mocksink "github.com/transferia/transferia/tests/helpers/mock_sink"
	transferhelpers "github.com/transferia/transferia/tests/helpers/transfer"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestS3SchemaAndPkeyCases(t *testing.T, src *s3_model.S3Source, columnName string, path string) {
	t.Run("__file_name, __row_index -- present & they are pkey", func(t *testing.T) {
		src.HideSystemCols = false
		src.OutputSchema = nil
		testS3SchemaAndPkeyCase(t, src)
	})

	t.Run("__file_name, __row_index -- present & they are pkey", func(t *testing.T) {
		src.HideSystemCols = false
		src.OutputSchema = []abstract.ColSchema{
			{
				ColumnName: columnName,
				Path:       path,
				DataType:   ytschema.TypeString.String(),
				PrimaryKey: false,
			},
		}
		testS3SchemaAndPkeyCase(t, src)
	})

	t.Run("__file_name, __row_index -- present & they are not pkey", func(t *testing.T) {
		src.HideSystemCols = false
		src.OutputSchema = []abstract.ColSchema{
			{
				ColumnName: columnName,
				Path:       path,
				DataType:   ytschema.TypeString.String(),
				PrimaryKey: true,
			},
		}
		testS3SchemaAndPkeyCase(t, src)
	})

	t.Run("__file_name, __row_index -- not present", func(t *testing.T) {
		src.HideSystemCols = true
		src.OutputSchema = nil
		testS3SchemaAndPkeyCase(t, src)
	})

	t.Run("__file_name, __row_index -- not present, userf-defined schema have pkeys", func(t *testing.T) {
		src.HideSystemCols = true
		src.OutputSchema = []abstract.ColSchema{
			{
				ColumnName: columnName,
				Path:       path,
				DataType:   ytschema.TypeString.String(),
				PrimaryKey: true,
			},
		}
		testS3SchemaAndPkeyCase(t, src)
	})

	t.Run("__file_name, __row_index -- not present, userf-defined schema don't have pkeys", func(t *testing.T) {
		src.HideSystemCols = true
		src.OutputSchema = []abstract.ColSchema{
			{
				ColumnName: columnName,
				Path:       path,
				DataType:   ytschema.TypeString.String(),
				PrimaryKey: false,
			},
		}
		testS3SchemaAndPkeyCase(t, src)
	})
}

func testS3SchemaAndPkeyCase(t *testing.T, src *s3_model.S3Source) {
	expectedIsSystemColsPresent := !src.HideSystemCols

	expectedIsSystemColsPkeys := !src.HideSystemCols
	if src.OutputSchema != nil && expectedIsSystemColsPresent {
		expectedIsSystemColsPkeys = !abstract.NewTableSchema(src.OutputSchema).Columns().HasPrimaryKey()
	}

	expectedKeys := expectedIsSystemColsPkeys
	for _, el := range src.OutputSchema {
		if el.PrimaryKey {
			expectedKeys = true
			break
		}
	}

	sink := mocksink.NewMockSink(nil)
	sink.PushCallback = func(input []abstract.ChangeItem) error {
		for _, el := range input {
			if el.IsRowEvent() {
				fmt.Println("ROW_EVENT", el.ToJSONString())

				// check 'isSystemColsPresent'
				isSystemColsPresent := slices.Contains(el.ColumnNames, s3_reader.FileNameSystemCol) && slices.Contains(el.ColumnNames, s3_reader.RowIndexSystemCol)
				require.Equal(t, expectedIsSystemColsPresent, isSystemColsPresent)

				// check 'isSystemKeysPkeys'
				isSystemKeysPkeys := slices.Compare(el.KeyCols(), []string{s3_reader.FileNameSystemCol, s3_reader.RowIndexSystemCol}) == 0
				require.Equal(t, expectedIsSystemColsPkeys, isSystemKeysPkeys)

				// check 'expectedKeys'
				require.Equal(t, expectedKeys, len(el.KeyCols()) != 0)

				// check if values of pkeys is not null
				for _, currColSchema := range el.TableSchema.Columns() {
					if currColSchema.PrimaryKey {
						currColumnValue := el.ColumnValues[el.ColumnNameIndex(currColSchema.ColumnName)]
						if currColumnValue == nil {
							t.Fail()
						}
					}
				}

				return abstract.NewFatalError(xerrors.New("to immediately exit"))
			}
		}
		return nil
	}
	dst := &model.MockDestination{
		SinkerFactory: func() abstract.Sinker { return sink },
		Cleanup:       model.DisabledCleanup,
	}

	transfer := transferhelpers.MakeTransfer("fake", src, dst, abstract.TransferTypeSnapshotOnly)
	_, err := delivery.ActivateErr(transfer)
	require.Error(t, err)
}
