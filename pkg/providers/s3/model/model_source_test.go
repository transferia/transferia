package s3_model

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestS3SourceValidateRejectsSystemColumns(t *testing.T) {
	for _, columnName := range []string{FileNameSystemCol, RowIndexSystemCol} {
		for _, hideSystemCols := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/hide=%t", columnName, hideSystemCols), func(t *testing.T) {
				source := new(S3Source)
				source.HideSystemCols = hideSystemCols
				source.OutputSchema = []abstract.ColSchema{
					abstract.NewColSchema("id", ytschema.TypeInt64, true),
					abstract.NewColSchema(columnName, ytschema.TypeString, false),
				}

				err := source.Validate()
				require.ErrorContains(t, err, columnName)
				require.ErrorContains(t, err, "reserved")
			})
		}
	}
}

func TestS3SourceValidateAcceptsUserColumns(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		schema []abstract.ColSchema
	}{
		{name: "empty", schema: nil},
		{
			name: "user columns",
			schema: []abstract.ColSchema{
				abstract.NewColSchema("id", ytschema.TypeInt64, true),
				abstract.NewColSchema("__file_names", ytschema.TypeString, false),
			},
		},
	} {
		for _, hideSystemCols := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/hide=%t", testCase.name, hideSystemCols), func(t *testing.T) {
				source := new(S3Source)
				source.HideSystemCols = hideSystemCols
				source.OutputSchema = testCase.schema

				require.NoError(t, source.Validate())
			})
		}
	}
}
