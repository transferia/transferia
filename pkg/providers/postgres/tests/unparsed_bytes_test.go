package tests

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	genericparser "github.com/transferia/transferia/pkg/parsers/generic"
	"github.com/transferia/transferia/pkg/providers/postgres"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

func TestUnparsedBytesRepresentedAsHexLiteral(t *testing.T) {
	ci := genericparser.NewUnparsed(
		abstract.Partition{Topic: "test-topic"},
		"test-topic",
		[]byte{0xff, 0xfe, 0x88}, // not valid UTF-8
		"reason",
		0,
		0,
		time.Now(),
	)

	schema := ci.TableSchema.Columns()[4] // unparsed_row
	require.Equal(t, ytschema.TypeBytes.String(), schema.DataType)

	value := ci.ColumnValues[4]
	got, err := postgres.Represent(value, schema)
	require.NoError(t, err)
	require.Equal(t, `'\xfffe88'`, got)
}
