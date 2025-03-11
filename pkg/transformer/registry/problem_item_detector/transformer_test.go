package problemitemdetector

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferria/transferria/internal/logger"
	"github.com/transferria/transferria/pkg/abstract"
)

func TestTransformer(t *testing.T) {
	transformer := &problemItemDetector{Config{}, logger.Log}
	require.False(t, transformer.Suitable(abstract.TableID{}, nil))
	require.Equal(t, "problem item detector", transformer.Description())

	changeItems := []abstract.ChangeItem{{}, {Table: "table"}}
	expected := abstract.TransformerResult{
		Transformed: changeItems,
		Errors:      nil,
	}
	require.Equal(t, expected, transformer.Apply(changeItems))

	tableSchema := &abstract.TableSchema{}

	schema, err := transformer.ResultSchema(tableSchema)
	require.NoError(t, err)
	require.Equal(t, tableSchema, schema)
}
