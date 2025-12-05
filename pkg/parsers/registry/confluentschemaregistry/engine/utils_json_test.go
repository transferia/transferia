package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
)

func TestProcessPayload(t *testing.T) {
	schemaVal := `
	{
		"title": "public.person",
			"type": "object",
			"properties": {
			"name": { "type": "string" },
			"id": { "type": "integer", "connect.type": "int32" },
			"email": { "type": "string" }
		},
		"required": ["name", "id"],
		"additionalProperties": false
	}`
	schema := &confluent.Schema{
		ID:         0,
		Schema:     schemaVal,
		SchemaType: confluent.JSON,
		References: nil,
	}
	buf := []byte(`{"id": 2, "name": "Bob"}`)

	result, _, err := makeChangeItemsFromMessageWithJSON(schema, buf, 0, time.Time{}, false)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, 3, len(result[0].ColumnNames))
	require.Equal(t, 3, len(result[0].ColumnValues))
}
