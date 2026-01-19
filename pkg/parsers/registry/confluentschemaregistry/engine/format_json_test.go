package engine

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry/table_name_policy"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
)

func jsonApply(t *testing.T, tableNamePolicy table_name_policy.TableNamePolicy, expectedNamespace string, expectedTableName string) {
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

	result, _, err := makeChangeItemsFromMessageWithJSON(schema, buf, 0, time.Time{}, false, tableNamePolicy)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
	require.Equal(t, expectedNamespace, result[0].Schema)
	require.Equal(t, expectedTableName, result[0].Table)
}

func TestJSONTableNamePolicy(t *testing.T) {
	jsonApply(t, table_name_policy.TableNamePolicy{
		Derived: table_name_policy.TableNamePolicyDerived{
			JSONTableNamePolicy: table_name_policy.JSONTableNamePolicyDebeziumStyle,
		},
		Manual: table_name_policy.TableNamePolicyManual{
			TableName: "",
		},
	}, "public", "person")

	jsonApply(t, table_name_policy.TableNamePolicy{
		Derived: table_name_policy.TableNamePolicyDerived{
			JSONTableNamePolicy: table_name_policy.JSONTableNamePolicyTitle,
		},
		Manual: table_name_policy.TableNamePolicyManual{
			TableName: "",
		},
	}, "", "public.person")

	jsonApply(t, table_name_policy.TableNamePolicy{
		Derived: table_name_policy.TableNamePolicyDerived{},
		Manual: table_name_policy.TableNamePolicyManual{
			TableName: "blablabla",
		},
	}, "", "blablabla")
}
