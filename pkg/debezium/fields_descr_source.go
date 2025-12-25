package debezium

import debeziumparameters "github.com/transferia/transferia/pkg/debezium/parameters"

func buildSourceSchemaDescr(sourceType string) map[string]interface{} {
	result := map[string]interface{}{
		"type":     "struct",
		"optional": false,
		"field":    "source",
	}
	fields := []map[string]interface{}{
		{
			"type":     "string",
			"optional": false,
			"field":    "version",
		}, {
			"type":     "string",
			"optional": false,
			"field":    "connector",
		}, {
			"type":     "string",
			"optional": false,
			"field":    "name",
		}, {
			"type":     "int64",
			"optional": false,
			"field":    "ts_ms",
		}, {
			"type":     "string",
			"optional": true,
			"name":     "io.debezium.data.Enum",
			"version":  1,
			"parameters": map[string]interface{}{
				"allowed": "true,last,false",
			},
			"default": "false",
			"field":   "snapshot",
		}, {
			"type":     "string",
			"optional": false,
			"field":    "db",
		}, {
			"type":     "string",
			"optional": false,
			"field":    "table",
		},
	}
	switch sourceType {
	case debeziumparameters.SourceTypePg:
		result["name"] = "io.debezium.connector.postgresql.Source"
		fields = append(fields, []map[string]interface{}{
			{
				"type":     "int64",
				"optional": true,
				"field":    "lsn",
			}, {
				"type":     "string",
				"optional": false,
				"field":    "schema",
			}, {
				"type":     "int64",
				"optional": true,
				"field":    "txId",
			}, {
				"type":     "int64",
				"optional": true,
				"field":    "xmin",
			},
		}...)
	case debeziumparameters.SourceTypeMysql:
		result["name"] = "io.debezium.connector.mysql.Source"
		fields[len(fields)-1]["optional"] = true
		fields = append(fields, []map[string]interface{}{
			{
				"type":     "string",
				"optional": false,
				"field":    "file",
			}, {
				"type":     "string",
				"optional": true,
				"field":    "gtid",
			}, {
				"type":     "int64",
				"optional": false,
				"field":    "pos",
			}, {
				"type":     "string",
				"optional": true,
				"field":    "query",
			}, {
				"type":     "int32",
				"optional": false,
				"field":    "row",
			}, {
				"type":     "int64",
				"optional": false,
				"field":    "server_id",
			}, {
				"type":     "int64",
				"optional": true,
				"field":    "thread",
			},
		}...)
	}
	result["fields"] = fields
	return result
}
