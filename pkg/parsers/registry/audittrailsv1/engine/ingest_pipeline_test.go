package engine

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/ohler55/ojg/jp"
	"github.com/ohler55/ojg/oj"
	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/test/canon"
)

func checkInputData(t *testing.T, inProgram ingestPipelineProgram, inData, jsonPath string) (interface{}, bool) {
	programResult, err := exec(inData, &inProgram)
	require.NoError(t, err)
	var jsonObj map[string]interface{}
	err = json.Unmarshal([]byte(programResult), &jsonObj)
	require.NoError(t, err)

	result, err := jsonpathGet(jsonPath, jsonObj)
	require.NoError(t, err)

	strObj1, err := json.Marshal(jsonObj)
	require.NoError(t, err)
	jsonPathDel(jsonObj, jsonPath)
	strObj2, err := json.Marshal(jsonObj)
	require.NoError(t, err)

	return result, string(strObj1) != string(strObj2)
}

func TestRenameAuthorizationAuthorized(t *testing.T) {
	programStr := `
	{
		"description": "Audit Trails Ingest Pipeline",
		"processors": [
			{
				"rename": {
					"field": "authorization.authorized",
					"target_field": "user.authorization",
					"ignore_failure": true
				}
			}
		]
	}
	`
	var currProgram ingestPipelineProgram
	err := json.Unmarshal([]byte(programStr), &currProgram)
	require.NoError(t, err)

	// test cases

	t.Run(`field 'authorization.authorized' exists, value is 'true'`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"authorization": {"authorized": true}}`,
			`user.authorization`,
		)
		require.Equal(t, true, result)
		require.Equal(t, true, exists)
	})

	t.Run(`field 'authorization.authorized' exists, value is 'false'`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"authorization": {"authorized": false}}`,
			`user.authorization`,
		)
		require.Equal(t, false, result)
		require.Equal(t, true, exists)
	})

	t.Run(`field 'authorization.authorized' is not exists`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"authorization": {"random_key": "random_val"}}`,
			`user.authorization`,
		)
		require.Equal(t, nil, result)
		require.Equal(t, false, exists)
	})
}

func TestSetIfConditionEventOutcome(t *testing.T) {
	programStr := `
	{
		"description": "Audit Trails Ingest Pipeline",
		"processors": [
			{
				"set": {
					"if": "ctx.event.status == 'DONE'",
					"field": "event.outcome",
					"value": "success",
					"ignore_failure": true
				}
			}
		]
	}
	`
	var currProgram ingestPipelineProgram
	err := json.Unmarshal([]byte(programStr), &currProgram)
	require.NoError(t, err)

	// test cases

	t.Run(`field 'event.status' not exists #0`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"random_key": "random_val"}`,
			`event.outcome`,
		)
		require.Equal(t, nil, result)
		require.Equal(t, false, exists)
	})

	t.Run(`field 'event.status' not exists #1`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"event": {"random_key": "random_val"}}`,
			`event.outcome`,
		)
		require.Equal(t, nil, result)
		require.Equal(t, false, exists)
	})

	t.Run(`field 'event.status' exists, contains value "DONE"`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"event": {"status": "DONE"}}`,
			`event.outcome`,
		)
		require.Equal(t, "success", result.(string))
		require.Equal(t, true, exists)
	})

	t.Run(`field 'event.status' exists, contains value not "DONE"`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"event": {"status": "ANYTHING"}}`,
			`event.outcome`,
		)
		require.Equal(t, nil, result)
		require.Equal(t, false, exists)
	})
}

func TestSetIfConditionSourceIP(t *testing.T) {
	programStr := `
	{
		"description": "Audit Trails Ingest Pipeline",
		"processors": [
			{
				"set": {
					"if": "ctx.request_metadata.remote_address != 'cloud.yandex'",
					"field": "source.ip",
					"value": "{{{request_metadata.remote_address}}}",
					"ignore_failure": true
				}
			}
		]
	}
	`
	var currProgram ingestPipelineProgram
	err := json.Unmarshal([]byte(programStr), &currProgram)
	require.NoError(t, err)

	// test cases

	t.Run(`field 'request_metadata.remote_address' not exists #0`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"random_key": "random_val"}`,
			`source.ip`,
		)
		require.Equal(t, nil, result)
		require.Equal(t, false, exists)
	})

	t.Run(`field 'request_metadata.remote_address' not exists #1`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"request_metadata": {"random_key": "random_val"}}`,
			`source.ip`,
		)
		require.Equal(t, nil, result)
		require.Equal(t, false, exists)
	})

	t.Run(`field 'request_metadata.remote_address' exists, contains value "cloud.yandex"`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"request_metadata": {"remote_address": "cloud.yandex"}}`,
			`source.ip`,
		)
		require.Equal(t, nil, result)
		require.Equal(t, false, exists)
	})

	t.Run(`field 'request_metadata.remote_address' exists, contains some random string, not ipv4/ipv6`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"request_metadata": {"remote_address": "blablabla"}}`,
			`source.ip`,
		)
		require.Equal(t, nil, result)
		require.Equal(t, false, exists)
	})

	t.Run(`field 'request_metadata.remote_address' exists, contains ipv4`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"request_metadata": {"remote_address": "12.34.56.78"}}`,
			`source.ip`,
		)
		require.Equal(t, "12.34.56.78", result.(string))
		require.Equal(t, true, exists)
	})

	t.Run(`field 'request_metadata.remote_address' exists, contains ipv6`, func(t *testing.T) {
		result, exists := checkInputData(t, currProgram,
			`{"request_metadata": {"remote_address": "fe80::7491:79ff:fef8:6a63"}}`,
			`source.ip`,
		)
		require.Equal(t, "fe80::7491:79ff:fef8:6a63", result.(string))
		require.Equal(t, true, exists)
	})
}

func TestKnownPaesslerAGJsonpathBug(t *testing.T) {
	t.Run("basic check", func(t *testing.T) {
		jsonObj := oj.MustParseString(`{"r": [{"a":1}]}`)
		x := jp.MustParseString(`r[1].a`)
		result := x.Get(jsonObj)
		require.Equal(t, 0, len(result))
	})

	t.Run("check on whole json", func(t *testing.T) {
		result, err := ExecProgram(rawLines[4])
		require.NoError(t, err)

		jsonObj := oj.MustParseString(result)
		x := jp.MustParseString(`cloud.folder.id`)
		result2 := x.Get(jsonObj)
		require.Equal(t, 0, len(result2))
	})
}

func TestJSONSet(t *testing.T) {
	t.Run("empty json", func(t *testing.T) {
		var myMap map[string]interface{}
		err := json.Unmarshal([]byte(`{}`), &myMap)
		require.NoError(t, err)
		jsonPathSet(myMap, "a.b.c", 777)
		bytes, err := json.Marshal(myMap)
		require.NoError(t, err)
		require.Equal(t, `{"a":{"b":{"c":777}}}`, string(bytes))
	})

	t.Run("not empty json", func(t *testing.T) {
		var myMap map[string]interface{}
		err := json.Unmarshal([]byte(`{"q":1}`), &myMap)
		require.NoError(t, err)
		jsonPathSet(myMap, "a.b.c", 777)
		bytes, err := json.Marshal(myMap)
		require.NoError(t, err)
		require.Equal(t, `{"a":{"b":{"c":777}},"q":1}`, string(bytes))
	})

	t.Run("append on some level", func(t *testing.T) {
		var myMap map[string]interface{}
		err := json.Unmarshal([]byte(`{"a":{"z":1}}`), &myMap)
		require.NoError(t, err)
		jsonPathSet(myMap, "a.b.c", 777)
		bytes, err := json.Marshal(myMap)
		require.NoError(t, err)
		require.Equal(t, `{"a":{"b":{"c":777},"z":1}}`, string(bytes))
	})

	t.Run("overwrite #0", func(t *testing.T) {
		var myMap map[string]interface{}
		err := json.Unmarshal([]byte(`{"a":{"b":{"c":666}}}`), &myMap)
		require.NoError(t, err)
		jsonPathSet(myMap, "a.b.c", 777)
		bytes, err := json.Marshal(myMap)
		require.NoError(t, err)
		require.Equal(t, `{"a":{"b":{"c":777}}}`, string(bytes))
	})

	t.Run("overwrite #1", func(t *testing.T) {
		var myMap map[string]interface{}
		err := json.Unmarshal([]byte(`{"a":{"b":"blablabla"}}`), &myMap)
		require.NoError(t, err)
		jsonPathSet(myMap, "a.b.c", 777)
		bytes, err := json.Marshal(myMap)
		require.NoError(t, err)
		require.Equal(t, `{"a":{"b":{"c":777}}}`, string(bytes))
	})
}

func TestRename(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		program := &ingestPipelineProgram{
			Description: "",
			Processors: []map[string]ingestPipelineCommandArgs{
				{"rename": ingestPipelineCommandArgs{
					Field:       "authentication.subject_name",
					TargetField: "user.name",
				}},
			},
		}
		result, err := exec(`{"authentication":{"subject_name":"mirtov8@yandex-team.ru"}}`, program)
		require.NoError(t, err)
		require.Equal(t, `{"authentication":{},"user":{"name":"mirtov8@yandex-team.ru"}}`, result)
	})

	t.Run("not exists #0", func(t *testing.T) {
		program := &ingestPipelineProgram{
			Description: "",
			Processors: []map[string]ingestPipelineCommandArgs{
				{"rename": ingestPipelineCommandArgs{
					Field:       "authentication.subject_name",
					TargetField: "user.name",
				}},
			},
		}
		result, err := exec(`{"authentication":{"k":"v"}}`, program)
		require.NoError(t, err)
		require.Equal(t, `{"authentication":{"k":"v"}}`, result)
	})

	t.Run("not exists #1", func(t *testing.T) {
		program := &ingestPipelineProgram{
			Description: "",
			Processors: []map[string]ingestPipelineCommandArgs{
				{"rename": ingestPipelineCommandArgs{
					Field:       "authentication.subject_name",
					TargetField: "user.name",
				}},
			},
		}
		result, err := exec(`{}`, program)
		require.NoError(t, err)
		require.Equal(t, `{}`, result)
	})
}

func TestURLDecode(t *testing.T) {
	t.Run("exists", func(t *testing.T) {
		program := &ingestPipelineProgram{
			Description: "",
			Processors: []map[string]ingestPipelineCommandArgs{
				{"urldecode": ingestPipelineCommandArgs{
					Field: "details.source_uri",
				}},
			},
		}
		result, err := exec(`{"details":{"source_uri":"mirtov8%40yandex-team.ru"}}`, program)
		require.NoError(t, err)
		require.Equal(t, `{"details":{"source_uri":"mirtov8@yandex-team.ru"}}`, result)
	})

	t.Run("not exists", func(t *testing.T) {
		program := &ingestPipelineProgram{
			Description: "",
			Processors: []map[string]ingestPipelineCommandArgs{
				{"urldecode": ingestPipelineCommandArgs{
					Field: "details.source_uri",
				}},
			},
		}
		result, err := exec(`{}`, program)
		require.NoError(t, err)
		require.Equal(t, `{}`, result)
	})
}

func TestSet(t *testing.T) {
	t.Run("exists (not uses mustache)", func(t *testing.T) {
		program := &ingestPipelineProgram{
			Description: "",
			Processors: []map[string]ingestPipelineCommandArgs{
				{"set": ingestPipelineCommandArgs{
					Field: "event.kind",
					Value: "event",
				}},
			},
		}
		result, err := exec(`{"event":{"kind":123}}`, program)
		require.NoError(t, err)
		require.Equal(t, `{"event":{"kind":"event"}}`, result)
	})

	t.Run("not exists (not uses mustache)", func(t *testing.T) {
		program := &ingestPipelineProgram{
			Description: "",
			Processors: []map[string]ingestPipelineCommandArgs{
				{"set": ingestPipelineCommandArgs{
					Field: "event.kind",
					Value: "event",
				}},
			},
		}
		result, err := exec(`{}`, program)
		require.NoError(t, err)
		require.Equal(t, `{"event":{"kind":"event"}}`, result)
	})

	t.Run("exists (use mustache)", func(t *testing.T) {
		program := &ingestPipelineProgram{
			Description: "",
			Processors: []map[string]ingestPipelineCommandArgs{
				{"set": ingestPipelineCommandArgs{
					Field: "cloud.org.name",
					Value: "{{{resource_metadata.path.0.resource_name}}}",
				}},
			},
		}
		result, err := exec(`{"resource_metadata":{"path":[{"resource_name":"my_val"}]}}`, program)
		require.NoError(t, err)
		require.Equal(t, `{"cloud":{"org":{"name":"my_val"}},"resource_metadata":{"path":[{"resource_name":"my_val"}]}}`, result)
	})

	t.Run("not exists (use mustache)", func(t *testing.T) {
		program := &ingestPipelineProgram{
			Description: "",
			Processors: []map[string]ingestPipelineCommandArgs{
				{"set": ingestPipelineCommandArgs{
					Field: "cloud.org.name",
					Value: "{{{resource_metadata.path.0.resource_name}}}",
				}},
			},
		}
		result, err := exec(`{}`, program)
		require.NoError(t, err)
		require.Equal(t, `{}`, result)
	})
}

func TestConditionIsTrue(t *testing.T) {
	t.Run("ctx.event.status == 'DONE'", func(t *testing.T) {
		var myMap map[string]interface{}
		err := json.Unmarshal([]byte(`{"event":{"status":"DONE"}}`), &myMap)
		require.NoError(t, err)
		args := ingestPipelineCommandArgs{
			If: "ctx.event.status == 'DONE'",
		}
		cond, err := conditionIsTrue(args, myMap)
		require.NoError(t, err)
		require.True(t, cond)
	})

	t.Run("ctx.event.status != 'DONE'", func(t *testing.T) {
		var myMap map[string]interface{}
		err := json.Unmarshal([]byte(`{"event":{"status":"NOT_DONE"}}`), &myMap)
		require.NoError(t, err)
		args := ingestPipelineCommandArgs{
			If: "ctx.event.status == 'DONE'",
		}
		cond, err := conditionIsTrue(args, myMap)
		require.NoError(t, err)
		require.False(t, cond)
	})

	t.Run("ctx.request_metadata.remote_address != 'cloud.yandex'", func(t *testing.T) {
		var myMap map[string]interface{}
		err := json.Unmarshal([]byte(`{"request_metadata":{"remote_address":"not.cloud.yandex"}}`), &myMap)
		require.NoError(t, err)
		args := ingestPipelineCommandArgs{
			If: "ctx.request_metadata.remote_address != 'cloud.yandex'",
		}
		cond, err := conditionIsTrue(args, myMap)
		require.NoError(t, err)
		require.False(t, cond) // bcs now there is extra condition - value must be valid ipv4/ipv6 address
	})

	t.Run("ctx.request_metadata.remote_address == 'cloud.yandex'", func(t *testing.T) {
		var myMap map[string]interface{}
		err := json.Unmarshal([]byte(`{"request_metadata":{"remote_address":"cloud.yandex"}}`), &myMap)
		require.NoError(t, err)
		args := ingestPipelineCommandArgs{
			If: "ctx.request_metadata.remote_address != 'cloud.yandex'",
		}
		cond, err := conditionIsTrue(args, myMap)
		require.NoError(t, err)
		require.False(t, cond)
	})
}

func TestCanonWholeProgram0(t *testing.T) {
	result, err := ExecProgram(rawLines[0])
	require.NoError(t, err)
	fmt.Println(result)
	canon.SaveJSON(t, result)
}

func TestCanonWholeProgram1(t *testing.T) {
	result, err := ExecProgram(rawLines[1])
	require.NoError(t, err)
	fmt.Println(result)
	canon.SaveJSON(t, result)
}
