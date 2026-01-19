package engine

import (
	"encoding/hex"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers/registry/confluentschemaregistry/table_name_policy"
	"github.com/transferia/transferia/pkg/schemaregistry/confluent"
	confluentsrmock "github.com/transferia/transferia/tests/helpers/confluent_schema_registry_mock"
)

func testRun(t *testing.T, schema string, messageAsciiHex string) {

	messageBytes, err := hex.DecodeString(messageAsciiHex)
	require.NoError(t, err)

	//---

	type schemaResponseT struct {
		Version    int    `json:"version"`
		Schema     string `json:"schema"`
		SchemaType string `json:"schemaType"`
		ID         int    `json:"id"`
	}
	schemaResponse := schemaResponseT{
		Version:    1,
		Schema:     schema,
		SchemaType: string(confluent.PROTOBUF),
		ID:         1,
	}
	schemaResponseArr, err := json.Marshal(schemaResponse)
	require.NoError(t, err)

	//---

	schemaRegistryMock := confluentsrmock.NewConfluentSRMock(nil, nil)
	defer schemaRegistryMock.Close()

	schemaRegistryMock.AddSchema(t, "my_file2.proto", 1, 1, string(schemaResponseArr))

	parser := NewConfluentSchemaRegistryImpl(
		schemaRegistryMock.URL(),
		"",
		"uname",
		"pass",
		false,
		table_name_policy.TableNamePolicy{
			Derived: table_name_policy.TableNamePolicyDerived{
				ProtobufTableNamePolicy: table_name_policy.ProtobufTableNamePolicyMessageName,
			},
			Manual: table_name_policy.TableNamePolicyManual{
				TableName: "",
			},
		},
		false,
		logger.Log,
	)

	leastBuf, changeItems := parser.DoOne(
		abstract.Partition{},
		messageBytes,
		0,
		time.Time{},
	)

	fmt.Println(changeItems)
	fmt.Println(leastBuf)
	require.Equal(t, "DataQualityAnalysisResult", changeItems[0].Table)
}

func TestMessageIndexes(t *testing.T) {
	t.Run("", func(t *testing.T) {
		schema := `
syntax = "proto3";
package data_quality.v1;

import "google/protobuf/timestamp.proto";

message DataQualityConstraint {
 string name = 1;
 string status = 2;
 string message = 3;
}
message DataQualityAnalysisResult {
 string dq_id = 1;
 string job_id = 2;
 string table = 3;
 string query = 4;
 string s3_path = 5;
 google.protobuf.Timestamp start_time = 6;
 google.protobuf.Timestamp end_time = 7;
 bool passed = 8;
 repeated DataQualityConstraint constraints = 9;
}
`
		messageAsciiHex := `000000000102020A2430313962626233642D366564312D373038362D626338622D66313263346631643565623612022D311A1F6464735F6D646D2E6C6963656E73655F61637469766974795F747970655F74320B08CD9E9ECB0610B8F8C94C3A0B08CD9E9ECB0610B8F8C94C40014A2C0A2153697A65436F6E73747261696E742853697A6528536F6D652831203D20312929291207537563636573734A620A57556E697175656E657373436F6E73747261696E7428556E697175656E6573732853747265616D286C6963656E73655F61637469766974795F747970655F7569642C203F292C536F6D652831203D2031292C4E6F6E6529291207537563636573734A5B0A50436F6D706C6574656E657373436F6E73747261696E7428436F6D706C6574656E657373286C6963656E73655F61637469766974795F747970655F7569642C536F6D652831203D2031292C4E6F6E6529291207537563636573734A5C0A51436F6D706C6574656E657373436F6E73747261696E7428436F6D706C6574656E657373286C6963656E73655F61637469766974795F747970655F6E616D652C536F6D652831203D2031292C4E6F6E6529291207537563636573734AA7010A9B01436F6D706C69616E6365436F6E73747261696E7428436F6D706C69616E636528636865636B5F7472675F6368616E6765645F64746D2C7472675F6368616E6765645F64746D203C3D2066726F6D5F7574635F74696D657374616D702863757272656E745F74696D657374616D7028292C20274575726F70652F4D6F73636F7727292C536F6D652831203D2031292C4C69737428292C4E6F6E652929120753756363657373`
		testRun(t, schema, messageAsciiHex)
	})

	t.Run("", func(t *testing.T) {
		schema := `
syntax = "proto3";
package data_quality.v1;

import "google/protobuf/timestamp.proto";

message A {
  string name = 1;
}
message B {
  string name = 1;
}
message C {
    message DataQualityConstraint {
        string name = 1;
        string status = 2;
        string message = 3;
    }
    message DataQualityAnalysisResult {
        string dq_id = 1;
        string job_id = 2;
        string table = 3;
        string query = 4;
        string s3_path = 5;
        google.protobuf.Timestamp start_time = 6;
        google.protobuf.Timestamp end_time = 7;
        bool passed = 8;
        repeated DataQualityConstraint constraints = 9;
    }

    repeated DataQualityAnalysisResult result = 1;
}
`
		messageAsciiHex2 := `00000000010404020a0664712d31323312076a6f622d3435361a0864622e7461626c65220873656c65637420312a1073333a2f2f6275636b65742f70617468320608aac0b8cb063a0608adc0b8cb0640014a180a086e6f745f6e756c6c12024f4b1a08616c6c20676f6f644a150a09726f775f636f756e7412024f4b1a043e3d2031`
		testRun(t, schema, messageAsciiHex2)
	})
}
