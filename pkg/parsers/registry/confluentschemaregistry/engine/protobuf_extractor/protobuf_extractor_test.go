package protobuf_extractor

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDTEXTSUPPORT236(t *testing.T) {
	currSchema := `
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
	fullName, err := ExtractMessageFullNameByIndex(currSchema, []int{1})
	require.NoError(t, err)
	require.Equal(t, "data_quality.v1.DataQualityAnalysisResult", fullName)
}

// https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html
func TestOnConfluentExample(t *testing.T) {
	currSchema := `
		package test.package;

		message MessageA {
			message MessageB {
				message MessageC {
				}
			}
			message MessageD {
			}
			message MessageE {
				message MessageF {
				}
				message MessageG {
				}
			}
		}
		message MessageH {
			message MessageI {
			}
		}
`
	fullName, err := ExtractMessageFullNameByIndex(currSchema, []int{0, 2, 1})
	require.NoError(t, err)
	require.Equal(t, "test.package.MessageA.MessageE.MessageG", fullName)
}
