package helpers

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestReduceMeteringData(t *testing.T) {
	buildMeteringMsg := func(rowSize string, usageQuantity int) MeteringMsg {
		return MeteringMsg{
			CloudID:    "my_cloud_id",
			FolderID:   "my_folder_id",
			ResourceID: "dtt",
			Schema:     "datatransfer.data.output.v1",
			Tags: map[string]interface{}{
				"dst_type":      "yt",
				"row_size":      rowSize,
				"runtime":       "serverless",
				"src_type":      "pg",
				"transfer_type": "SNAPSHOT_AND_INCREMENT",
			},
			Labels: map[string]interface{}{},
			Usage: Usage{
				Quantity: usageQuantity,
				Type:     "delta",
				Unit:     "rows",
			},
			Version: "v1alpha1",
		}
	}

	msgs := []MeteringMsg{
		buildMeteringMsg("small", 14),
		buildMeteringMsg("large", 0),
		buildMeteringMsg("small", 5),
		buildMeteringMsg("large", 0),
	}

	result := reduceMeteringData(msgs)
	require.Equal(
		t,
		[]MeteringMsg{
			buildMeteringMsg("large", 0),
			buildMeteringMsg("small", 19),
		},
		result,
	)
}
