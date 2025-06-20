package protoparser

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoscanner"
)

func TestMetrikaHitLog(t *testing.T) {
	testCases := getMetrikaHitsTestCases(t)
	pMsg := parsers.Message{
		Offset: 1,
		SeqNo:  1,
		Value:  protoSampleContent(t, "metrika-data/metrika_hit_log_data.bin"),
	}

	for i, test := range testCases {
		t.Run(fmt.Sprintf("case_%d", i), func(t *testing.T) {
			par, err := NewProtoParser(&test.conf, getSourceStatsMock())
			require.NoError(t, err)

			actual := par.Do(pMsg, abstract.NewPartition("", 0))

			unparsed := parsers.ExtractUnparsed(actual)
			if len(unparsed) > 0 {
				require.FailNow(t, "unexpected unparsed items", unparsed)
			}

			checkColsEqual(t, actual)

			for _, item := range actual {
				require.Equal(t, test.colsLen, len(item.ColumnNames))
			}
		})
	}
}

func getMetrikaHitsTestCases(t *testing.T) []struct {
	conf    ProtoParserConfig
	colsLen int
} {

	res := []struct {
		conf    ProtoParserConfig
		colsLen int
	}{
		{
			conf:    ProtoParserConfig{},
			colsLen: 92,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"TransferID",
					"WatchID",
				},
			},
			colsLen: 2,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"WatchID",
				},
			},
			colsLen: 2,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
					{
						Name:     "WatchID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"WatchID",
				},
			},
			colsLen: 2,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
					{
						Name:     "WatchID",
						Required: true,
					},
				},
			},
			colsLen: 2,
		},
		{
			conf: ProtoParserConfig{
				AddSyntheticKeys: true,
			},
			colsLen: 95,
		},
		{
			conf: ProtoParserConfig{
				AddSyntheticKeys: true,
				AddSystemColumns: true,
			},
			colsLen: 97,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				AddSyntheticKeys: true,
			},
			colsLen: 4,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"TransferID",
				},
				AddSyntheticKeys: true,
			},
			colsLen: 4,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"WatchID",
				},
				AddSyntheticKeys: true,
			},
			colsLen: 5,
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{
					"WatchID",
				},
				AddSyntheticKeys: true,
			},
			colsLen: 95,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				AddSyntheticKeys: true,
				AddSystemColumns: true,
			},
			colsLen: 97,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"TransferID",
				},
				AddSyntheticKeys: true,
				AddSystemColumns: true,
			},
			colsLen: 97,
		},
		{
			conf: ProtoParserConfig{
				IncludeColumns: []ColParams{
					{
						Name:     "TransferID",
						Required: true,
					},
				},
				PrimaryKeys: []string{
					"TransferID",
					"WatchID",
				},
				AddSyntheticKeys: true,
				AddSystemColumns: true,
			},
			colsLen: 97,
		},
		{
			conf: ProtoParserConfig{
				PrimaryKeys: []string{
					"TransferID",
				},
				AddSyntheticKeys: true,
				AddSystemColumns: true,
			},
			colsLen: 97,
		},
	}

	rootMsgDesc, err := extractMessageDesc(
		protoSampleContent(t, "metrika-data/metrika_hit_log.desc"),
		"CloudTransferHitList",
	)
	require.NoError(t, err)

	embeddedMsgDesc, ok := extractEmbeddedRepeatedMsgDesc(rootMsgDesc)
	require.True(t, ok)

	for i := range res {
		res[i].conf.ScannerMessageDesc = rootMsgDesc
		res[i].conf.ProtoMessageDesc = embeddedMsgDesc
		res[i].conf.ProtoScannerType = protoscanner.ScannerTypeRepeated
		res[i].conf.LineSplitter = abstract.LfLineSplitterDoNotSplit
	}

	return res
}
