package proto

import (
	"bytes"
	"context"
	_ "embed"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics/mock"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/metrika/proto/cloud_export"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoparser"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoscanner"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	abstract_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
)

//go:embed gotest/metrika-data/metrika_hit_protoseq_data.bin
var metrikaData []byte

func TestStreamParseFile(t *testing.T) {
	oneItemSize := 2183 // size of one item in metrika_hit_protoseq_data.bin
	oneItem := metrikaData[:oneItemSize]
	expectedItems := 100
	data := make([]byte, 0, expectedItems*oneItemSize)
	for i := 0; i < expectedItems; i++ {
		data = append(data, oneItem...)
	}

	parserBuilder, err := protoparser.NewLazyProtoParserBuilder(MetrikaHitProtoseqConfig(), stats.NewSourceStats(mock.NewRegistry(mock.NewRegistryOpts())))
	require.NoError(t, err)
	genericParserReader := ProtoReader{
		blockSize:     100,
		parserBuilder: parserBuilder,
		logger:        logger.Log,
	}

	var pushedItems []abstract.ChangeItem
	mockPusher := func(items []abstract.ChangeItem) error {
		for _, item := range items {
			if parsers.IsUnparsed(item) {
				logger.Log.Infof("found unparsed item: %v", item)
			}
		}
		pushedItems = append(pushedItems, items...)
		return nil
	}

	rawReader := s3raw.NewFakeS3RawReader(int64(len(data)))
	reader := bytes.NewReader(data)
	rawReader.ReadF = func(p []byte) (int, error) {
		return reader.Read(p)
	}

	parser := genericParserReader.parserBuilder.BuildBaseParser()
	itemsParsedByDo := parser.Do(constructMessage(time.Now(), data, nil), abstract.NewPartition("metrika-data/metrika_hit_protoseq_data.bin", 0))
	require.Equal(t, expectedItems, len(itemsParsedByDo))
	require.True(t, allParsed(itemsParsedByDo))

	filePath := "metrika-data/metrika_hit_protoseq_data.bin"
	chunkReader := abstract_reader.NewChunkReader(rawReader, 2184, logger.Log)
	defer chunkReader.Close()
	err = streamParseFile(context.Background(), &genericParserReader, filePath, chunkReader, pusher.NewSyncPusher(mockPusher), time.Now())
	require.NoError(t, err)
	require.Empty(t, chunkReader.Data())
	require.True(t, allParsed(pushedItems))
	require.Equal(t, expectedItems, len(pushedItems))
}

func allParsed(items []abstract.ChangeItem) bool {
	for _, item := range items {
		if parsers.IsUnparsed(item) {
			return false
		}
	}
	return true
}

func MetrikaHitProtoseqConfig() *protoparser.ProtoParserConfig {
	requiredHitsV2Columns := []string{
		"CounterID", "EventDate", "CounterUserIDHash", "UTCEventTime", "WatchID", "Sign", "HitVersion",
	}
	optionalHitsV2Columns := []string{
		"AdvEngineID", "AdvEngineStrID", "BrowserCountry", "BrowserEngineID", "BrowserEngineStrID",
		"BrowserEngineVersion1", "BrowserEngineVersion2", "BrowserEngineVersion3", "BrowserEngineVersion4",
		"BrowserLanguage", "CLID", "ClientIP", "ClientIP6", "ClientTimeZone", "CookieEnable", "DevicePixelRatio",
		"DirectCLID", "Ecommerce", "FirstPartyCookie", "FromTag", "GCLID", "GoalsReached", "HasGCLID", "HTTPError",
		"IsArtifical", "IsDownload", "IsIFrame", "IsLink", "IsMobile", "IsNotBounce", "IsPageView", "IsParameter",
		"IsTablet", "IsTV", "JavascriptEnable", "MessengerID", "MessengerStrID", "MobilePhoneModel",
		"MobilePhoneVendor", "MobilePhoneVendorStr", "NetworkType", "NetworkTypeStr", "OpenstatAdID",
		"OpenstatCampaignID", "OpenstatServiceName", "OpenstatSourceID", "OriginalURL", "OS", "OSFamily", "OSName",
		"OSRoot", "OSRootStr", "OSStr", "PageCharset", "PageViewID", "Params", "ParsedParams.Key1",
		"ParsedParams.Key10", "ParsedParams.Key2", "ParsedParams.Key3", "ParsedParams.Key4", "ParsedParams.Key5",
		"ParsedParams.Key6", "ParsedParams.Key7", "ParsedParams.Key8", "ParsedParams.Key9", "ParsedParams.Quantity",
		"QRCodeProviderID", "QRCodeProviderStrID", "RecommendationSystemID", "RecommendationSystemStrID", "Referer",
		"RegionID", "ResolutionDepth", "ResolutionHeight", "ResolutionWidth", "SearchEngineID", "SearchEngineRootID",
		"SearchEngineRootStrID", "SearchEngineStrID", "ShareService", "ShareTitle", "ShareURL", "SocialSourceNetworkID",
		"SocialSourceNetworkStrID", "SocialSourcePage", "ThirdPartyCookieEnable", "Title", "TrafficSourceID",
		"TrafficSourceStrID", "URL", "UserAgent", "UserAgentMajor", "UserAgentStr", "UserAgentVersion2",
		"UserAgentVersion3", "UserAgentVersion4", "UTMCampaign", "UTMContent", "UTMMedium", "UTMSource", "UTMTerm",
		"WindowClientHeight", "WindowClientWidth", "YQRID",
	}

	metrikaNameToProto := func(name string) string { return strings.ReplaceAll(name, ".", "_") }
	primaryKeys := yslices.Map(requiredHitsV2Columns, metrikaNameToProto)
	optionalColumns := yslices.Map(optionalHitsV2Columns, metrikaNameToProto)

	allColumns := append(
		yslices.Map(primaryKeys, protoparser.RequiredColumn),
		yslices.Map(optionalColumns, protoparser.OptionalColumn)...,
	)
	msg := new(cloud_export.CloudTransferHit)
	return &protoparser.ProtoParserConfig{
		IncludeColumns:     allColumns,
		PrimaryKeys:        primaryKeys,
		ScannerMessageDesc: msg.ProtoReflect().Descriptor(),
		ProtoMessageDesc:   msg.ProtoReflect().Descriptor(),
		ProtoScannerType:   protoscanner.ScannerTypeLineSplitter,
		LineSplitter:       abstract.LfLineSplitterProtoseq,
	}
}
