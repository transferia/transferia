package protoparser

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/metrika/proto/cloud_export"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/protobuf/protoscanner"
)

func TestMetrikaHitProtoseq(t *testing.T) {
	msg := parsers.Message{
		Value: protoSampleContent(t, "metrika-data/metrika_hit_protoseq_data.bin"),
	}
	parser, err := NewProtoParser(metrikaHitProtoseqConfig(), getSourceStatsMock())
	require.NoError(t, err)
	actual := parser.Do(msg, abstract.NewPartition("", 0))
	if unparsed := parsers.ExtractUnparsed(actual); len(unparsed) > 0 {
		require.FailNow(t, "unexpected unparsed items", unparsed)
	}
	checkColsEqual(t, actual)
	require.Equal(t, 8639, len(actual))
	for _, item := range actual {
		require.Equal(t, 112, len(item.ColumnNames))
	}
}

func metrikaHitProtoseqConfig() *ProtoParserConfig {
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
		yslices.Map(primaryKeys, RequiredColumn),
		yslices.Map(optionalColumns, OptionalColumn)...,
	)
	msg := new(cloud_export.CloudTransferHit)
	return &ProtoParserConfig{
		IncludeColumns:     allColumns,
		PrimaryKeys:        primaryKeys,
		ScannerMessageDesc: msg.ProtoReflect().Descriptor(),
		ProtoMessageDesc:   msg.ProtoReflect().Descriptor(),
		ProtoScannerType:   protoscanner.ScannerTypeLineSplitter,
		LineSplitter:       abstract.LfLineSplitterProtoseq,
	}
}
