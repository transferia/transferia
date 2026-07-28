package logbroker

import (
	core_metrics "github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/native"
	topiccommon "github.com/transferia/transferia/pkg/providers/ydb/topics/common"
	topicsource "github.com/transferia/transferia/pkg/providers/ydb/topics/source"
	pqv1source "github.com/transferia/transferia/pkg/providers/ydb/topics/source/pqv1"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

func newNativeSource(cfg *LbSource, logger log.Logger, registry core_metrics.Registry) (abstract.Source, error) {
	parser, err := parsers.NewParserFromParserConfig(&native.ParserConfigNativeLb{}, false, logger, stats.NewSourceStats(registry))
	if err != nil {
		return nil, xerrors.Errorf("unable to make native parser, err: %w", err)
	}

	topicSourceCfg := &topicsource.Config{
		Connection: topiccommon.ConnectionConfig{
			Endpoint:         topiccommon.FormatEndpoint(cfg.Instance, cfg.Port),
			Database:         cfg.Database,
			Credentials:      cfg.Credentials,
			TLSEnabled:       cfg.TLS == EnabledTLS,
			RootCAFiles:      cfg.RootCAFiles,
			TLSCACertificate: "",
		},

		Topics:   []string{cfg.Topic},
		Consumer: cfg.Consumer,
		ReaderOpts: topicsource.ReaderOptions{
			ReadOnlyLocal:       false,
			MaxMemory:           100 * 1024 * 1024, // 100 mb max memory usage
			MaxReadSize:         1 * 1024 * 1024,
			MaxReadMessageCount: 0,
			MaxTimeLag:          0,
			MinReadInterval:     0,
		},
		Transformer: nil,

		IsYDBTopicSink:             cfg.IsLbSink,
		AllowTTLRewind:             cfg.AllowTTLRewind,
		ParseQueueParallelism:      10,
		UseFullTopicNameForParsing: false,
	}

	return pqv1source.NewSource(topicSourceCfg, parser, logger, stats.NewSourceStats(registry))
}
