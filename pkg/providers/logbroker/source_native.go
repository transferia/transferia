package logbroker

import (
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue"
	"github.com/transferia/transferia/kikimr/public/sdk/go/persqueue/log/corelogadapter"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/parsers"
	"github.com/transferia/transferia/pkg/parsers/registry/native"
	ydssource "github.com/transferia/transferia/pkg/providers/yds/source"
	"github.com/transferia/transferia/pkg/stats"
	"github.com/transferia/transferia/pkg/xtls"
	"go.ytsaurus.tech/library/go/core/log"
)

func NewNativeSource(cfg *LbSource, logger log.Logger, registry metrics.Registry) (abstract.Source, error) {
	var opts persqueue.ReaderOptions
	opts.Logger = corelogadapter.New(logger)
	opts.Endpoint = cfg.Instance
	opts.Database = cfg.Database
	opts.ManualPartitionAssignment = true
	opts.Consumer = cfg.Consumer
	opts.Topics = []persqueue.TopicInfo{{Topic: cfg.Topic}}
	opts.MaxReadSize = 1 * 1024 * 1024
	opts.MaxMemory = 100 * 1024 * 1024 // 100 mb max memory usage
	opts.RetryOnFailure = true
	opts.Port = cfg.Port
	opts.Credentials = cfg.Credentials

	if cfg.TLS == EnabledTLS {
		tls, err := xtls.FromPath(cfg.RootCAFiles)
		if err != nil {
			return nil, xerrors.Errorf("failed to get TLS config for cloud: %w", err)
		}
		opts.TLSConfig = tls
	}

	return newPqv1NativeSource(cfg, logger, registry, opts)
}

func newPqv1NativeSource(
	cfg *LbSource,
	logger log.Logger,
	registry metrics.Registry,
	readerOpts persqueue.ReaderOptions,
) (abstract.Source, error) {
	ydsCfg := &ydssource.YDSSource{
		AllowTTLRewind:        cfg.AllowTTLRewind,
		IsLbSink:              cfg.IsLbSink,
		ParseQueueParallelism: 10,

		// These fields are either irrelevant for lb source or already specified in readerOpts and parser
		Endpoint:         "",
		Database:         "",
		Stream:           "",
		Consumer:         "",
		S3BackupBucket:   "",
		Port:             0,
		BackupMode:       model.S3BackupModeNoBackup,
		Transformer:      nil,
		SubNetworkID:     "",
		SecurityGroupIDs: nil,
		SupportedCodecs:  nil,
		TLSEnalbed:       false,
		RootCAFiles:      nil,
		ParserConfig:     nil,
		Underlay:         false,
		Credentials:      nil,
		ServiceAccountID: "",
		SAKeyContent:     "",
		TokenServiceURL:  "",
		Token:            "",
		UserdataAuth:     false,
	}

	parser, err := parsers.NewParserFromParserConfig(&native.ParserConfigNativeLb{}, false, logger, stats.NewSourceStats(registry))
	if err != nil {
		return nil, xerrors.Errorf("unable to make native parser, err: %w", err)
	}

	// transferID is empty because it is used to specify the consumer, and it is already specified in the readerOpts
	transferID := ""
	return ydssource.NewSourceWithOpts(transferID, ydsCfg, logger, registry,
		ydssource.WithCreds(cfg.Credentials),
		ydssource.WithReaderOpts(&readerOpts),
		ydssource.WithParser(parser),
	)
}
