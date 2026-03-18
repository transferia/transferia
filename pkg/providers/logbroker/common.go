package logbroker

import (
	"github.com/transferia/transferia/pkg/abstract/model"
	ydssource "github.com/transferia/transferia/pkg/providers/yds/source"
)

func ydsSourceConfig(allowTTLRewind, isLBSink bool, parseQueueParallelism int) *ydssource.YDSSource {
	return &ydssource.YDSSource{
		AllowTTLRewind:        allowTTLRewind,
		IsLbSink:              isLBSink,
		ParseQueueParallelism: parseQueueParallelism,

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
}
