//go:build !disable_yt_provider

package lfstaging

import (
	"time"

	"go.ytsaurus.tech/yt/go/ypath"
)

func defaultSinkConfig() *sinkConfig {
	return &sinkConfig{
		cluster:            "primary",
		topic:              "some-topic",
		tmpPath:            ypath.Path("//test/tmp"),
		stagingPath:        ypath.Path("//test/staging-area"),
		jobIndex:           0,
		ytAccount:          "default",
		ytPool:             "default",
		aggregationPeriod:  time.Second * 10,
		useNewMetadataFlow: false,
	}
}
