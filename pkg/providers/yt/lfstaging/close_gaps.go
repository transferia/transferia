//go:build !disable_yt_provider

package lfstaging

import (
	"time"

	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/xerrors"
)

func closeGaps(
	tx yt.Tx,
	config *sinkConfig,
	now time.Time,
) error {
	state, err := loadYtState(tx, config.tmpPath)
	if err != nil {
		return xerrors.Errorf("Cannot load state: %w", err)
	}

	// no need to do anything if lastTableTS is not initialized
	if state.LastTableTS == 0 {
		return nil
	}

	latestTableTS := state.LastTableTS
	currentTableTS := roundTimestampToNearest(now, config.aggregationPeriod).Unix()

	latestTableTS += int64(config.aggregationPeriod / time.Second)

	for latestTableTS < currentTableTS {
		newTableTime := time.Unix(latestTableTS, 0)
		w, err := newStagingWriter(tx, config, newTableTime)
		if err != nil {
			_ = tx.Abort()
			return xerrors.Errorf("Cannot create empty staging writer: %w", err)
		}

		err = w.CommitWithoutClosingGaps(tx)
		if err != nil {
			_ = tx.Abort()
			return xerrors.Errorf("Cannot commit empty staging writer: %w", err)
		}
		latestTableTS += int64(config.aggregationPeriod / time.Second)
	}

	return nil
}
