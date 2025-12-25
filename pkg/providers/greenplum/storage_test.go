package greenplum

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	gpfdistbin "github.com/transferia/transferia/pkg/providers/greenplum/gpfdist/gpfdist_bin"
)

func TestDisableCheckReplIdentity(t *testing.T) {
	t.Run("NewGpfdistStorage", func(t *testing.T) {
		gpfdistStorage := NewGpfdistStorage(nil, solomon.NewRegistry(nil), gpfdistbin.GpfdistParams{})
		require.True(t, gpfdistStorage.storage.postgresesCfg.DisableCheckReplIdentity)
		require.True(t, gpfdistStorage.storage.postgresesCfg.DisableViewsExtraction)
	})

	t.Run("NewStorage", func(t *testing.T) {
		storage := NewStorage(nil, solomon.NewRegistry(nil))
		cfg := pgStorageConfig{
			DisableCheckReplIdentity: false,
			DisableViewsExtraction:   false,
		}
		require.Equal(t, cfg, storage.postgresesCfg)

		cfg.DisableCheckReplIdentity = true
		storage.overridePostgresesCfg(cfg)
		require.Equal(t, cfg, storage.postgresesCfg)

		cfg.DisableViewsExtraction = true
		storage.overridePostgresesCfg(cfg)
		require.Equal(t, cfg, storage.postgresesCfg)

		cfg.DisableCheckReplIdentity = false
		storage.overridePostgresesCfg(cfg)
		require.Equal(t, cfg, storage.postgresesCfg)

		cfg.DisableViewsExtraction = false
		storage.overridePostgresesCfg(cfg)
		require.Equal(t, cfg, storage.postgresesCfg)
	})
}
