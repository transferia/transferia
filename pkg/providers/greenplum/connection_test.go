package greenplum

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

func TestConfigurePGStorageForGreenplum_OverrideConfig(t *testing.T) {
	gpStorage := NewStorage(&GpSource{}, solomon.NewRegistry(nil))
	gpStorage.overridePostgresesCfg(pgStorageConfig{
		DisableCheckReplIdentity: true,
		DisableViewsExtraction:   false,
	})
	pgStorage := &postgres.Storage{}
	gpStorage.configurePGStorageForGreenplum(pgStorage)
	require.True(t, pgStorage.DisableCheckReplIdentity)
	require.False(t, pgStorage.DisableViewsExtraction)

	gpStorage.overridePostgresesCfg(pgStorageConfig{
		DisableCheckReplIdentity: false,
		DisableViewsExtraction:   true,
	})
	gpStorage.configurePGStorageForGreenplum(pgStorage)
	require.False(t, pgStorage.DisableCheckReplIdentity)
	require.True(t, pgStorage.DisableViewsExtraction)
}
