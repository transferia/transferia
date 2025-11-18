package greenplum

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/library/go/core/metrics/solomon"
	"github.com/transferia/transferia/pkg/providers/postgres"
)

func TestConfigurePGStorageForGreenplum_DisableCheckReplIdentity(t *testing.T) {
	gpStorage := NewStorage(&GpSource{}, solomon.NewRegistry(nil))
	gpStorage.setDisableReplIdentityCheck(true)
	pgStorage := &postgres.Storage{}
	gpStorage.configurePGStorageForGreenplum(pgStorage)
	require.True(t, pgStorage.DisableCheckReplIdentity)

	gpStorage.disableCheckReplIdentity = false
	gpStorage.configurePGStorageForGreenplum(pgStorage)
	require.False(t, pgStorage.DisableCheckReplIdentity)
}
