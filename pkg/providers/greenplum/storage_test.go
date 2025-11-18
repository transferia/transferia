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
		require.True(t, gpfdistStorage.storage.disableCheckReplIdentity)
	})

	t.Run("NewStorage", func(t *testing.T) {
		storage := NewStorage(nil, solomon.NewRegistry(nil))
		require.False(t, storage.disableCheckReplIdentity)
		storage.setDisableReplIdentityCheck(true)
		require.True(t, storage.disableCheckReplIdentity)
		storage.setDisableReplIdentityCheck(false)
		require.False(t, storage.disableCheckReplIdentity)
	})
}
