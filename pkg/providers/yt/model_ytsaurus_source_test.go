package yt

import (
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract/model"
)

// TestYTSaurusSourceIsAbstract2 covers the abstract1 migration routing: only
// copy destinations keep the legacy abstract2 flow (UploadV2); every other
// destination runs the v1 pipeline.
func TestYTSaurusSourceIsAbstract2(t *testing.T) {
	src := &YTSaurusSource{}
	require.True(t, src.IsAbstract2(&YtCopyDestination{}))
	require.False(t, src.IsAbstract2(&model.MockDestination{}))
}
