//go:build !disable_yt_provider

package lfstaging

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/providers/yt/recipe"
	"go.ytsaurus.tech/yt/go/yttest"
)

func createEnvs(t *testing.T) (*yttest.Env, *sinkConfig, *intermediateWriter, func()) {
	env, cancel := recipe.NewEnv(t)

	config := defaultSinkConfig()
	config.secondsPerTmpTable = 1000
	config.bytesPerTmpTable = 100
	config.tmpPath = "//iw-test/tmp"

	iw, err := newIntermediateWriter(config, env.YT, env.L.Logger())
	require.NoError(t, err, "newIntermediateWriter throws")

	return env, config, iw, cancel
}

func TestIntermediateWriterWrittenBytes(t *testing.T) {
	_, _, iw, cancel := createEnvs(t)
	defer cancel()

	err := iw.Write([]abstract.ChangeItem{
		abstract.MakeRawMessage("", time.Now(), "", 0, 0, []byte("abacaba")),
	})
	require.NoError(t, err, "Write throws")

	require.Equal(t, int64(7), iw.writtenBytes)

	err = iw.Write([]abstract.ChangeItem{
		abstract.MakeRawMessage("", time.Now(), "", 0, 0, []byte("aboba123")),
	})
	require.NoError(t, err, "Write throws")

	require.Equal(t, int64(15), iw.writtenBytes)
}

func TestIntermediateWriterRotatesOnBytes(t *testing.T) {
	_, _, iw, cancel := createEnvs(t)
	defer cancel()

	for i := 0; i < 16; i++ {
		err := iw.Write([]abstract.ChangeItem{
			abstract.MakeRawMessage("", time.Now(), "", 0, 0, []byte("123456")),
		})
		require.NoError(t, err, "Write throws")
	}

	require.Equal(t, int64(96), iw.writtenBytes)

	err := iw.Write([]abstract.ChangeItem{
		abstract.MakeRawMessage("", time.Now(), "", 0, 0, []byte("123456")),
	})
	require.NoError(t, err, "Write throws")

	require.Equal(t, int64(102), iw.writtenBytes)

	// Enough time for rotator fiber to do its thing?
	time.Sleep(time.Second * 2)

	err = iw.Write([]abstract.ChangeItem{
		abstract.MakeRawMessage("", time.Now(), "", 0, 0, []byte("123456")),
	})
	require.NoError(t, err, "Write throws")

	require.Equal(t, int64(6), iw.writtenBytes)
}
