package helpers

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/providers/mysql"
	"github.com/transferia/transferia/pkg/providers/yt"
	"github.com/transferia/transferia/pkg/providers/yt/storage"
	"go.ytsaurus.tech/yt/go/ypath"
	ytMain "go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

type MySQL2YTTestFixture struct {
	YTDir      ypath.Path
	YTEnv      *yttest.Env
	Src        *mysql.MysqlSource
	Dst        yt.YtDestinationModel
	SrcStorage *mysql.Storage
	DstStorage *storage.Storage

	cancelYtEnv func()
}

func SetupMySQL2YTTest(t *testing.T, src *mysql.MysqlSource, dst yt.YtDestinationModel) *MySQL2YTTestFixture {
	ytEnv, cancelYtEnv := yttest.NewEnv(t)
	_, err := ytEnv.YT.CreateNode(context.Background(), ypath.Path(dst.Path()), ytMain.NodeMap, &ytMain.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	mysqlStorage, err := mysql.NewStorage(src.ToStorageParams())
	require.NoError(t, err)
	ytStorage, err := storage.NewStorage(dst.ToStorageParams())
	require.NoError(t, err)

	return &MySQL2YTTestFixture{
		YTDir:       ypath.Path(dst.Path()),
		YTEnv:       ytEnv,
		Src:         src,
		Dst:         dst,
		SrcStorage:  mysqlStorage,
		DstStorage:  ytStorage,
		cancelYtEnv: cancelYtEnv,
	}
}

func (f *MySQL2YTTestFixture) Teardown(t *testing.T) {
	err := f.YTEnv.YT.RemoveNode(context.Background(), f.YTDir, &ytMain.RemoveNodeOptions{Recursive: true, Force: true})
	require.NoError(t, err)
	f.cancelYtEnv()
}
