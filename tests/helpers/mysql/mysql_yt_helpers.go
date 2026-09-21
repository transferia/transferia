package mysql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	provider_mysql "github.com/transferia/transferia/pkg/providers/mysql"
	provider_yt "github.com/transferia/transferia/pkg/providers/yt"
	yt_storage "github.com/transferia/transferia/pkg/providers/yt/storage"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"go.ytsaurus.tech/yt/go/yttest"
)

type MySQL2YTTestFixture struct {
	YTDir      ypath.Path
	YTEnv      *yttest.Env
	Src        *provider_mysql.MysqlSource
	Dst        provider_yt.YtDestinationModel
	SrcStorage *provider_mysql.Storage
	DstStorage *yt_storage.Storage

	cancelYtEnv func()
}

func SetupMySQL2YTTest(t *testing.T, src *provider_mysql.MysqlSource, dst provider_yt.YtDestinationModel) *MySQL2YTTestFixture {
	ytEnv, cancelYtEnv := yttest.NewEnv(t)
	_, err := ytEnv.YT.CreateNode(context.Background(), ypath.Path(dst.Path()), yt.NodeMap, &yt.CreateNodeOptions{Recursive: true})
	require.NoError(t, err)

	mysqlStorage, err := provider_mysql.NewStorage(src.ToStorageParams())
	require.NoError(t, err)
	ytStorage, err := yt_storage.NewStorage(dst.ToStorageParams())
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
	err := f.YTEnv.YT.RemoveNode(context.Background(), f.YTDir, &yt.RemoveNodeOptions{Recursive: true, Force: true})
	require.NoError(t, err)
	f.cancelYtEnv()
}
