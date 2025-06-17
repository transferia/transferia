//go:build !disable_yt_provider

package storage

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func isDynamicTable(ytClient yt.Client, pathToTable ypath.Path) (bool, error) {
	pathToDynAttr := pathToTable.Attr("dynamic")
	var dynamic bool
	err := ytClient.GetNode(context.Background(), pathToDynAttr, &dynamic, nil)
	if err != nil {
		return false, xerrors.Errorf("unable to get node %s:%w", pathToDynAttr, err)
	}
	return dynamic, nil
}
