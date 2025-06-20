//go:build !disable_yt_provider

package yt

import (
	"context"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/providers"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type tmpCleanerYt struct {
	client yt.Client
	dir    ypath.Path
	logger log.Logger
}

func NewTmpCleaner(dst YtDestinationModel, logger log.Logger) (providers.Cleaner, error) {
	client, err := ytclient.FromConnParams(dst, logger)
	if err != nil {
		return nil, xerrors.Errorf("error getting YT Client: %w", err)
	}

	dir := ypath.Path(dst.Path())
	return &tmpCleanerYt{client: client, dir: dir, logger: logger}, nil
}

func (c *tmpCleanerYt) Close() error {
	c.client.Stop()
	return nil
}

func (c *tmpCleanerYt) CleanupTmp(ctx context.Context, transferID string, tmpPolicy *model.TmpPolicyConfig) error {
	dirExists, err := c.client.NodeExists(ctx, c.dir, nil)
	if err != nil {
		return xerrors.Errorf("unable to check if target directory '%v' exists: %w", c.dir, err)
	}
	if !dirExists {
		c.logger.Infof("target directory '%v' does not exist", c.dir)
		return nil
	}

	var nodes []struct {
		Name string `yson:",value"`
	}
	err = c.client.ListNode(ctx, c.dir, &nodes, &yt.ListNodeOptions{})
	if err != nil {
		return xerrors.Errorf("unable to list nodes: %w", err)
	}

	suffix := tmpPolicy.BuildSuffix(transferID)
	for _, node := range nodes {
		if !strings.HasSuffix(node.Name, suffix) {
			continue
		}

		path := SafeChild(c.dir, node.Name)
		err = UnmountAndWaitRecursive(ctx, c.logger, c.client, path, nil)
		if err != nil {
			return xerrors.Errorf("unable to unmount node '%v': %w", path, err)
		}
		c.logger.Infof("successfully unmounted node: %v", path)

		options := &yt.RemoveNodeOptions{
			Recursive: true,
			Force:     true,
		}
		err = c.client.RemoveNode(ctx, path, options)
		if err != nil {
			return xerrors.Errorf("unable to remove node '%v': %w", path, err)
		}
		c.logger.Infof("successfully removed node: %v", path)
	}

	return nil
}
