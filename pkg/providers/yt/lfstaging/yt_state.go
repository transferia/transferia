//go:build !disable_yt_provider

package lfstaging

import (
	"context"

	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/xerrors"
)

type ytState struct {
	LastTableTS int64 `yson:"_last_table_ts,attr"`
}

func loadYtState(tx yt.CypressClient, tmpPath ypath.Path) (ytState, error) {
	statePath := tmpPath.Child("__state")

	exists, err := tx.NodeExists(context.Background(), statePath, nil)
	if err != nil {
		return ytState{}, xerrors.Errorf("Cannot check state dir for existence: %w", err)
	}

	if !exists {
		return ytState{
			LastTableTS: 0,
		}, nil
	}

	attrPath := statePath.Child("@_lfstaging_state")

	var result ytState
	err = tx.GetNode(
		context.Background(),
		attrPath,
		&result,
		&yt.GetNodeOptions{
			Attributes: []string{"_last_table_ts"},
		},
	)
	if err != nil {
		return ytState{}, err
	}
	return result, nil
}

func storeYtState(tx yt.CypressClient, tmpPath ypath.Path, state ytState) error {
	statePath := tmpPath.Child("__state")
	_, err := tx.CreateNode(context.Background(), statePath, yt.NodeMap, &yt.CreateNodeOptions{
		Recursive:      true,
		IgnoreExisting: true,
	})
	if err != nil {
		return xerrors.Errorf("Cannot create state dir: %w", err)
	}

	attrPath := statePath.Child("@_lfstaging_state")

	return tx.SetNode(
		context.Background(),
		attrPath,
		state,
		nil,
	)
}
