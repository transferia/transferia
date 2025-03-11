package kv

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

//---------------------------------------------------------------------------------------------------------------------
// If you create yt dyn_table by Path, where earlier another dyn_table existed, after success creation some time
// you will get some errors on random functions (for example: tx.Commit() after tx.InsertRows()).
// You will get them at random, in random places.
//
// Possible errors (it can be not full list - only what I caught):
//     - call d9acb8df-61491676-c6b6feaa-9d83ede6 failed: table //home/logfeller/tmp/test_kp3 has no mounted tablets
//     - call c1d40783-d749828c-541ee28d-b3e9d7df failed: error committing transaction 17eff820-40006820-3fe0002-a260d302: error sending transaction rows: no such tablet 3b41-38dca8-3aae02be-b09a09cb
//     - call 62c55fc4-7a42d8ad-5ba9bdac-f0894f53 failed: error committing transaction 17eff8a3-80001fc6-3fe0002-9260f7db: error sending transaction rows: tablet 96d1-6ab2a2-1b6e02be-845892d5 is not known
//     - call d17f5630-f14f95b0-5b1f538a-dbcd1675 failed: error getting mount info for #20b19-1c7d6-3fe0191-4e24a530: error getting attributes of table #20b19-1c7d6-3fe0191-4e24a530: error communicating with master: error resolving Path #20b19-1c7d6-3fe0191-4e24a530/@: no such object 20b19-1c7d6-3fe0191-4e24a530
//
// This happens bcs yt caches some time stores metadata of previous dyn_table
// "The only & best way to handle it - retry" - babenko@
//---------------------------------------------------------------------------------------------------------------------

type YtDynTableKVWrapper struct {
	YtClient yt.Client
	Path     ypath.Path

	keyStructExample interface{}
	valStructExample interface{}
}

func (l *YtDynTableKVWrapper) GetValueByKey(ctx context.Context, key interface{}) (bool, interface{}, error) {
	tx, err := l.YtClient.BeginTabletTx(ctx, nil)
	if err != nil {
		return false, nil, xerrors.Errorf("Cannot begin tablet transaction: %w", err)
	}
	found, result, err := l.GetValueByKeyTx(ctx, tx, key)
	if err != nil {
		_ = tx.Abort()
		return found, nil, xerrors.Errorf("Table read failed: %w", err)
	}
	return found, result, tx.Commit()
}

func (l *YtDynTableKVWrapper) GetValueByKeyTx(ctx context.Context, tx yt.TabletTx, key interface{}) (bool, interface{}, error) {
	if !util.IsTwoStructTypesTheSame(key, l.keyStructExample) {
		return false, nil, xerrors.Errorf("key has wrong type")
	}

	var result interface{}
	res, err := tx.LookupRows(ctx, l.Path, []interface{}{key}, nil)
	if err != nil {
		//nolint:descriptiveerrors
		return false, nil, err
	}
	found := false
	for res.Next() {
		found = true
		if err := res.Scan(&result); err != nil {
			//nolint:descriptiveerrors
			return found, nil, err
		}
		result = util.ExtractStructFromScanResult(result, l.valStructExample)
	}
	return found, result, nil
}

func (l *YtDynTableKVWrapper) CountAllRows(ctx context.Context) (uint64, error) {
	res, err := l.YtClient.SelectRows(ctx, fmt.Sprintf("sum(1) as Count from [%v] group by 1", l.Path), nil)
	if err != nil {
		//nolint:descriptiveerrors
		return 0, err
	}
	defer res.Close()

	type countRow struct {
		Count int64
	}

	var count countRow
	for res.Next() {
		if err := res.Scan(&count); err != nil {
			//nolint:descriptiveerrors
			return 0, err
		}
	}
	return uint64(count.Count), nil
}

func (l *YtDynTableKVWrapper) InsertRow(ctx context.Context, key, value interface{}) error {
	return l.InsertRows(ctx, []interface{}{key}, []interface{}{value})
}

func (l *YtDynTableKVWrapper) InsertRowTx(ctx context.Context, tx yt.TabletTx, key, value interface{}) error {
	return l.InsertRowsTx(ctx, tx, []interface{}{key}, []interface{}{value})
}

func (l *YtDynTableKVWrapper) InsertRows(ctx context.Context, keys, values []interface{}) error {
	tx, err := l.YtClient.BeginTabletTx(ctx, nil)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}
	err = l.InsertRowsTx(ctx, tx, keys, values)
	if err != nil {
		_ = tx.Abort()
		//nolint:descriptiveerrors
		return err
	}
	//nolint:descriptiveerrors
	return tx.Commit()
}

func (l *YtDynTableKVWrapper) InsertRowsTx(ctx context.Context, tx yt.TabletTx, keys, values []interface{}) error {
	if len(keys) != len(values) {
		return xerrors.Errorf("len(keys)(%d) != len(values)(%d)", len(keys), len(values))
	}

	var kv []interface{}
	for i := range keys {
		if !util.IsTwoStructTypesTheSame(keys[i], l.keyStructExample) {
			return xerrors.Errorf("key has wrong type")
		}
		if !util.IsTwoStructTypesTheSame(values[i], l.valStructExample) {
			return xerrors.Errorf("value has wrong type")
		}
		kv = append(kv, util.MakeUnitedStructByKeyVal(true, keys[i], values[i]))
	}

	if err := tx.InsertRows(ctx, l.Path, kv, nil); err != nil {
		return err
	}
	return nil
}

func (l *YtDynTableKVWrapper) DeleteRow(ctx context.Context, key interface{}) error {
	return l.DeleteRows(ctx, []interface{}{key})
}

func (l *YtDynTableKVWrapper) DeleteRows(ctx context.Context, keys []interface{}) error {
	tx, err := l.YtClient.BeginTabletTx(ctx, nil)
	if err != nil {
		//nolint:descriptiveerrors
		return err
	}
	err = tx.DeleteRows(ctx, l.Path, keys, nil)
	if err != nil {
		_ = tx.Abort()
		//nolint:descriptiveerrors
		return err
	}
	//nolint:descriptiveerrors
	return tx.Commit()
}

func createDynTableAndMount(ctx context.Context, yc yt.Client, path ypath.YPath, schema schema.Schema, bundle string, attrs map[string]interface{}) error {
	finalAttrs := make(map[string]interface{})
	finalAttrs["dynamic"] = true
	finalAttrs["schema"] = schema
	if bundle != "" {
		finalAttrs["tablet_cell_bundle"] = bundle
	}
	for k, v := range attrs {
		finalAttrs[k] = v
	}

	_, err := yc.CreateNode(ctx, path, yt.NodeTable, &yt.CreateNodeOptions{
		Recursive:  true,
		Attributes: finalAttrs,
	})

	if err != nil {
		//nolint:descriptiveerrors
		return err
	}

	//nolint:descriptiveerrors
	return migrate.MountAndWait(ctx, yc, path.YPath())
}

func NewYtDynTableKVWrapper(ctx context.Context, client yt.Client, path ypath.Path, key, val interface{}, bundle string, attrs map[string]interface{}) (*YtDynTableKVWrapper, error) {
	err := util.ValidateKey(key)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}
	err = util.ValidateVal(val)
	if err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}

	if err := backoff.Retry(func() error {
		exist, err := client.NodeExists(ctx, path, nil)
		if err != nil {
			return xerrors.Errorf("Cannot check existence of the node %s: %w", path.String(), err)
		}
		if !exist {
			keyValStruct := util.MakeUnitedStructByKeyVal(false, key, val)
			return createDynTableAndMount(ctx, client, path, schema.MustInfer(keyValStruct), bundle, attrs)
		}
		return nil
	}, backoff.WithMaxRetries(backoff.NewConstantBackOff(time.Second), 3)); err != nil {
		//nolint:descriptiveerrors
		return nil, err
	}
	return &YtDynTableKVWrapper{
		YtClient:         client,
		Path:             path,
		keyStructExample: key,
		valStructExample: val,
	}, nil
}
