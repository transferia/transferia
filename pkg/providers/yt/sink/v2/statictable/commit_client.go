//go:build !disable_yt_provider

package statictable

import (
	"context"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	ytmerge "github.com/transferia/transferia/pkg/providers/yt/mergejob"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

const (
	blockSize         = 256 * (2 << 10)
	maxFailedJobCount = 5
)

func init() {
	mapreduce.Register(&ytmerge.MergeWithDeduplicationJob{
		Untyped: mapreduce.Untyped{},
	})
}

type commitClient struct {
	Tx     yt.Tx
	Client yt.Client
	Scheme schema.Schema

	Pool             string
	OptimizedFor     string
	CustomAttributes map[string]any
}

func (c *commitClient) sortTable(currentPath ypath.Path, sortedPath ypath.Path) (ypath.Path, error) {
	if !isSorted(c.Scheme) {
		return currentPath, nil
	}

	keyCols := c.Scheme.KeyColumns()
	if err := c.createTableForOperation(sortedPath, c.Scheme); err != nil {
		return "unable to create table for the sorting operation: %w", err
	}

	sortClient := mapreduce.New(c.Client).WithTx(c.Tx)
	sortSpec := spec.Sort()
	sortSpec.Pool = c.Pool
	sortSpec.InputTablePaths = []ypath.YPath{currentPath}
	sortSpec.OutputTablePath = sortedPath
	sortSpec.SortBy = keyCols
	sortSpec.PartitionJobIO = &spec.JobIO{TableWriter: map[string]any{"block_size": blockSize}}
	sortSpec.MergeJobIO = &spec.JobIO{TableWriter: map[string]any{"block_size": blockSize}}
	sortSpec.SortJobIO = &spec.JobIO{TableWriter: map[string]any{"block_size": blockSize}}
	sortSpec.MaxFailedJobCount = maxFailedJobCount
	mergeOperation, err := sortClient.Sort(sortSpec)
	if err != nil {
		return "", xerrors.Errorf("unable to start sorting operation: %w", err)
	}

	if err := mergeOperation.Wait(); err != nil {
		return "", xerrors.Errorf("unable to finish sort operation or to check operation status: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.Tx.RemoveNode(ctx, currentPath, nil); err != nil {
		return "", xerrors.Errorf("unable to remove sorting tmp table: %w", err)
	}
	return sortedPath, nil
}

func (c *commitClient) mergeTables(currentPath ypath.Path, userPath ypath.Path, sortedMerge bool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	ok, err := c.Tx.NodeExists(ctx, userPath, nil)
	if err != nil {
		return xerrors.Errorf("unable to check table existence: %w", err)
	}
	if !ok {
		return nil
	}

	if err := c.checkTablesAttrsCompatibility(currentPath, userPath); err != nil {
		return xerrors.Errorf("unable to merge tables: %w", err)
	}

	mergeMode := "ordered"
	var keyCols []string
	if sortedMerge {
		mergeMode = "sorted"
		keyCols = c.Scheme.KeyColumns()
	}

	mergeClient := mapreduce.New(c.Client).WithTx(c.Tx)
	mergeSpec := spec.Merge()
	mergeSpec.Pool = c.Pool
	mergeSpec.InputTablePaths = []ypath.YPath{userPath, currentPath}
	mergeSpec.OutputTablePath = currentPath
	mergeSpec.MergeMode = mergeMode
	mergeSpec.MergeBy = keyCols
	mergeSpec.CombineChunks = true
	mergeSpec.MaxFailedJobCount = maxFailedJobCount
	mergeOperation, err := mergeClient.Merge(mergeSpec)
	if err != nil {
		return xerrors.Errorf("unable to start merging operation: %w", err)
	}

	if err := mergeOperation.Wait(); err != nil {
		return xerrors.Errorf("unable to finish merge operation or to check operation status: %w", err)
	}

	return nil
}

func (c *commitClient) reduceTables(currentPath, userPath, reducedPath, pathToBinary ypath.Path) (ypath.Path, error) {
	ok, err := c.Tx.NodeExists(context.Background(), userPath, nil)
	if err != nil {
		return "", xerrors.Errorf("unable to check table existence: %w", err)
	}
	if !ok {
		return currentPath, nil
	}

	if err := c.createTableForOperation(reducedPath, c.Scheme); err != nil {
		return "", xerrors.Errorf("unable to create table for the reducing operation: %w", err)
	}

	reduceClient := mapreduce.New(c.Client).WithTx(c.Tx)
	reduceSpec := spec.Reduce()
	reduceSpec.Pool = c.Pool
	reduceSpec.InputTablePaths = []ypath.YPath{userPath, currentPath}
	reduceSpec.OutputTablePaths = []ypath.YPath{reducedPath}
	reduceSpec.SortBy = c.Scheme.KeyColumns()
	reduceSpec.ReduceBy = c.Scheme.KeyColumns()
	reduceSpec.ReduceJobIO = &spec.JobIO{TableWriter: map[string]any{"block_size": blockSize}}
	reduceSpec.MaxFailedJobCount = maxFailedJobCount
	reduceSpec.Reducer = new(spec.UserScript)
	reduceSpec.Reducer.MemoryLimit = 2147483648
	reduceSpec.Reducer.Environment = map[string]string{
		"DT_YT_SKIP_INIT": "1",
	}
	var reduceOpts []mapreduce.OperationOption
	if pathToBinary != "" {
		reduceSpec.PatchUserBinary(pathToBinary)
		reduceOpts = append(reduceOpts, mapreduce.SkipSelfUpload())
	}

	reduceOperation, err := reduceClient.Reduce(ytmerge.NewMergeWithDeduplicationJob(), reduceSpec, reduceOpts...)
	if err != nil {
		return "", xerrors.Errorf("unable to start reduce operation: %w", err)
	}

	if err = reduceOperation.Wait(); err != nil {
		return "", xerrors.Errorf("unable to reduce: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := c.Tx.RemoveNode(ctx, currentPath, nil); err != nil {
		return "", xerrors.Errorf("unable to remove reducing tmp table: %w", err)
	}
	return reducedPath, nil
}

func (c *commitClient) moveTables(src ypath.Path, dst ypath.Path) error {
	if src == dst {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	moveOptions := yt2.ResolveMoveOptions(c.Tx, src, false)
	if _, err := c.Tx.MoveNode(ctx, src, dst, moveOptions); err != nil {
		return err
	}
	return nil
}

func (c *commitClient) createTableForOperation(tablePath ypath.Path, scheme schema.Schema) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	createOptions := createNodeOptions(scheme, c.OptimizedFor, c.CustomAttributes)
	if _, err := c.Tx.CreateNode(ctx, tablePath, yt.NodeTable, &createOptions); err != nil {
		return err
	}

	return nil
}

func (c *commitClient) checkTablesAttrsCompatibility(tmpTable, userTable ypath.Path) error {
	ctx := context.Background()
	var tmpIsSorted, userIsSorted bool
	if err := c.Tx.GetNode(ctx, tmpTable.Attr("sorted"), &tmpIsSorted, nil); err != nil {
		return xerrors.Errorf("unable to get first table (%s) \"sorted\" attr: %w", tmpTable.String(), err)
	}
	if err := c.Tx.GetNode(ctx, userTable.Attr("sorted"), &userIsSorted, nil); err != nil {
		return xerrors.Errorf("unable to get second table (%s) \"sorted\" attr: %w", userTable.String(), err)
	}

	if tmpIsSorted != userIsSorted {
		return xerrors.Errorf("incompatible table sorting: tmp table (%s) sorted: %t, user table (%s) sorted: %t", tmpTable.String(), tmpIsSorted, userTable.String(), userIsSorted)
	}
	return nil
}

func newCommitClient(tx yt.Tx, client yt.Client, scheme []abstract.ColSchema, pool string, optimizedFor string, customAttributes map[string]any, useUniqueKeys bool) *commitClient {
	finalSchema := makeYtSchema(scheme)
	if useUniqueKeys {
		finalSchema.UniqueKeys = true
	}
	return &commitClient{
		Tx:               tx,
		Client:           client,
		Scheme:           finalSchema,
		Pool:             pool,
		OptimizedFor:     optimizedFor,
		CustomAttributes: customAttributes,
	}
}
