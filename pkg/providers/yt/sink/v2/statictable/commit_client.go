package statictable

import (
	"context"
	"time"

	"github.com/transferria/transferria/library/go/core/xerrors"
	"github.com/transferria/transferria/pkg/abstract"
	yt2 "github.com/transferria/transferria/pkg/providers/yt"
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

func newCommitClient(tx yt.Tx, client yt.Client, scheme []abstract.ColSchema, pool string, optimizedFor string, customAttributes map[string]any) *commitClient {
	return &commitClient{
		Tx:               tx,
		Client:           client,
		Scheme:           makeYtSchema(scheme),
		Pool:             pool,
		OptimizedFor:     optimizedFor,
		CustomAttributes: customAttributes,
	}
}
