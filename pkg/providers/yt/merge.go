package yt

import (
	"context"

	"github.com/transferia/transferia/library/go/core/xerrors"
	ytmerge "github.com/transferia/transferia/pkg/providers/yt/mergejob"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/mapreduce"
	ytspec "go.ytsaurus.tech/yt/go/mapreduce/spec"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/exp/slices"
)

func init() {
	mapreduce.Register(&ytmerge.MergeWithDeduplicationJob{
		Untyped: mapreduce.Untyped{},
	})
}

func Merge(
	ctx context.Context,
	cypressClient yt.CypressClient,
	mrClient mapreduce.Client,
	logger log.Logger,
	mergeNodes map[ypath.Path][]*NodeInfo,
	pathToBinary ypath.Path,
	tableWriterSpec interface{},
) error {
	var specs []*ytspec.Spec
	for outputPath, inputNodes := range mergeNodes {
		var spec *ytspec.Spec
		var outputAttrs NodeAttrs
		for _, inputNode := range inputNodes {
			keyColumns := inputNode.Attrs.Schema.KeyColumns()
			if spec == nil {
				outputAttrs.Schema = inputNode.Attrs.Schema
				outputAttrs.OptimizeFor = inputNode.Attrs.OptimizeFor
				outputAttrs.Atomicity = inputNode.Attrs.Atomicity
				if len(keyColumns) > 0 {
					outputAttrs.Schema.UniqueKeys = true
					spec = ytspec.Reduce()
					spec.ReduceBy = keyColumns
					spec.AddOutput(outputPath)
				} else {
					spec = ytspec.Merge()
					spec.MergeMode = "ordered"
					spec.CombineChunks = true
					spec.SetOutput(outputPath)
				}
				if tableWriterSpec != nil {
					util.NewIfNil(&spec.JobIO).TableWriter = tableWriterSpec
				}
				spec.Pool = "transfer_manager"
			} else {
				if !slices.Equal(spec.ReduceBy, keyColumns) {
					return xerrors.Errorf("input table keys are not equal: %v != %v", spec.ReduceBy, keyColumns)
				}
				if !schemasEqual(outputAttrs.Schema, inputNode.Attrs.Schema) {
					return xerrors.Errorf("input table schemas are not equal: %v != %v", outputAttrs.Schema, inputNode.Attrs.Schema)
				}
				if outputAttrs.Atomicity != inputNode.Attrs.Atomicity {
					return xerrors.Errorf("input table attributes 'atomicity' are not equal: %v != %v", outputAttrs.Atomicity, inputNode.Attrs.Atomicity)
				}
			}
			spec.AddInput(inputNode.Path)
		}

		outputExists, err := cypressClient.NodeExists(ctx, outputPath, nil)
		if err != nil {
			return xerrors.Errorf("unable to check if output node exists: %w", err)
		}
		if !outputExists {
			_, err := cypressClient.CreateNode(ctx, outputPath, yt.NodeTable, &yt.CreateNodeOptions{
				Recursive: true,
				Attributes: map[string]interface{}{
					"schema":       outputAttrs.Schema,
					"optimize_for": outputAttrs.OptimizeFor,
					"atomicity":    outputAttrs.Atomicity,
				},
			})
			if err != nil {
				return xerrors.Errorf("unable to create output node: %w", err)
			}
		} else {
			outputNodeInfo, err := GetNodeInfo(ctx, cypressClient, outputPath)
			if err != nil {
				return xerrors.Errorf("unable to get output node info: %w", err)
			}
			if !outputNodeInfo.Attrs.Schema.Equal(outputAttrs.Schema) {
				return xerrors.Errorf("output node schema is not equal to expected: %v != %v", outputNodeInfo.Attrs.Schema, outputAttrs.Schema)
			}
			if outputNodeInfo.Attrs.OptimizeFor != outputAttrs.OptimizeFor {
				return xerrors.Errorf("output node attribute 'optimize_for' is not equal to expected: %v != %v", outputNodeInfo.Attrs.OptimizeFor, outputAttrs.OptimizeFor)
			}
			if outputNodeInfo.Attrs.Atomicity != outputAttrs.Atomicity {
				return xerrors.Errorf("output node attribute 'atomicity' is not equal to expected: %v != %v", outputNodeInfo.Attrs.Atomicity, outputAttrs.Atomicity)
			}
		}

		specs = append(specs, spec)
	}

	errors := make(chan error)
	for _, x := range specs {
		go func(spec *ytspec.Spec) {
			if spec.Type == yt.OperationReduce {
				errors <- reduceAndSort(mrClient, logger, spec, pathToBinary)
			} else {
				errors <- merge(mrClient, logger, spec)
			}
		}(x)
	}
	for range specs {
		err := <-errors
		if err != nil {
			return err
		}
	}

	return nil
}

func schemasEqual(schema1, schema2 schema.Schema) bool {
	schema1.UniqueKeys = false
	schema2.UniqueKeys = false
	return schema1.Equal(schema2)
}

func reduceAndSort(mrClient mapreduce.Client, logger log.Logger, reduceSpec *ytspec.Spec, pathToBinary ypath.Path) error {
	if len(reduceSpec.OutputTablePaths) != 1 {
		return xerrors.New("single output path expected")
	}
	outputTablePath := reduceSpec.OutputTablePaths[0]

	reduceSpec.Reducer = new(ytspec.UserScript)
	reduceSpec.Reducer.MemoryLimit = 2147483648
	reduceSpec.Reducer.Environment = map[string]string{
		"DT_YT_SKIP_INIT": "1",
	}
	var reduceOpts []mapreduce.OperationOption
	if pathToBinary != "" {
		logger.Info("path to binary is provided, self upload skipped", log.Any("path", pathToBinary))
		reduceSpec.PatchUserBinary(pathToBinary)
		reduceOpts = append(reduceOpts, mapreduce.SkipSelfUpload())
	} else {
		logger.Warn("path to binary is not specified, self upload will be done")
	}

	logger.Info("starting reduce", log.Any("output_table_path", outputTablePath), log.Array("input_table_paths", reduceSpec.InputTablePaths))
	operation, err := mrClient.Reduce(ytmerge.NewMergeWithDeduplicationJob(), reduceSpec, reduceOpts...)
	if err != nil {
		return xerrors.Errorf("unable to start reduce operation: %w", err)
	}

	logger.Info("waiting reduce completion", log.Any("output_table_path", outputTablePath), log.Any("operation_id", operation.ID()))
	if err = operation.Wait(); err != nil {
		return xerrors.Errorf("unable to reduce: %w", err)
	}
	logger.Info("reduce successfully done", log.Any("output_table_path", outputTablePath))

	sortSpec := ytspec.Sort()
	sortSpec.SortBy = reduceSpec.ReduceBy
	sortSpec.AddInput(outputTablePath)
	sortSpec.SetOutput(outputTablePath)
	sortSpec.Pool = "transfer_manager"
	if reduceSpec.JobIO != nil && reduceSpec.JobIO.TableWriter != nil {
		util.NewIfNil(&sortSpec.PartitionJobIO).TableWriter = reduceSpec.JobIO.TableWriter
		util.NewIfNil(&sortSpec.SortJobIO).TableWriter = reduceSpec.JobIO.TableWriter
		util.NewIfNil(&sortSpec.MergeJobIO).TableWriter = reduceSpec.JobIO.TableWriter
	}
	logger.Info("starting sort", log.Any("path", outputTablePath))
	operation, err = mrClient.Sort(sortSpec)
	if err != nil {
		return xerrors.Errorf("unable to start sort operation: %w", err)
	}
	if err = operation.Wait(); err != nil {
		return xerrors.Errorf("unable to sort: %w", err)
	}
	logger.Info("sort successfully done", log.Any("path", outputTablePath))

	return nil
}

func merge(mrClient mapreduce.Client, logger log.Logger, mergeSpec *ytspec.Spec) error {
	logger.Info("starting merge", log.Any("output_table_path", mergeSpec.OutputTablePath), log.Array("input_table_paths", mergeSpec.InputTablePaths))
	operation, err := mrClient.Merge(mergeSpec)
	if err != nil {
		return xerrors.Errorf("unable to start merge operation: %w", err)
	}

	logger.Info("waiting merge completion", log.Any("output_table_path", mergeSpec.OutputTablePath), log.Any("operation_id", operation.ID()))
	if err = operation.Wait(); err != nil {
		return xerrors.Errorf("unable to merge: %w", err)
	}
	logger.Info("merge successfully done", log.Any("output_table_path", mergeSpec.OutputTablePath))
	return nil
}

func MoveAndMount(
	ctx context.Context,
	logger log.Logger,
	client yt.Client,
	srcDstMap map[ypath.Path]ypath.Path,
	buildAttrs func(schema schema.Schema) map[string]interface{},
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errors := make(chan error)
	for src, dst := range srcDstMap {
		go func(src, dst ypath.Path) {
			err := moveAndMount(ctx, client, src, dst, buildAttrs)
			if err == nil {
				logger.Info("move and mount successfully done", log.Any("source", src), log.Any("destination", dst))
			}
			errors <- err
		}(src, dst)
	}

	for range srcDstMap {
		err := <-errors
		if err != nil {
			return err
		}
	}
	return nil
}

func moveAndMount(
	ctx context.Context,
	client yt.Client,
	src ypath.Path,
	dst ypath.Path,
	buildAttrs func(schema schema.Schema) map[string]interface{},
) error {
	exists, err := client.NodeExists(ctx, dst, nil)
	if err != nil {
		return xerrors.Errorf("unable to check if destination %q exists: %w", dst, err)
	}
	if exists {
		if err := migrate.UnmountAndWait(ctx, client, dst); err != nil {
			return xerrors.Errorf("unable to unmount destination %q: %w", dst, err)
		}
	}

	moveOptions := ResolveMoveOptions(client, src, true)
	if _, err := client.MoveNode(ctx, src, dst, moveOptions); err != nil {
		return xerrors.Errorf("unable to move from %q to %q: %w", src, dst, err)
	}

	alterOptions := yt.AlterTableOptions{
		Dynamic: util.TruePtr(),
	}
	if err := client.AlterTable(ctx, dst, &alterOptions); err != nil {
		return xerrors.Errorf("unable to alter destination table %q: %w", dst, err)
	}

	dstInfo, err := GetNodeInfo(ctx, client, dst)
	if err != nil {
		return xerrors.Errorf("unable to get node info: %w", err)
	}
	attrs := buildAttrs(dstInfo.Attrs.Schema)
	if err = client.MultisetAttributes(ctx, dst.Attrs(), attrs, nil); err != nil {
		return xerrors.Errorf("unable to set destination attributes: %w", err)
	}

	if err := migrate.MountAndWait(ctx, client, dst); err != nil {
		return xerrors.Errorf("unable to mount destination table %q: %w", dst, err)
	}
	return nil
}
