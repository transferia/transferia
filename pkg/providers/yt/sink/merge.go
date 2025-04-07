package sink

import (
	"context"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	yt2 "github.com/transferia/transferia/pkg/providers/yt"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/mapreduce"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

func Merge(
	ctx context.Context,
	client yt.Client,
	logger log.Logger,
	dirPath ypath.Path,
	cleanupPath ypath.Path,
	prefix string,
	infix string,
	tmpSuffix string,
	pathToBinary ypath.Path,
	tableWriterSpec interface{},
	buildAttrs func(schema schema.Schema) map[string]interface{},
	tableRotationEnabled bool,
) error {
	// If table rotation is enabled then such table hierarhy would be created inside dirPath: {dirPath}/{origTableName}_{partInfix}/{rotatedName},
	// otherwise - {dirPath}/{origTableName}_{partInfix}_{partTmpSuffix}.
	// So for rotated tables we moves whole directory {origTableName} with all nested tables and final tables otherwise.
	// Thus we should not list nodes recursively for rotated tables to get correct merge and move nodes plans in method getMergeNodes.
	// But we should list nodes recursively for all other cases because {origTableName} may contains slashes.
	listNodesRecursive := !tableRotationEnabled
	inputNodes, err := yt2.ListNodesWithAttrs(ctx, client, dirPath, prefix, listNodesRecursive)
	if err != nil {
		return xerrors.Errorf("unable to list nodes by table prefix: %w", err)
	}

	mergeNodes, tmpToOutputMap, err := getMergeNodes(ctx, client, dirPath, inputNodes, infix, tmpSuffix, logger)
	if err != nil {
		return xerrors.Errorf("unable to prepare merge nodes by infix %q: %w", infix, err)
	}

	err = yt2.WithTx(ctx, client, func(ctx context.Context, tx yt.Tx) error {
		mrClient := mapreduce.New(client).WithTx(tx)
		if err := yt2.Merge(ctx, tx, mrClient, logger, mergeNodes, pathToBinary, tableWriterSpec); err != nil {
			return err
		}
		for _, node := range inputNodes {
			exists, err := tx.NodeExists(ctx, node.Path, nil)
			if err != nil {
				return xerrors.Errorf("unable to check if node %q exists: %w", node.Path, err)
			}
			if !exists {
				logger.Info("node does not exist, probably was moved", log.Any("path", node.Path))
				continue
			}
			if err := tx.RemoveNode(ctx, node.Path, &yt.RemoveNodeOptions{Recursive: true}); err != nil {
				return xerrors.Errorf("unable to remove node %q: %w", node.Path, err)
			}
			logger.Info("node successfully removed", log.Any("path", node.Path))
		}
		return nil
	})
	if err != nil {
		return xerrors.Errorf("unable to merge tmp: %w", err)
	}

	if cleanupPath != "" {
		exists, err := client.NodeExists(ctx, cleanupPath, nil)
		if err != nil {
			return xerrors.Errorf("unable to check if cleanup path %q exists: %w", cleanupPath, err)
		}
		if exists {
			logger.Info("merge result is about to replace existed node", log.Any("path", cleanupPath))
			if err := yt2.UnmountAndWaitRecursive(ctx, logger, client, cleanupPath, nil); err != nil {
				return xerrors.Errorf("unable to unmount node %q: %w", cleanupPath, err)
			}
			logger.Info("existed node is unmounted successfully", log.Any("path", cleanupPath))

			cleanupNode, err := yt2.GetNodeInfo(ctx, client, cleanupPath)
			if err != nil {
				return xerrors.Errorf("unable to get cleanup node %q info: %w", cleanupPath, err)
			}
			if cleanupNode.Attrs.Type == yt.NodeMap {
				logger.Info("due to result is a directory(that means original table it is partitioned by some field value into tables) it is about to be removed before replacement")
				if err := client.RemoveNode(ctx, cleanupPath, &yt.RemoveNodeOptions{Recursive: true}); err != nil {
					return xerrors.Errorf("unable to cleanup directory %q: %w", cleanupPath, err)
				}
				logger.Info("cleanup directory finished successfully", log.Any("path", cleanupPath))
			}
		} else {
			logger.Info("cleanup path does not exist", log.Any("path", cleanupPath))
		}
	} else {
		mergeNodesRefill, err := getMergeNodesRefill(ctx, logger, client, tmpToOutputMap)
		if err != nil {
			return xerrors.Errorf("unable to prepare merge nodes refill")
		}
		err = yt2.WithTx(ctx, client, func(ctx context.Context, tx yt.Tx) error {
			mrClient := mapreduce.New(client).WithTx(tx)
			return yt2.Merge(ctx, tx, mrClient, logger, mergeNodesRefill, pathToBinary, tableWriterSpec)
		})
		if err != nil {
			return xerrors.Errorf("unable to merge refill: %w", err)
		}
	}

	err = moveAndMount(ctx, logger, client, tmpToOutputMap, buildAttrs)
	if err != nil {
		return xerrors.Errorf("unable to move and mount: %w", err)
	}

	return nil
}

func getMergeNodes(
	ctx context.Context,
	client yt.CypressClient,
	path ypath.Path,
	inputNodes []*yt2.NodeInfo,
	inputInfix string,
	tmpSuffix string,
	logger log.Logger,
) (
	map[ypath.Path][]*yt2.NodeInfo,
	map[ypath.Path]ypath.Path,
	error,
) {
	mergeNodes := map[ypath.Path][]*yt2.NodeInfo{}
	tmpToOutputMap := map[ypath.Path]ypath.Path{}
	for _, inputNode := range inputNodes {
		infixIndex := strings.LastIndex(inputNode.Name, inputInfix)
		if infixIndex == -1 {
			return nil, nil, xerrors.Errorf("infix %q not found in node %q", inputInfix, inputNode.Path)
		}
		outputPath := yt2.SafeChild(path, inputNode.Name[:infixIndex])
		outputPathTmp := ypath.Path(string(outputPath) + tmpSuffix)
		tmpToOutputMap[outputPathTmp] = outputPath
		switch inputNode.Attrs.Type {
		case yt.NodeMap:
			childNodes, err := yt2.ListNodesWithAttrs(ctx, client, inputNode.Path, "", false)
			if err != nil {
				return nil, nil, xerrors.Errorf("unable to get child nodes: %w", err)
			}
			for _, childNode := range childNodes {
				childOutputPath := yt2.SafeChild(outputPathTmp, childNode.Name)
				mergeNodes[childOutputPath] = append(mergeNodes[childOutputPath], childNode)
			}
		case yt.NodeTable:
			mergeNodes[outputPathTmp] = append(mergeNodes[outputPathTmp], inputNode)
		default:
			return nil, nil, xerrors.Errorf("unsupported node type: %v", inputNode.Attrs.Type)
		}
	}
	logMergePlan(path, inputInfix, tmpSuffix, inputNodes, mergeNodes, tmpToOutputMap, logger)
	return mergeNodes, tmpToOutputMap, nil
}

type mapFunc[E any, S any] func(E) S

func mapMap[M ~map[K]V, T map[A]B, K, A comparable, V, B any](
	m M,
	keyF mapFunc[K, A],
	valueF mapFunc[V, B],
) T {
	result := make(map[A]B)
	for k, v := range m {
		result[keyF(k)] = valueF(v)
	}
	return result
}

func logMergePlan(path ypath.Path, inputInfix string, tmpSuffix string,
	inputNodes []*yt2.NodeInfo,
	mergeNodes map[ypath.Path][]*yt2.NodeInfo,
	tmpToOutput map[ypath.Path]ypath.Path,
	logger log.Logger,
) {
	ypathToString := func(p ypath.Path) string { return string(p) }

	getNodePath := func(n *yt2.NodeInfo) string {
		if n == nil {
			return ""
		}
		return string(n.Path)
	}

	getNodesPath := func(nodes []*yt2.NodeInfo) []string {
		return yslices.Map(nodes, getNodePath)
	}

	mergePlan := mapMap(mergeNodes, ypathToString, getNodesPath)
	movePlan := mapMap(tmpToOutput, ypathToString, ypathToString)
	logger.Infof("infer merge plan by input infix - %v and tmp suffix - %v for input(%v) inside %v:\n\t\tmerges input tables to tmp tables:\n\t\t\t%v\n\t\tmove tmp to result:\n\t\t\t%v",
		inputInfix,
		tmpSuffix,
		yslices.Map(inputNodes, getNodePath),
		path,
		mergePlan,
		movePlan,
	)

}

func getMergeNodesRefill(ctx context.Context, logger log.Logger, client yt.CypressClient, tmpToOutputMap map[ypath.Path]ypath.Path) (map[ypath.Path][]*yt2.NodeInfo, error) {
	mergeNodes := map[ypath.Path][]*yt2.NodeInfo{}
	for tmpPath, outputPath := range tmpToOutputMap {
		outputExists, err := client.NodeExists(ctx, outputPath, nil)
		if err != nil {
			return nil, xerrors.Errorf("unable to check if node %q exists: %w", outputPath, err)
		}
		if !outputExists {
			logger.Info("output path does not exist, nothing to refill", log.Any("path", outputPath))
			continue
		}

		outputNode, err := yt2.GetNodeInfo(ctx, client, outputPath)
		if err != nil {
			return nil, xerrors.Errorf("unable to get output node %q info: %w", outputPath, err)
		}
		tmpNode, err := yt2.GetNodeInfo(ctx, client, tmpPath)
		if err != nil {
			return nil, xerrors.Errorf("unable to get tmp node %q info: %w", tmpPath, err)
		}
		if tmpNode.Attrs.Type == yt.NodeMap {
			outputChildNodes, err := yt2.ListNodesWithAttrs(ctx, client, outputPath, "", false)
			if err != nil {
				return nil, xerrors.Errorf("unable to list nodes: %w", err)
			}
			for _, outputChildNode := range outputChildNodes {
				tmpChildPath := yt2.SafeChild(tmpPath, outputChildNode.Name)
				exists, err := client.NodeExists(ctx, tmpChildPath, nil)
				if err != nil {
					return nil, xerrors.Errorf("unable to check if node %q exists", tmpChildPath)
				}
				if exists {
					mergeNodes[tmpChildPath] = append(mergeNodes[tmpChildPath], outputChildNode)
				}
			}
			for tmpChildPath := range mergeNodes {
				tmpChildNode, err := yt2.GetNodeInfo(ctx, client, tmpChildPath)
				if err != nil {
					return nil, xerrors.Errorf("unable to get tmp node %q info: %w", tmpChildPath, err)
				}
				mergeNodes[tmpChildPath] = append(mergeNodes[tmpChildPath], tmpChildNode)
			}
		} else {
			mergeNodes[tmpPath] = []*yt2.NodeInfo{outputNode, tmpNode}
		}

	}
	return mergeNodes, nil
}

func moveAndMount(
	ctx context.Context,
	logger log.Logger,
	client yt.Client,
	srcDstMap map[ypath.Path]ypath.Path,
	buildAttrs func(schema schema.Schema) map[string]interface{},
) error {
	srcDstMapFinal := map[ypath.Path]ypath.Path{}
	var srcDirsToRemoveAfter []ypath.Path
	for src, dst := range srcDstMap {
		srcInfo, err := yt2.GetNodeInfo(ctx, client, src)
		if err != nil {
			return xerrors.Errorf("unable to get source %q info: %w", src, err)
		}
		if srcInfo.Attrs.Type == yt.NodeMap {
			var srcNodes []struct {
				Name string `yson:",value"`
			}
			if err := client.ListNode(ctx, src, &srcNodes, nil); err != nil {
				return xerrors.Errorf("unable to list source %q nodes: %w", src, err)
			}
			for _, srcNode := range srcNodes {
				srcDstMapFinal[yt2.SafeChild(src, srcNode.Name)] = yt2.SafeChild(dst, srcNode.Name)
			}
			srcDirsToRemoveAfter = append(srcDirsToRemoveAfter, src)
		} else {
			srcDstMapFinal[src] = dst
		}
	}

	logger.Infof("move and mount: final(%v), input(%v)", srcDstMapFinal, srcDstMap)

	if err := yt2.MoveAndMount(ctx, logger, client, srcDstMapFinal, buildAttrs); err != nil {
		return err
	}

	for _, srcDir := range srcDirsToRemoveAfter {
		if err := client.RemoveNode(ctx, srcDir, nil); err != nil {
			return xerrors.Errorf("unable to remove source directory %q: %w", srcDir, err)
		}
		logger.Info("source directory successfully removed", log.Any("path", srcDir))
	}
	return nil
}
