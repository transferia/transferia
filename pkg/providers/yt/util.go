//go:build !disable_yt_provider

package yt

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/library/go/ptr"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/migrate"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
	"golang.org/x/sync/semaphore"
)

var defaultHandleParams = NewHandleParams(50)

type ColumnSchema struct {
	Name    string      `yson:"name" json:"name"`
	YTType  schema.Type `yson:"type" json:"type"`
	Primary bool        `json:"primary"`
}

type nodeHandler func(ctx context.Context, client yt.Client, path ypath.Path, attrs *NodeAttrs) error

func handleNodes(
	ctx context.Context,
	client yt.Client,
	path ypath.Path,
	params *HandleParams,
	handler nodeHandler,
) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errors := make(chan error)
	var count int
	semaphore := semaphore.NewWeighted(params.ConcurrencyLimit)
	err := handleNodesAsync(ctx, client, path, handler, semaphore, errors, &count)
	if err != nil {
		return err
	}

	for i := 0; i < count; i++ {
		err = <-errors
		if err != nil {
			return xerrors.Errorf("unable to handle node: %w", err)
		}
	}

	return nil
}

func handleNodesAsync(
	ctx context.Context,
	client yt.Client,
	path ypath.Path,
	handler nodeHandler,
	semaphore *semaphore.Weighted,
	errors chan<- error,
	count *int,
) error {
	attrs, err := GetNodeAttrs(ctx, client, path)
	if err != nil {
		return xerrors.Errorf("unable to get node attributes: %w", err)
	}

	switch attrs.Type {
	case yt.NodeMap:
		var childNodes []struct {
			Name string `yson:",value"`
		}
		err := client.ListNode(ctx, path, &childNodes, nil)
		if err != nil {
			return xerrors.Errorf("unable to list nodes: %w", err)
		}
		for _, childNode := range childNodes {
			err = handleNodesAsync(ctx, client, SafeChild(path, childNode.Name), handler, semaphore, errors, count)
			if err != nil {
				return xerrors.Errorf("unable to handle child node: %w", err)
			}
		}
		return nil
	default:
		go func() {
			err := semaphore.Acquire(ctx, 1)
			if err == nil {
				err = handler(ctx, client, path, attrs)
				semaphore.Release(1)
			}
			errors <- err
		}()
		*count++
		return nil
	}
}

type HandleParams struct {
	ConcurrencyLimit int64
}

func NewHandleParams(concurrencyLimit int64) *HandleParams {
	return &HandleParams{ConcurrencyLimit: concurrencyLimit}
}

func UnmountAndWaitRecursive(ctx context.Context, logger log.Logger, client yt.Client, path ypath.Path, params *HandleParams) error {
	if params == nil {
		params = defaultHandleParams
	}
	return handleNodes(ctx, client, path, params,
		func(ctx context.Context, client yt.Client, path ypath.Path, attrs *NodeAttrs) error {
			if attrs.Type == yt.NodeTable && attrs.Dynamic {
				if attrs.TabletState != yt.TabletUnmounted {
					err := MountUnmountWrapper(ctx, client, path, migrate.UnmountAndWait)
					if err == nil {
						logger.Info("successfully unmounted table", log.Any("path", path))
					}
					return err
				}
				logger.Info("table is already unmounted", log.Any("path", path))
			}
			return nil
		})
}

func MountAndWaitRecursive(ctx context.Context, logger log.Logger, client yt.Client, path ypath.Path, params *HandleParams) error {
	if params == nil {
		params = defaultHandleParams
	}
	return handleNodes(ctx, client, path, params,
		func(ctx context.Context, client yt.Client, path ypath.Path, attrs *NodeAttrs) error {
			if attrs.Type == yt.NodeTable && attrs.Dynamic {
				if attrs.TabletState != yt.TabletMounted {
					err := MountUnmountWrapper(ctx, client, path, migrate.MountAndWait)
					if err == nil {
						logger.Info("successfully mounted table", log.Any("path", path))
					}
					return err
				}
				logger.Info("table is already mounted", log.Any("path", path))
			}
			return nil
		})
}

func YTColumnToColSchema(columns []schema.Column) *abstract.TableSchema {
	tableSchema := make([]abstract.ColSchema, len(columns))

	for i, c := range columns {
		tableSchema[i] = abstract.ColSchema{
			ColumnName:   c.Name,
			DataType:     string(c.Type),
			PrimaryKey:   c.SortOrder != "",
			Required:     c.Required,
			TableSchema:  "",
			TableName:    "",
			Path:         "",
			FakeKey:      false,
			Expression:   "",
			OriginalType: "",
			Properties:   nil,
		}
	}

	return abstract.NewTableSchema(tableSchema)
}

func WaitMountingPreloadState(yc yt.Client, path ypath.Path) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute) // mounting for reading takes 5-10 minutes
	defer cancel()

	poll := func() (bool, error) {
		var currentState string
		if err := yc.GetNode(ctx, path.Attr("preload_state"), &currentState, nil); err != nil {
			return false, err
		}
		if currentState == "complete" {
			return true, nil
		}

		return false, nil
	}

	tick := time.NewTicker(10 * time.Second)
	defer tick.Stop()
	for {
		stop, err := poll()
		if err != nil {
			return xerrors.Errorf("unable to poll master: %w", err)
		}

		if stop {
			return nil
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-tick.C:
		}
	}
}

func ResolveMoveOptions(client yt.CypressClient, table ypath.Path, isRecursive bool) *yt.MoveNodeOptions {
	ctx := context.Background()

	var tableTimeout int64
	var tableExpirationTime string
	preserveExpirationTimeoutErr := client.GetNode(ctx, table.Attr("expiration_timeout"), &tableTimeout, nil)
	preserveExpirationTimeErr := client.GetNode(ctx, table.Attr("expiration_time"), &tableExpirationTime, nil)

	result := &yt.MoveNodeOptions{
		Force:                     true,
		Recursive:                 isRecursive,
		PreserveExpirationTimeout: ptr.Bool(preserveExpirationTimeoutErr == nil),
		PreserveExpirationTime:    ptr.Bool(preserveExpirationTimeErr == nil),
	}
	return result
}

func ToYtSchema(original []abstract.ColSchema, fixAnyTypeInPrimaryKey bool) []schema.Column {
	result := make([]schema.Column, len(original))
	for idx, el := range original {
		result[idx] = schema.Column{
			Name:       el.ColumnName,
			Expression: el.Expression,
			Type:       schema.Type(el.DataType),
		}
		if el.PrimaryKey {
			result[idx].SortOrder = schema.SortAscending
			if result[idx].Type == schema.TypeAny && fixAnyTypeInPrimaryKey {
				result[idx].Type = schema.TypeString // should not use any as keys
			}
		}
	}
	sort.Slice(result, func(i, j int) bool {
		return result[i].SortOrder != schema.SortNone && result[j].SortOrder == schema.SortNone
	})
	return result
}

func MakeTableName(tableID abstract.TableID, altNames map[string]string) string {
	var name string
	if tableID.Namespace == "public" || tableID.Namespace == "" {
		name = tableID.Name
	} else {
		name = fmt.Sprintf("%v_%v", tableID.Namespace, tableID.Name)
	}

	if altName, ok := altNames[name]; ok {
		name = altName
	}

	return name
}

func MountUnmountWrapper(
	ctx context.Context,
	ytClient yt.Client,
	path ypath.Path,
	f func(context.Context, yt.Client, ypath.Path) error,
) error {
	customCtx, cancel := context.WithTimeout(ctx, time.Minute*5)
	defer cancel()
	return f(customCtx, ytClient, path)
}
