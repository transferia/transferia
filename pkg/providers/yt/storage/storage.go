//go:build !disable_yt_provider

package storage

import (
	"context"
	"fmt"
	"time"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	ytprovider "github.com/transferia/transferia/pkg/providers/yt"
	ytclient "github.com/transferia/transferia/pkg/providers/yt/client"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
	"go.ytsaurus.tech/yt/go/ypath"
	"go.ytsaurus.tech/yt/go/yt"
)

type Storage struct {
	path     string
	ytClient yt.Client
	logger   log.Logger
	config   map[string]interface{}
}

func (s *Storage) Close() {
}

func (s *Storage) Ping() error {
	return nil
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	allTables, err := s.LoadSchema()
	if err != nil {
		return nil, xerrors.Errorf("unable to load schema: %w", err)
	}

	return allTables[table], nil
}

func (s *Storage) TableList(includeTableFilter abstract.IncludeTableList) (abstract.TableMap, error) {
	var tables []struct {
		Name string `yson:",value"`
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := s.ytClient.ListNode(ctx, ypath.Path(s.path), &tables, &yt.ListNodeOptions{}); err != nil {
		return nil, err
	}

	sch, err := s.LoadSchema()
	if err != nil {
		return nil, xerrors.Errorf("Cannot load schema: %w", err)
	}

	res := make(abstract.TableMap)
	for tID, columns := range sch {
		res[tID] = abstract.TableInfo{
			EtaRow: 0,
			IsView: false,
			Schema: columns,
		}
	}

	return model.FilteredMap(res, includeTableFilter), nil
}

func getTableName(t abstract.TableDescription) string {
	if t.Schema == "" || t.Schema == "public" {
		return t.Name
	}

	return t.Schema + "_" + t.Name
}

func (s *Storage) LoadTable(ctx context.Context, t abstract.TableDescription, pusher abstract.Pusher) error {
	st := util.GetTimestampFromContextOrNow(ctx)

	tablePath := ytprovider.SafeChild(ypath.Path(s.path), getTableName(t))
	partID := t.PartID()

	var scheme schema.Schema
	if err := s.ytClient.GetNode(ctx, tablePath.Attr("schema"), &scheme, nil); err != nil {
		return xerrors.Errorf("unable to get schema node: %s: %w", tablePath, err)
	}

	reader, err := s.ytClient.SelectRows(
		ctx,
		fmt.Sprintf("* from [%v]", tablePath),
		&yt.SelectRowsOptions{},
	)
	if err != nil {
		return xerrors.Errorf("unable to select: %s: %w", tablePath, err)
	}

	tableSchema := ytprovider.YTColumnToColSchema(scheme.Columns)

	totalIdx := uint64(0)
	wrapAroundIdx := uint64(0)
	cols := make([]string, len(scheme.Columns))
	for i, c := range scheme.Columns {
		cols[i] = c.Name
	}
	s.logger.Info("start read", log.Any("fqtn", t.Fqtn()), log.Any("schema", tableSchema))
	changes := make([]abstract.ChangeItem, 0)
	for reader.Next() {
		vals := make([]interface{}, len(cols))

		r := map[string]interface{}{}
		if err := reader.Scan(&r); err != nil {
			return xerrors.Errorf("unable to scan: %w", err)
		}
		for i, colName := range cols {
			vals[i] = restore(r[colName], scheme.Columns[i])
		}
		changes = append(changes, abstract.ChangeItem{
			ID:           0,
			LSN:          0,
			CommitTime:   uint64(st.UnixNano()),
			Counter:      0,
			Kind:         abstract.InsertKind,
			Schema:       "",
			Table:        getTableName(t),
			PartID:       partID,
			ColumnNames:  cols,
			ColumnValues: vals,
			TableSchema:  tableSchema,
			OldKeys:      abstract.EmptyOldKeys(),
			TxID:         "",
			Query:        "",
			Size:         abstract.RawEventSize(util.DeepSizeof(vals)),
		})
		if wrapAroundIdx == 10000 {
			if err := pusher(changes); err != nil {
				return xerrors.Errorf("unable to push: %w", err)
			}
			changes = make([]abstract.ChangeItem, 0)
			wrapAroundIdx = 0
		}
		totalIdx++
		wrapAroundIdx++
	}

	if wrapAroundIdx != 0 {
		if err := pusher(changes); err != nil {
			return xerrors.Errorf("unable to push: %w", err)
		}
	}
	s.logger.Info("Sink done uploading table", log.String("fqtn", t.Fqtn()))
	return nil
}

func restore(val interface{}, column schema.Column) interface{} {
	switch column.Type {
	case schema.TypeTimestamp:
		switch v := val.(type) {
		case uint64:
			return schema.Timestamp(v).Time()
		default:
			return v
		}
	case schema.TypeDatetime:
		switch v := val.(type) {
		case uint64:
			return schema.Datetime(v).Time()
		default:
			return v
		}
	case schema.TypeDate:
		switch v := val.(type) {
		case uint64:
			return schema.Date(v).Time()
		default:
			return v
		}
	default:
		return val
	}
}

func (s *Storage) LoadSchema() (dbSchema abstract.DBSchema, err error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var tables []struct {
		Name string `yson:",value"`
	}

	if err := s.ytClient.ListNode(ctx, ypath.Path(s.path), &tables, &yt.ListNodeOptions{}); err != nil {
		return nil, xerrors.Errorf("unable to list: %s: %w", s.path, err)
	}

	resultSchema := make(abstract.DBSchema)
	for _, table := range tables {
		var scheme schema.Schema
		if err := s.ytClient.GetNode(ctx, ytprovider.SafeChild(ypath.Path(s.path), table.Name).Attr("schema"), &scheme, nil); err != nil {
			return nil, xerrors.Errorf("unable to get schema not: %s: %w", table.Name, err)
		}

		tableSchema := ytprovider.YTColumnToColSchema(scheme.Columns)

		for i := range tableSchema.Columns() {
			tableSchema.Columns()[i].TableName = table.Name
		}
		resultSchema[abstract.TableID{Namespace: "", Name: table.Name}] = tableSchema
	}

	return resultSchema, nil
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return 0, xerrors.New("not implemented")
}

func makeTableName(tableID abstract.TableID) string {
	if tableID.Namespace == "public" || tableID.Namespace == "" {
		return tableID.Name
	} else {
		return fmt.Sprintf("%s_%s", tableID.Namespace, tableID.Name)
	}
}

func (s *Storage) ExactTableRowsCount(table abstract.TableID) (uint64, error) {
	pathToTable := ytprovider.SafeChild(ypath.Path(s.path), makeTableName(table))
	return ExactYTTableRowsCount(s.ytClient, pathToTable)
}

func ExactYTTableRowsCount(ytClient yt.Client, pathToTable ypath.Path) (uint64, error) {
	dynamicTable, err := isDynamicTable(ytClient, pathToTable)
	if err != nil {
		return 0, xerrors.Errorf("unable to check if table is dynamic for table %s: %w", pathToTable, err)
	}
	if dynamicTable {
		rows, err := ytClient.SelectRows(
			context.Background(),
			fmt.Sprintf("sum(1) as Count from [%s] group by 1", pathToTable),
			nil,
		)
		if err != nil {
			return 0, xerrors.Errorf("unable to count #rows for dynamic table %s: %w", pathToTable, err)
		}
		defer rows.Close()

		type countRow struct {
			Count uint64
		}
		var count countRow
		for rows.Next() {
			if err := rows.Scan(&count); err != nil {
				return 0, xerrors.Errorf("unable to read result #rows for dynamic table %s: %w", pathToTable, err)
			}
		}
		return count.Count, nil
	} else {
		pathToDynAttr := pathToTable.Attr("row_count")
		var rowCount uint64
		err := ytClient.GetNode(context.Background(), pathToDynAttr, &rowCount, nil)
		if err != nil {
			return 0, xerrors.Errorf("unable to get node %s:%w", pathToDynAttr, err)
		}
		return rowCount, nil
	}
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	return s.ytClient.NodeExists(context.Background(), ytprovider.SafeChild(ypath.Path(s.path), makeTableName(table)), nil)
}

func NewStorage(config *ytprovider.YtStorageParams) (*Storage, error) {
	ytConfig := yt.Config{
		Proxy:                 config.Cluster,
		Logger:                nil,
		Token:                 config.Token,
		AllowRequestsFromJob:  true,
		DisableProxyDiscovery: config.DisableProxyDiscovery,
	}
	ytClient, err := ytclient.NewYtClientWrapper(ytclient.HTTP, nil, &ytConfig)
	if err != nil {
		return nil, err
	}

	return &Storage{
		path:     config.Path,
		ytClient: ytClient,
		logger:   logger.Log,
		config:   config.Spec,
	}, nil
}
