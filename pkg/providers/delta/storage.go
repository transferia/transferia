package delta

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/parquet-go/parquet-go"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/format"
	"github.com/transferia/transferia/pkg/providers/delta/protocol"
	"github.com/transferia/transferia/pkg/providers/delta/store"
	"github.com/transferia/transferia/pkg/providers/delta/types"
	s3_source "github.com/transferia/transferia/pkg/providers/s3"
	"github.com/transferia/transferia/pkg/providers/s3/pusher"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

// To verify providers contract implementation
var (
	_ abstract.Storage = (*Storage)(nil)
)

// defaultReadBatchSize is magic number by in-leskin
// we need to push rather small chunks so our bufferer can buffer effectively
const defaultReadBatchSize = 128

type Storage struct {
	cfg         *DeltaSource
	client      s3iface.S3API
	reader      *s3_reader.ReaderParquet
	logger      log.Logger
	table       *protocol.TableLog
	snapshot    *protocol.Snapshot
	tableSchema *abstract.TableSchema
	colNames    []string
	registry    metrics.Registry
}

func (s *Storage) Ping() error {
	return nil
}

func (s *Storage) TableSchema(ctx context.Context, table abstract.TableID) (*abstract.TableSchema, error) {
	return s.tableSchema, nil
}

func (s *Storage) LoadTable(ctx context.Context, table abstract.TableDescription, abstractPusher abstract.Pusher) error {
	if table.Filter == "" {
		return xerrors.Errorf("delta lake works only with enabled filter: %s", table.ID().String())
	}

	pusher := pusher.New(abstractPusher, nil, s.logger, 0)
	return s.reader.Read(ctx, fmt.Sprintf("%s/%s", s.cfg.PathPrefix, string(table.Filter)), pusher)
}

func (s *Storage) TableList(_ abstract.IncludeTableList) (abstract.TableMap, error) {
	if err := s.ensureSnapshot(); err != nil {
		return nil, xerrors.Errorf("unable to ensure snapshot: %w", err)
	}
	return map[abstract.TableID]abstract.TableInfo{
		*abstract.NewTableID(s.cfg.TableNamespace, s.cfg.TableName): {
			EtaRow: 0,
			IsView: false,
			Schema: s.tableSchema,
		},
	}, nil
}

func (s *Storage) asTableSchema(typ *types.StructType) *abstract.TableSchema {
	var res []abstract.ColSchema
	if !s.cfg.HideSystemCols {
		res = append(res, abstract.NewColSchema("__delta_file_name", schema.TypeString, true))
		res = append(res, abstract.NewColSchema("__delta_row_index", schema.TypeUint64, true))
	}
	for _, f := range typ.Fields {
		jsonType, _ := types.ToJSON(f.DataType)
		res = append(res, abstract.ColSchema{
			TableSchema:  "",
			TableName:    "",
			Path:         "",
			ColumnName:   f.Name,
			DataType:     mapDataType(f.DataType).String(),
			PrimaryKey:   false,
			FakeKey:      false,
			Required:     !f.Nullable,
			Expression:   "",
			OriginalType: fmt.Sprintf("delta:%s", jsonType),
			Properties:   nil,
		})
	}
	return abstract.NewTableSchema(res)
}

func mapDataType(dataType types.DataType) schema.Type {
	if dtType, ok := typesystem.RuleFor(ProviderType).Source[dataType.Name()]; ok {
		return dtType
	}
	return schema.TypeAny
}

func (s *Storage) ExactTableRowsCount(_ abstract.TableID) (uint64, error) {
	if err := s.ensureSnapshot(); err != nil {
		return 0, xerrors.Errorf("unable to ensure snapshot: %w", err)
	}
	files, err := s.snapshot.AllFiles()
	if err != nil {
		return 0, xerrors.Errorf("unable to load file list: %w", err)
	}
	totalByteSize := int64(0)
	totalRowCount := int64(0)
	for _, file := range files {
		totalByteSize += file.Size
		filePath := fmt.Sprintf("%s/%s", s.cfg.PathPrefix, file.Path)
		sr, err := s3raw.NewS3RawReader(context.TODO(), s.client, nil, s.cfg.Bucket, filePath, stats.NewSourceStats(s.registry))
		if err != nil {
			return 0, xerrors.Errorf("unable to create reader at: %w", err)
		}
		pr := parquet.NewReader(sr)
		defer pr.Close()
		totalRowCount += pr.NumRows()
	}
	s.logger.Infof("extract total row count: %d in %d files with total size: %s", totalRowCount, len(files), format.SizeUInt64(uint64(totalByteSize)))
	return uint64(totalRowCount), nil
}

func (s *Storage) EstimateTableRowsCount(table abstract.TableID) (uint64, error) {
	return s.ExactTableRowsCount(table)
}

func (s *Storage) TableExists(table abstract.TableID) (bool, error) {
	return s.table.TableExists(), nil
}

func (s *Storage) Close() {}

func NewStorage(cfg *DeltaSource, lgr log.Logger, registry metrics.Registry) (*Storage, error) {
	sess, err := s3_source.NewAWSSession(lgr, cfg.Bucket, cfg.ConnectionConfig())
	if err != nil {
		return nil, xerrors.Errorf("unable to init aws session: %w", err)
	}
	st, err := store.New(&store.S3Config{
		Endpoint:         cfg.Endpoint,
		TablePath:        cfg.PathPrefix,
		Region:           cfg.Region,
		AccessKey:        cfg.AccessKey,
		S3ForcePathStyle: cfg.S3ForcePathStyle,
		Secret:           string(cfg.SecretKey),
		Bucket:           cfg.Bucket,
		UseSSL:           cfg.UseSSL,
		VerifySSL:        cfg.VersifySSL,
	})
	if err != nil {
		return nil, xerrors.Errorf("unable to init s3 delta protocol store: %w", err)
	}
	table, err := protocol.NewTableLog(cfg.PathPrefix, st)
	if err != nil {
		return nil, xerrors.Errorf("unable to load delta table: %w", err)
	}

	s3Source := new(s3_source.S3Source)
	s3Source.ConnectionConfig = s3_source.ConnectionConfig{
		Endpoint:         cfg.Endpoint,
		Region:           cfg.Region,
		AccessKey:        cfg.AccessKey,
		S3ForcePathStyle: cfg.S3ForcePathStyle,
		SecretKey:        cfg.SecretKey,
		UseSSL:           cfg.UseSSL,
		VerifySSL:        cfg.VersifySSL,
		ServiceAccountID: "",
	}
	s3Source.Bucket = cfg.Bucket
	s3Source.TableName = cfg.TableName
	s3Source.TableNamespace = cfg.TableNamespace
	s3Source.PathPrefix = cfg.PathPrefix
	s3Source.ReadBatchSize = defaultReadBatchSize
	s3Source.HideSystemCols = cfg.HideSystemCols

	reader, err := s3_reader.NewParquet(s3Source, lgr, sess, stats.NewSourceStats(registry))
	if err != nil {
		return nil, xerrors.Errorf("unable to initialize parquet reader: %w", err)
	}
	return &Storage{
		cfg:         cfg,
		client:      s3.New(sess),
		logger:      lgr,
		reader:      reader,
		table:       table,
		snapshot:    nil,
		tableSchema: nil,
		colNames:    nil,
		registry:    registry,
	}, nil
}
