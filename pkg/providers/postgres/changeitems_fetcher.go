//go:build !disable_postgres_provider

package postgres

import (
	"io"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

// ChangeItemsFetcher consolidates multiple objects (of which most important is pgx.Rows) into a single one, providing bufferized fetching and parsing of rows from a postgres source
type ChangeItemsFetcher struct {
	rows             pgx.Rows
	connInfo         *pgtype.ConnInfo
	template         abstract.ChangeItem
	parseSchema      abstract.TableColumns
	sourceStats      *stats.SourceStats
	unmarshallerData UnmarshallerData
	limitCount       int
	limitBytes       model.BytesSize
	maybeHasMore     bool
	logger           log.Logger
}

// NewChangeItemsFetcher constructs minimal working fetcher.
// ChangeItems fetched are shallow copies of the template except for ColumnValues - these are filled by the fetcher.
// The sequence of ColumnValues, ColSchemas (in TableSchema) and rows.FieldDescriptions() MUST BE THE SAME.
func NewChangeItemsFetcher(rows pgx.Rows, conn *pgx.Conn, template abstract.ChangeItem, sourceStats *stats.SourceStats) *ChangeItemsFetcher {
	return &ChangeItemsFetcher{
		rows:             rows,
		connInfo:         conn.ConnInfo(),
		template:         template,
		parseSchema:      template.TableSchema.Columns().Copy(),
		sourceStats:      sourceStats,
		unmarshallerData: MakeUnmarshallerData(false, conn),
		limitCount:       128,
		limitBytes:       16 * humanize.MiByte,
		maybeHasMore:     true,
		logger:           logger.Log,
	}
}

func (f *ChangeItemsFetcher) WithUnmarshallerData(data UnmarshallerData) *ChangeItemsFetcher {
	f.unmarshallerData = data
	return f
}

func (f *ChangeItemsFetcher) WithLimitCount(limitCount int) *ChangeItemsFetcher {
	f.limitCount = limitCount
	return f
}

// WithLimitBytes accepts 0 to disable limit
func (f *ChangeItemsFetcher) WithLimitBytes(limitBytes model.BytesSize) *ChangeItemsFetcher {
	f.limitBytes = limitBytes
	return f
}

func (f *ChangeItemsFetcher) WithLogger(logger log.Logger) *ChangeItemsFetcher {
	f.logger = logger
	return f
}

func (f *ChangeItemsFetcher) MaybeHasMore() bool {
	return f.maybeHasMore
}

func (f *ChangeItemsFetcher) Fetch() (items []abstract.ChangeItem, err error) {
	result := make([]abstract.ChangeItem, f.limitCount)
	resultCount := 0
	resultBytes := model.BytesSize(0)

	casters := make([]*Unmarshaller, len(f.rows.FieldDescriptions())) // slice is much faster than map for some reason
	for i, fd := range f.rows.FieldDescriptions() {
		caster, err := NewUnmarshaller(&f.unmarshallerData, f.connInfo, &f.parseSchema[i], &fd)
		if err != nil {
			return nil, xerrors.Errorf("failed to construct a parser of raw PostgreSQL-format data for field [%d]: %w", i, err)
		}
		casters[i] = caster
	}

	for f.rows.Next() {
		ciResult := &result[resultCount]
		*ciResult = f.template

		ciResult.ColumnValues = make([]interface{}, len(ciResult.TableSchema.Columns()))

		decodeStartT := time.Now()
		rowRawSize := uint64(0)

		rawData := f.rows.RawValues()
		for i := range f.rows.FieldDescriptions() {
			fRawValue := rawData[i]

			resultBytes += model.BytesSize(len(fRawValue))
			rowRawSize += uint64(len(fRawValue))

			ciResult.ColumnValues[i], err = casters[i].Cast(fRawValue)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse the value of field [%d] from PostgreSQL format: %w", i, err)
			}
		}

		f.sourceStats.Size.Add(int64(rowRawSize))
		f.sourceStats.DecodeTime.RecordDuration(time.Since(decodeStartT))

		ciResult.Size.Read = rowRawSize

		resultCount += 1
		f.sourceStats.Parsed.Inc()

		if resultCount >= f.limitCount {
			f.logger.Debugf("Finishing chunk of %d ChangeItems (%s) due to buffer elements' count limit (%d)", resultCount, humanize.Bytes(uint64(resultBytes)), f.limitCount)
			return result[:resultCount], nil
		}
		if f.limitBytes > 0 && resultBytes >= f.limitBytes {
			f.logger.Debugf("Finishing chunk of %d ChangeItems (%s) due to buffer size limit (%s)", resultCount, humanize.Bytes(uint64(resultBytes)), humanize.Bytes(uint64(f.limitBytes)))
			return result[:resultCount], nil
		}
	}
	if err := f.rows.Err(); err != nil {
		f.maybeHasMore = false
		if xerrors.Is(err, io.ErrUnexpectedEOF) {
			err = xerrors.Errorf("connection closed unexpectedly: %w", err)
		}
		return nil, xerrors.Errorf("failed while reading data from source: %w", err)
	}

	f.logger.Debugf("Finishing chunk of %d ChangeItems (%s) due to exhaustion of data in result set", resultCount, humanize.Bytes(uint64(resultBytes)))
	f.maybeHasMore = false
	return result[:resultCount], nil
}
