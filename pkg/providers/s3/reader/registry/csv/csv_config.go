package csv

import (
	"context"
	"fmt"
	"io"

	aws_session "github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3/s3iface"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	dt_csv "github.com/transferia/transferia/pkg/csv"
	s3_model "github.com/transferia/transferia/pkg/providers/s3/model"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/transferia/transferia/pkg/stats"
	"go.ytsaurus.tech/library/go/core/log"
)

// csvConfig holds S3 transport, logger, and CSV parser settings built from S3Source.
// It is shared between CSVReader (row reading) and CSVSchemaResolver only as duplicated configuration
// from the same source at construction time — there is no shared runtime object between them.
type csvConfig struct {
	s3RawReaderBuilder      s3raw.S3RawReaderBuilder
	bucket                  string
	client                  s3iface.S3API
	logger                  log.Logger
	hideSystemCols          bool
	blockSize               int64
	pathPrefix              string
	delimiter               rune
	quoteChar               rune
	escapeChar              rune
	encoding                string
	doubleQuote             bool
	newlinesInValue         bool
	additionalReaderOptions s3_model.AdditionalOptions
	advancedOptions         s3_model.AdvancedOptions
	headerPresent           bool
	pathPattern             string
	metrics                 *stats.SourceStats
	unparsedPolicy          s3_model.UnparsedPolicy
}

// newCSVConfigFromSource builds runtime dependencies and optional tableSchema schema from output schema.
func newCSVConfigFromSource(
	src *s3_model.S3Source,
	lgr log.Logger,
	sess *aws_session.Session,
	s3RawReaderBuilder s3raw.S3RawReaderBuilder,
	metrics *stats.SourceStats,
) (*csvConfig, *abstract.TableSchema, error) {
	if src == nil || src.Format.CSVSetting == nil {
		return nil, nil, xerrors.Errorf("s3 source must not be nil")
	}
	csvSettings := src.Format.CSVSetting

	if len(csvSettings.Delimiter) != 1 {
		return nil, nil, xerrors.Errorf("csv delimiter must have exactly one delimiter")
	}

	var delimiter rune
	var escapeChar rune
	var quoteChar rune

	delimiter = []rune(csvSettings.Delimiter)[0]
	if len(csvSettings.QuoteChar) != 0 {
		quoteChar = []rune(csvSettings.QuoteChar)[0]
	}
	if len(csvSettings.EscapeChar) != 0 {
		escapeChar = []rune(csvSettings.EscapeChar)[0]
	}

	config := &csvConfig{
		s3RawReaderBuilder:      s3RawReaderBuilder,
		bucket:                  src.Bucket,
		client:                  s3RawReaderBuilder.BuildClient(sess),
		logger:                  lgr,
		hideSystemCols:          src.HideSystemCols,
		blockSize:               csvSettings.BlockSize,
		pathPrefix:              src.PathPrefix,
		delimiter:               delimiter,
		quoteChar:               quoteChar,
		escapeChar:              escapeChar,
		encoding:                csvSettings.Encoding,
		doubleQuote:             csvSettings.DoubleQuote,
		newlinesInValue:         csvSettings.NewlinesInValue,
		additionalReaderOptions: csvSettings.AdditionalReaderOptions,
		advancedOptions:         csvSettings.AdvancedOptions,
		headerPresent:           false,
		pathPattern:             src.PathPattern,
		metrics:                 metrics,
		unparsedPolicy:          src.UnparsedPolicy,
	}

	tableSchema := abstract.NewTableSchema(src.OutputSchema)
	if len(tableSchema.Columns()) == 0 {
		if len(config.advancedOptions.ColumnNames) == 0 && !config.advancedOptions.AutogenerateColumnNames {
			config.headerPresent = true
		}
		return config, nil, nil
	}

	var cols []abstract.ColSchema
	for index, col := range tableSchema.Columns() {
		if col.Path == "" {
			col.Path = fmt.Sprintf("%d", index)
		}
		if col.OriginalType == "" {
			col.OriginalType = fmt.Sprintf("csv:%s", col.DataType)
		}
		cols = append(cols, col)
	}
	tableSchema = abstract.NewTableSchema(cols)

	if !config.hideSystemCols {
		cols := tableSchema.Columns()
		userDefinedSchemaHasPkey := tableSchema.Columns().HasPrimaryKey()
		tableSchema = s3_reader.AppendSystemColsTableSchema(cols, !userDefinedSchemaHasPkey)
	}

	return config, tableSchema, nil
}

func (d *csvConfig) newS3RawReader(ctx context.Context, filePath string) (s3raw.S3RawReader, error) {
	s3RawReader, err := s3raw.CoalesceS3RawReaderBuilder(d.s3RawReaderBuilder).BuildReader(ctx, d.client, d.bucket, filePath, d.metrics)
	if err != nil {
		return nil, xerrors.Errorf("unable to build s3 raw reader in file: %s, err: %w", filePath, err)
	}
	return s3RawReader, nil
}

func (d *csvConfig) newCSVReaderFromReader(reader io.Reader) *dt_csv.Reader {
	csvReader := dt_csv.NewReader(reader)
	csvReader.NewlinesInValue = d.newlinesInValue
	csvReader.QuoteChar = d.quoteChar
	csvReader.EscapeChar = d.escapeChar
	csvReader.Encoding = d.encoding
	csvReader.Delimiter = d.delimiter
	csvReader.DoubleQuote = d.doubleQuote
	csvReader.DoubleQuoteStr = fmt.Sprintf("%s%s", string(d.quoteChar), string(d.quoteChar))

	return csvReader
}
