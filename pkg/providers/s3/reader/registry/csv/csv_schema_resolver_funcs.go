package csv

import (
	"bytes"
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	dt_csv "github.com/transferia/transferia/pkg/csv"
	s3_reader "github.com/transferia/transferia/pkg/providers/s3/reader"
	"github.com/transferia/transferia/pkg/providers/s3/reader/reader_error"
	"github.com/transferia/transferia/pkg/providers/s3/reader/s3raw"
	"github.com/valyala/fastjson"
	ytschema "go.ytsaurus.tech/yt/go/schema"
)

// csvInferTableSchemaFromSample infers schema from a sample of the given object.
//
//nolint:descriptiveerrors
func csvInferTableSchemaFromSample(ctx context.Context, d *csvConfig, key string) (*abstract.TableSchema, reader_error.ReaderError) {
	buff, rerr := s3_reader.ReadSchemaInferenceFirstChunk(
		ctx,
		key,
		d.pathPrefix,
		int(d.blockSize),
		d.logger,
		func() (s3raw.S3RawReader, error) { return d.newS3RawReader(ctx, key) },
		"csv.parser.resolveSchemaData.newS3RawReader",
		"chunk.ReadNextChunk",
		"csv.empty_sample",
	)
	if rerr != nil {
		return nil, rerr
	}
	if len(buff) == 0 {
		return abstract.NewTableSchema(nil), nil
	}

	csvReader := d.newCSVReaderFromReader(bytes.NewReader(buff))

	allColNames, err := csvSchemaGetColumnNames(d, csvReader, key)
	if err != nil {
		return nil, reader_error.NewReaderErrorDataSchema("csv.getColumnNames", key, err)
	}

	filteredColNames, err := csvSchemaFilterColNames(d, allColNames, key)
	if err != nil {
		return nil, reader_error.NewReaderErrorDataSchema("csv.filterColNames", key, err)
	}

	currSchema, err := csvSchemaGetColumnTypes(d, filteredColNames, csvReader, key)
	if err != nil {
		return nil, reader_error.NewReaderErrorDataSchema("csv.getColumnTypes", key, err)
	}

	return abstract.NewTableSchema(currSchema), nil
}

func csvSchemaGetColumnTypes(d *csvConfig, columns []abstract.ColSchema, csvReader *dt_csv.Reader, sampleKey string) ([]abstract.ColSchema, error) {
	readAfter := d.advancedOptions.SkipRowsAfterNames
	elements, err := readAfterNRows(readAfter, csvReader, sampleKey)
	if err != nil {
		return nil, xerrors.Errorf("unable to readAfterNRows, err: %w", err)
	}

	var colsWithSchema []abstract.ColSchema

	for _, col := range columns {
		index, err := strconv.Atoi(col.Path)
		if err != nil {
			return nil, xerrors.Errorf("unable to readAfterNRows column index, err: %w", err)
		}

		if index >= len(elements) {
			return nil, xerrors.Errorf("index out of bounds: %d, column: %s", index, col.Path)
		}

		var val string
		if index < 0 {
			val = ""
		} else {
			val = elements[index]
		}

		dataType := csvSchemaDeduceDataType(d, val)
		column := abstract.NewColSchema(col.ColumnName, dataType, false)
		column.OriginalType = fmt.Sprintf("csv:%s", dataType.String())
		column.Path = col.Path

		colsWithSchema = append(colsWithSchema, column)
	}

	return colsWithSchema, nil
}

func csvSchemaDeduceDataType(d *csvConfig, val string) ytschema.Type {
	if val == "" {
		return ytschema.TypeString
	}
	if strings.Contains(val, string('`')) || strings.Contains(val, string('"')) {
		if d.additionalReaderOptions.QuotedStringsCanBeNull {
			if yslices.Contains(d.additionalReaderOptions.NullValues, val) {
				return ytschema.TypeString
			}
		}
		if err := fastjson.Validate(val); err == nil && (strings.Contains(val, "{") || strings.Contains(val, "[")) {
			return ytschema.TypeAny
		}
		return ytschema.TypeString

	}
	if d.additionalReaderOptions.StringsCanBeNull && yslices.Contains(d.additionalReaderOptions.NullValues, val) {
		return ytschema.TypeString
	}
	if yslices.Contains(d.additionalReaderOptions.FalseValues, val) || yslices.Contains(d.additionalReaderOptions.TrueValues, val) {
		return ytschema.TypeBoolean
	}
	if d.additionalReaderOptions.DecimalPoint != "" {
		possibleNumber := strings.Replace(val, d.additionalReaderOptions.DecimalPoint, ".", 1)

		_, err := strconv.ParseFloat(possibleNumber, 64)
		if err == nil {
			return ytschema.TypeFloat64
		}
	}
	_, err := strconv.ParseFloat(val, 64)
	if err == nil {
		return ytschema.TypeFloat64
	}

	return ytschema.TypeString
}

func csvSchemaGetColumnNames(d *csvConfig, csvReader *dt_csv.Reader, sampleKey string) ([]string, error) {
	var columnNames []string

	if len(d.advancedOptions.ColumnNames) != 0 {
		columnNames = append(columnNames, d.advancedOptions.ColumnNames...)
	} else if len(d.advancedOptions.ColumnNames) == 0 && d.advancedOptions.AutogenerateColumnNames {
		elements, err := readAfterNRows(d.advancedOptions.SkipRows, csvReader, sampleKey)
		if err != nil {
			return nil, xerrors.Errorf("unable to readAfterNRows, err: %w", err)
		}
		for i := range elements {
			columnNames = append(columnNames, fmt.Sprintf("f%d", i))
		}
	}

	if len(columnNames) == 0 {
		readAfter := d.advancedOptions.SkipRows
		elements, err := readAfterNRows(readAfter, csvReader, sampleKey)
		if err != nil {
			return nil, xerrors.Errorf("unable to readAfterNRows, err: %w", err)
		}
		columnNames = append(columnNames, elements...)
	}

	return columnNames, nil
}

func csvSchemaFilterColNames(d *csvConfig, colNames []string, sampleKey string) ([]abstract.ColSchema, error) {
	var cols []abstract.ColSchema
	if len(d.additionalReaderOptions.IncludeColumns) != 0 {
		for _, name := range d.additionalReaderOptions.IncludeColumns {
			contained := false
			atIndex := -1
			for index, element := range colNames {
				if element == name {
					contained = true
					atIndex = index
					break
				}
			}

			if !contained && !d.additionalReaderOptions.IncludeMissingColumns {
				return nil, xerrors.Errorf("unable to read sample data from file: %s", sampleKey)
			}
			column := abstract.NewColSchema(name, ytschema.TypeAny, false)
			column.Path = strconv.Itoa(atIndex)
			cols = append(cols, column)
		}
	} else {
		for index, name := range colNames {
			column := abstract.NewColSchema(name, ytschema.TypeAny, false)
			column.Path = strconv.Itoa(index)
			cols = append(cols, column)
		}
	}

	return cols, nil
}
