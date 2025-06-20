//go:build !disable_elastic_provider

package elastic

import (
	"encoding/json"
	"reflect"
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util/jsonx"
	"go.ytsaurus.tech/yt/go/schema"
)

func (s *Storage) readRowsAndPushByChunks(
	result *searchResponse,
	st time.Time,
	table abstract.TableDescription,
	chunkSize uint64,
	chunkByteSize uint64,
	pusher abstract.Pusher,
) error {
	partID := table.PartID()
	inflight := make([]abstract.ChangeItem, 0)
	globalIdx := uint64(0)
	byteSize := uint64(0)

	var schemaDescription *SchemaDescription
	if len(result.Hits.Hits) != 0 {
		currSchema, err := s.getSchema(table.Name)
		if err != nil {
			return xerrors.Errorf("failed to fetch schema, index: %s, err: %w", table.Name, err)
		}
		schemaDescription = currSchema
	}

	for len(result.Hits.Hits) != 0 {
		for _, doc := range result.Hits.Hits {
			names, values, err := extractColumnValues(schemaDescription, doc.Source, doc.ID)
			if err != nil {
				return xerrors.Errorf("failed to extract values, index: %s, _id: %s, err: %w", table.Name, doc.ID, err)
			}

			inflight = append(inflight, abstract.ChangeItem{
				CommitTime:   uint64(st.UnixNano()),
				Kind:         abstract.InsertKind,
				Schema:       table.Schema,
				Table:        table.Name,
				PartID:       partID,
				ColumnNames:  names,
				ColumnValues: values,
				TableSchema:  abstract.NewTableSchema(schemaDescription.Columns),
				OldKeys:      abstract.EmptyOldKeys(),
				Counter:      0,
				ID:           0,
				LSN:          0,
				TxID:         "",
				Query:        "",
				Size:         abstract.RawEventSize(uint64(len(doc.Source))),
			})
			globalIdx++
			byteSize += uint64(len(doc.Source))
			s.Metrics.ChangeItems.Inc()
			s.Metrics.Size.Add(int64(len(doc.Source)))

			if uint64(len(inflight)) >= chunkSize {
				if err := pusher(inflight); err != nil {
					return xerrors.Errorf("cannot push documents to the sink: %w", err)
				}
				byteSize = 0
				inflight = make([]abstract.ChangeItem, 0)
			} else if byteSize > chunkByteSize {
				if err := pusher(inflight); err != nil {
					return xerrors.Errorf("cannot push documents (%d bytes, %d items) to the sink: %w", byteSize, len(inflight), err)
				}
				byteSize = 0
				inflight = make([]abstract.ChangeItem, 0)
			}
		}

		body, err := getResponseBody(s.Client.Scroll(
			s.Client.Scroll.WithScrollID(result.ScrollID),
			s.Client.Scroll.WithScroll(scrollDuration)))
		if err != nil {
			return xerrors.Errorf("unable to fetch documents, index: %s, err: %w", table.Name, err)
		}
		if err := jsonx.Unmarshal(body, &result); err != nil {
			return xerrors.Errorf("failed to unmarshal documents, index: %s, err: %w", table.Name, err)
		}
	}
	if len(inflight) > 0 {
		if err := pusher(inflight); err != nil {
			return xerrors.Errorf("cannot push last chunk (%d items) to the sink: %w", len(inflight), err)
		}
	}

	return nil
}

// extractColumnValues extracts the values contained in elasticsearch document based on the provided column schema.
// This method also checks that the extracted value is not an array type if this was not defined beforehand.
func extractColumnValues(schemaDescription *SchemaDescription, rawValues json.RawMessage, id string) ([]string, []interface{}, error) {
	var doc map[string]interface{}

	if err := jsonx.Unmarshal(rawValues, &doc); err != nil {
		return nil, nil, err
	}

	values := make([]interface{}, 0, len(doc))
	names := make([]string, 0, len(doc))
	for _, column := range schemaDescription.Columns {
		if column.ColumnName == idColumn {
			values = append(values, id)
			names = append(names, idColumn)
			continue
		}
		value, ok := doc[column.ColumnName]
		if !ok {
			continue
		}

		// possible array check for all non json fields
		if column.DataType != schema.TypeAny.String() {
			if reflect.TypeOf(value) != nil && ((reflect.TypeOf(value).Kind() == reflect.Slice) || (reflect.TypeOf(value).Kind() == reflect.Array)) {
				return nil, nil, xerrors.Errorf("invalid field type array for single value field detected")
			}
		}

		val, err := unmarshalField(value, &column)
		if err != nil {
			return nil, nil, xerrors.Errorf("failed to unmarshal a value: %w", err)
		}
		names = append(names, column.ColumnName)
		values = append(values, val)
	}
	return names, values, nil
}
