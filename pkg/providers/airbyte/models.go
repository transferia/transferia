//go:build !disable_airbyte_provider

package airbyte

import (
	"fmt"
	"sort"

	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/typesystem"
	"github.com/transferia/transferia/pkg/base"
	"go.ytsaurus.tech/library/go/core/log"
	"go.ytsaurus.tech/yt/go/schema"
)

type StreamState struct {
	StreamName      string   `json:"stream_name"`
	StreamNamespace string   `json:"stream_namespace"`
	CursorField     []string `json:"cursor_field"`
	Cursor          string   `json:"cursor,omitempty"`
}

func (s StreamState) TableID() abstract.TableID {
	return abstract.TableID{
		Namespace: s.StreamNamespace,
		Name:      s.StreamName,
	}
}

type LogRecord struct {
	Level   string `json:"level"`
	Message string `json:"message"`
}

func (r Record) TableID() abstract.TableID {
	return abstract.TableID{
		Namespace: r.Namespace,
		Name:      r.Stream,
	}
}

type DataObjects struct {
	iter    int
	catalog *Catalog
}

func (d *DataObjects) Next() bool {
	d.iter++
	return len(d.catalog.Streams) > d.iter
}

func (d *DataObjects) Err() error {
	return nil
}

func (d *DataObjects) Close() {
	d.iter = len(d.catalog.Streams)
}

func (d *DataObjects) Object() (base.DataObject, error) {
	return &DataObject{
		iter:   -1,
		stream: d.catalog.Streams[d.iter],
	}, nil
}

func (d *DataObjects) ToOldTableMap() (abstract.TableMap, error) {
	res := abstract.TableMap{}
	keys := map[string]bool{}
	for _, item := range d.catalog.Streams {
		for _, keyRow := range item.SourceDefinedPrimaryKey {
			for _, colName := range keyRow {
				keys[colName] = true
			}
		}
		res[item.TableID()] = abstract.TableInfo{
			EtaRow: 0,
			IsView: false,
			Schema: abstract.NewTableSchema(toSchema(item.ParsedJSONSchema(), keys)),
		}
	}
	return res, nil
}

func NewDataObjects(catalog *Catalog, filter base.DataObjectFilter) (*DataObjects, error) {
	if filter != nil {
		var filteredStream []Stream
		for _, stream := range catalog.Streams {
			incl, err := filter.IncludesID(stream.TableID())
			if err != nil {
				return nil, xerrors.Errorf("unable to check stream: %s filter: %w", stream.TableID(), err)
			}
			if incl {
				filteredStream = append(filteredStream, stream)
			}
		}
		catalog.Streams = filteredStream
	}
	return &DataObjects{
		iter:    -1,
		catalog: catalog,
	}, nil
}

type DataObject struct {
	iter   int
	stream Stream
}

func (d *DataObject) ToOldTableDescription() (*abstract.TableDescription, error) {
	desc := &abstract.TableDescription{
		Name:   d.stream.Name,
		Schema: d.stream.Namespace,
		Filter: "",
		EtaRow: 0,
		Offset: 0,
	}
	logger.Log.Info(fmt.Sprintf("Serialize %v", desc.ID()), log.Any("stream", d.stream))
	return desc, nil
}

func (d *DataObject) ToTablePart() (*abstract.TableDescription, error) {
	return d.ToOldTableDescription()
}

func (d *DataObject) Name() string {
	return d.stream.Fqtn()
}

func (d *DataObject) FullName() string {
	return d.stream.Fqtn()
}

func (d *DataObject) Next() bool {
	d.iter++
	return d.iter == 0
}

func (d *DataObject) Err() error {
	return nil
}

func (d *DataObject) Close() {
}

func (d *DataObject) Part() (base.DataObjectPart, error) {
	return d, nil
}

func (d *DataObject) ToOldTableID() (*abstract.TableID, error) {
	return &abstract.TableID{Namespace: d.stream.Namespace, Name: d.stream.Name}, nil
}

func toSchema(schema JSONSchema, keys map[string]bool) abstract.TableColumns {
	var res abstract.TableColumns
	for k, p := range schema.Properties {
		res = append(res, abstract.ColSchema{
			TableSchema:  "",
			TableName:    "",
			Path:         k,
			ColumnName:   k,
			DataType:     string(toTypeV1(p)),
			PrimaryKey:   keys[k],
			FakeKey:      false,
			Required:     false,
			Expression:   "",
			OriginalType: fmt.Sprintf("airbyte: %v", p.Type),
			Properties:   nil,
		})
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].ColumnName < res[j].ColumnName
	})
	return res
}

func toTypeV1(typ JSONProperty) schema.Type {
	if res, ok := typesystem.RuleFor(ProviderType).Source[typ.AirbyteType]; ok {
		return res
	}
	if res, ok := typesystem.RuleFor(ProviderType).Source[typ.Format]; ok {
		return res
	}
	for _, typ := range typ.Type {
		if res, ok := typesystem.RuleFor(ProviderType).Source[typ]; ok {
			return res
		}
	}
	return typesystem.RuleFor(ProviderType).Source[typesystem.RestPlaceholder]
}
