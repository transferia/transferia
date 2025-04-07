package action

import (
	"time"

	"github.com/google/uuid"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/providers/delta/types"
	"github.com/transferia/transferia/pkg/util/set"
)

type Metadata struct {
	ID               string            `json:"id,omitempty"`
	Name             string            `json:"name,omitempty"`
	Description      string            `json:"description,omitempty"`
	Format           Format            `json:"format,omitempty"`
	SchemaString     string            `json:"schemaString,omitempty"`
	PartitionColumns []string          `json:"partitionColumns,omitempty"`
	Configuration    map[string]string `json:"configuration,omitempty"`
	CreatedTime      *int64            `json:"createdTime,omitempty"`
}

func DefaultMetadata() *Metadata {
	now := time.Now().UnixMilli()

	return &Metadata{
		ID:               uuid.New().String(),
		Name:             "",
		Description:      "",
		Format:           Format{Provider: "parquet", Options: map[string]string{}},
		SchemaString:     "",
		PartitionColumns: nil,
		Configuration:    map[string]string{},
		CreatedTime:      &now,
	}
}

func (m *Metadata) Wrap() *Single {
	res := new(Single)
	res.MetaData = m
	return res
}

func (m *Metadata) JSON() (string, error) {
	return jsonString(m)
}

func (m *Metadata) Schema() (*types.StructType, error) {
	if len(m.SchemaString) == 0 {
		return types.NewStructType(make([]*types.StructField, 0)), nil
	}

	if dt, err := types.FromJSON(m.SchemaString); err != nil {
		return nil, err
	} else {
		return dt.(*types.StructType), nil
	}
}

func (m *Metadata) PartitionSchema() (*types.StructType, error) {
	schema, err := m.Schema()
	if err != nil {
		return nil, xerrors.Errorf("unable to extract part schema: %w", err)
	}

	var fields []*types.StructField
	for _, c := range m.PartitionColumns {
		if f, err := schema.Get(c); err != nil {
			return nil, xerrors.Errorf("unable to get col: %s: %w", c, err)
		} else {
			fields = append(fields, f)
		}
	}
	return types.NewStructType(fields), nil
}

func (m *Metadata) DataSchema() (*types.StructType, error) {
	partitions := set.New(m.PartitionColumns...)
	s, err := m.Schema()
	if err != nil {
		return nil, err
	}

	fields := yslices.Filter(s.GetFields(), func(f *types.StructField) bool {
		return !partitions.Contains(f.Name)
	})

	return types.NewStructType(fields), nil
}
