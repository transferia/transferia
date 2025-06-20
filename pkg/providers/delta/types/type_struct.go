//go:build !disable_delta_provider

package types

import (
	"github.com/transferia/transferia/library/go/core/xerrors"
)

type StructType struct {
	Fields      []*StructField
	nameToField map[string]*StructField
}

func (s *StructType) Name() string {
	return "struct"
}

func (s *StructType) FieldNames() []string {
	res := make([]string, len(s.Fields))
	for i, f := range s.Fields {
		res[i] = f.Name
	}
	return res
}

func (s *StructType) Length() int {
	return len(s.Fields)
}

func (s *StructType) Get(fieldName string) (*StructField, error) {
	v, ok := s.nameToField[fieldName]
	if !ok {
		return nil, xerrors.Errorf("Field %s does not exist.", fieldName)
	}
	return v, nil
}

func (s *StructType) Add(field *StructField) *StructType {
	newFields := make([]*StructField, len(s.Fields)+1)
	copy(newFields, s.Fields)
	newFields[len(newFields)-1] = field
	return NewStructType(newFields)
}

func (s *StructType) Add2(fieldName string, dt DataType) *StructType {
	return s.Add(NewStructField(fieldName, dt, true))
}

func (s *StructType) Add3(fieldName string, dt DataType, nullable bool) *StructType {
	return s.Add(NewStructField(fieldName, dt, nullable))
}

func (s *StructType) GetFields() []*StructField {
	newFields := make([]*StructField, len(s.Fields))
	copy(newFields, s.Fields)
	return newFields
}

func NewStructType(fields []*StructField) *StructType {
	s := &StructType{
		Fields:      fields,
		nameToField: make(map[string]*StructField),
	}

	for _, f := range fields {
		s.nameToField[f.Name] = f
	}
	return s
}

type StructField struct {
	Name     string
	DataType DataType
	Nullable bool

	// a map is used for metadata, be aware of all the numbers are marshalled as float64
	// Note: Java version only supports Long(array)/Double(array)/Bool(array)/String(array)/Map type, but we do not check them explicitly.
	Metadata map[string]interface{}
}

func NewStructField(name string, t DataType, nullable bool) *StructField {
	return &StructField{
		Name:     name,
		DataType: t,
		Nullable: nullable,
		Metadata: make(map[string]interface{}),
	}
}
