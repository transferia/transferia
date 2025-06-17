//go:build !disable_delta_provider

package types

import "fmt"

type DataType interface {
	Name() string
}

type AliaseDataType interface {
	Aliases() []string
}

type BinaryType struct{}

func (b *BinaryType) Name() string {
	return "binary"
}

type BooleanType struct{}

func (b *BooleanType) Name() string {
	return "boolean"
}

type ByteType struct{}

func (b *ByteType) Name() string {
	return "tinyint"
}

func (b *ByteType) Aliases() []string {
	return []string{"tinyint", "byte"}
}

type DateType struct{}

func (d *DateType) Name() string {
	return "date"
}

type DecimalType struct {
	Precision int `json:"precision,omitempty"`
	Scale     int `json:"scale,omitempty"`
}

func (d *DecimalType) Name() string {
	return "decimal"
}

func (d *DecimalType) JSON() string {
	return fmt.Sprintf("decimal(%d,%d)", d.Precision, d.Scale)
}

type DoubleType struct{}

func (d *DoubleType) Name() string {
	return "double"
}

type FloatType struct{}

func (f *FloatType) Name() string {
	return "float"
}

func (f *FloatType) Aliases() []string {
	return []string{f.Name(), "real"}
}

type IntegerType struct{}

func (i *IntegerType) Name() string {
	return "int"
}

func (i *IntegerType) Aliases() []string {
	return []string{i.Name(), "integer"}
}

type LongType struct{}

func (l *LongType) Name() string {
	return "bigint"
}

func (l *LongType) Aliases() []string {
	return []string{l.Name(), "long"}
}

type NullType struct{}

func (n *NullType) Name() string {
	return "null"
}

func (n *NullType) Aliases() []string {
	return []string{n.Name(), "void"}
}

type ShortType struct{}

func (s *ShortType) Name() string {
	return "smallint"
}

func (s *ShortType) Aliases() []string {
	return []string{s.Name(), "short"}
}

type StringType struct{}

func (s *StringType) Name() string {
	return "string"
}

type TimestampType struct{}

func (t *TimestampType) Name() string {
	return "timestamp"
}
