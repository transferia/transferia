package packer

import (
	"github.com/transferria/transferria/pkg/abstract"
)

type PackerSkipSchema struct {
}

func (*PackerSkipSchema) GetSchemaIDResolver() SchemaIDResolver { return nil }

func (s *PackerSkipSchema) Pack(
	changeItem *abstract.ChangeItem,
	payloadBuilder BuilderFunc,
	_ BuilderFunc,
	_ []byte,
) ([]byte, error) {
	return payloadBuilder(changeItem)
}

func (s *PackerSkipSchema) BuildFinalSchema(_ *abstract.ChangeItem, _ BuilderFunc) ([]byte, error) {
	return nil, nil
}

func (s *PackerSkipSchema) IsDropSchema() bool {
	return true
}

func NewPackerSkipSchema() *PackerSkipSchema {
	return &PackerSkipSchema{}
}
