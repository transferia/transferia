//go:build !disable_postgres_provider

package postgres

import (
	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/library/go/core/xerrors"
)

type textCoder interface {
	pgtype.Value
	pgtype.TextEncoder
	pgtype.TextDecoder
}

type binaryCoder interface {
	pgtype.Value
	pgtype.BinaryEncoder
	pgtype.BinaryDecoder
}

// These are adapters for pgtype.ValueTranscoder interface, which are included
// into the pgtype.CompositeType as its fields. We need those since
// pgtype.CompositeType requires all of its fields to implement both text and
// binary serialization, while some types, namely pgtype.GenericText and
// pgtype.GenericBinary, are text-only and binary-only and cannot be used as a
// field of pgtype.CompositeType.
//
// To work that around, we store transcoderAdapterText and
// transcoderAdapterBinary values for text-only and binary-only types,
// respectively. Since we only use binary->binary (de)serialization of
// pgtype.GenericBinary in homogeneous pg->pg, and only text->text
// (de)serialization in heterogeneous pg->whatever, we only need to implement
// one of the two encoding methods.
//
// Both transcoderAdapterText and transcoderAdapterBinary also implement
// pgtype.ValueType interface. This is necessary because of NewTypeValue
// method: we cannot create a usable new instance of the adapter type using
// pgtype.NewValue without implementing that method. Unfortunately, pgtype's
// interfaces are not granular enough, and we also need to implement empty
// TypeName() method which is actually unused by the pgtype.

type (
	transcoderAdapterText   struct{ textCoder }
	transcoderAdapterBinary struct{ binaryCoder }
)

func (a *transcoderAdapterText) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) (newBuf []byte, err error) {
	return nil, xerrors.New("not implemented")
}

func (a *transcoderAdapterText) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	return xerrors.New("not implemented")
}

func (a *transcoderAdapterText) NewTypeValue() pgtype.Value {
	newTextCoder := pgtype.NewValue(a.textCoder)
	return &transcoderAdapterText{textCoder: newTextCoder.(textCoder)}
}

func (a *transcoderAdapterText) TypeName() string {
	return "" // unused anyway
}

func (a *transcoderAdapterBinary) EncodeText(ci *pgtype.ConnInfo, buf []byte) (newBuf []byte, err error) {
	return nil, xerrors.New("not implemented")
}

func (a *transcoderAdapterBinary) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	return xerrors.New("not implemented")
}

func (a *transcoderAdapterBinary) NewTypeValue() pgtype.Value {
	newBinaryCoder := pgtype.NewValue(a.binaryCoder)
	return &transcoderAdapterBinary{binaryCoder: newBinaryCoder.(binaryCoder)}
}

func (a *transcoderAdapterBinary) TypeName() string {
	return "" // unused anyway
}

func makeValueTranscoders(connInfo *pgtype.ConnInfo, fields []pgtype.CompositeTypeField) (transcoders []pgtype.ValueTranscoder, err error) {
	for _, field := range fields {
		dataType, ok := connInfo.DataTypeForOID(field.OID)
		if !ok {
			return nil, xerrors.Errorf("unknown data type for OID %d (field %s)", field.OID, field.Name)
		}
		value := pgtype.NewValue(dataType.Value)
		if transcoder, ok := value.(pgtype.ValueTranscoder); ok {
			transcoders = append(transcoders, transcoder)
			continue
		}
		if textCoder, ok := value.(textCoder); ok {
			transcoders = append(transcoders, &transcoderAdapterText{textCoder: textCoder})
			continue
		}
		if binaryCoder, ok := value.(binaryCoder); ok {
			transcoders = append(transcoders, &transcoderAdapterBinary{binaryCoder: binaryCoder})
			continue
		}
		return nil, xerrors.Errorf("type %T does not implement any encoding interfaces", value)
	}
	return transcoders, nil
}
