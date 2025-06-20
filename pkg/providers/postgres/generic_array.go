//go:build !disable_postgres_provider

package postgres

import (
	"database/sql/driver"
	"encoding/binary"
	"encoding/json"
	"net"

	"github.com/gofrs/uuid"
	"github.com/jackc/pgio"
	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/util"
	"go.ytsaurus.tech/library/go/core/log"
)

// GenericArray is based on ArrayType https://github.com/transferia/transferia/arc_vcs/vendor/github.com/jackc/pgtype/array_type.go?rev=r8263453#L15
type GenericArray struct {
	elements   []pgtype.Value
	dimensions []pgtype.ArrayDimension
	status     pgtype.Status

	typeName   string
	elementOID pgtype.OID
	newElement func() (pgtype.Value, error)
	logger     log.Logger
	source     []byte // fallback if unpack can't be done
}

func textDecoder(element interface{}) (pgtype.TextDecoder, error) {
	if textDecoder, ok := element.(pgtype.TextDecoder); ok {
		return textDecoder, nil
	}
	return nil, xerrors.Errorf("array element '%T' does not support text decoder interface", element)
}

func binaryDecoder(element interface{}) (pgtype.BinaryDecoder, error) {
	if binaryDecoder, ok := element.(pgtype.BinaryDecoder); ok {
		return binaryDecoder, nil
	}
	return nil, xerrors.Errorf("array element '%T' does not support binary decoder interface", element)
}

func textEncoder(element interface{}) (pgtype.TextEncoder, error) {
	if textEncoder, ok := element.(pgtype.TextEncoder); ok {
		return textEncoder, nil
	}
	return nil, xerrors.Errorf("array element '%T' does not support text encoder interface", element)
}

func binaryEncoder(element interface{}) (pgtype.BinaryEncoder, error) {
	if binaryEncoder, ok := element.(pgtype.BinaryEncoder); ok {
		return binaryEncoder, nil
	}
	return nil, xerrors.Errorf("array element '%T' does not support binary encoder interface", element)
}

func NewGenericArray(logger log.Logger, typeName string, elementOID pgtype.OID, newElement func() (pgtype.Value, error)) (*GenericArray, error) {
	_, err := newElement()
	if err != nil {
		return nil, xerrors.Errorf("unable to create new array element: %w", err)
	}

	result := &GenericArray{
		elements:   nil,
		dimensions: nil,
		status:     0,
		typeName:   typeName,
		elementOID: elementOID,
		newElement: newElement,
		logger:     logger,
		source:     nil,
	}
	return result, nil
}

func (ga *GenericArray) unpackTypes(val pgtype.Value, connInfo *pgtype.ConnInfo) interface{} {
	var unpVal interface{}
	if composite, ok := val.(*pgtype.CompositeType); !ok {
		unpVal = val.Get()
	} else {
		// Historically, composite types were represented as
		// *pgtype.GenericText, which returned string as a result of Get()
		// method. Currently composite types are represented as
		// *pgtype.CompositeType. Get() method for this type returns
		// map[string]interface{}, while we want the plain old text
		// representation to preserve the backward compatibility.
		buf, err := composite.EncodeText(connInfo, nil)
		if err != nil {
			logger.Log.Warn("unable to encode composite type to text", log.Error(err))
			return nil
		}
		if buf == nil {
			unpVal = nil
		} else {
			unpVal = string(buf)
		}
	}

	switch v := val.(type) {
	case *pgtype.UUID:
		var payload []byte
		if unpVal != nil {
			tmp := unpVal.([16]byte)
			payload = tmp[:16]
		}
		return uuid.FromBytesOrNil(payload).String()
	case *pgtype.Numeric:
		if v.Status == pgtype.Null {
			return nil
		}
		pV, _ := v.Value()
		if pV == nil {
			return nil
		}
		return json.Number(pV.(string))
	default:
		switch t := unpVal.(type) {
		case *net.IPNet:
			return t.String()
		default:
			return unpVal
		}
	}
}

func (ga *GenericArray) Unpack(connInfo *pgtype.ConnInfo) (*util.XDArray, error) {
	if ga.status == pgtype.Undefined {
		return nil, xerrors.New("value is undefined")
	}

	if ga.status == pgtype.Null {
		return nil, nil
	}

	if ga.dimensions == nil {
		return util.EmptyXDArray(), nil
	}

	size := make([]int, len(ga.dimensions))
	for i, d := range ga.dimensions {
		size[i] = int(d.Length)
	}

	xda, err := util.NewXDArray(size)
	if err != nil {
		return nil, xerrors.Errorf("unable to create new xd array: %w", err)
	}
	fullSize := util.FullSize(size)
	for i, x := range ga.elements {
		index := util.XDIndex(i, fullSize)
		value := ga.unpackTypes(x, connInfo)
		err := xda.Set(index, value)
		if err != nil {
			return nil, xerrors.Errorf("unable to set value: %w", err)
		}
	}
	return xda, nil
}

func (ga *GenericArray) NewTypeValue() pgtype.Value {
	return ga.NewTypeValueImpl()
}

func (ga *GenericArray) NewTypeValueImpl() *GenericArray {
	return &GenericArray{
		elements:   nil,
		dimensions: nil,
		status:     0,
		typeName:   ga.typeName,
		elementOID: ga.elementOID,
		newElement: ga.newElement,
		logger:     ga.logger,
		source:     nil,
	}
}

func (ga *GenericArray) TypeName() string {
	return ga.typeName
}

func (ga *GenericArray) Set(interface{}) error {
	return xerrors.New("unsupported")
}

func (ga *GenericArray) Get() interface{} {
	switch ga.status {
	case pgtype.Present:
		return ga
	case pgtype.Null:
		return nil
	default:
		return ga.status
	}
}

func (ga *GenericArray) AssignTo(_ interface{}) error {
	return xerrors.New("unsupported")
}

func (ga *GenericArray) setNil() {
	ga.elements = nil
	ga.dimensions = nil
	ga.status = pgtype.Null
}

func (ga *GenericArray) DecodeText(ci *pgtype.ConnInfo, src []byte) error {
	ga.source = src
	if src == nil {
		ga.setNil()
		return nil
	}

	uta, err := pgtype.ParseUntypedTextArray(string(src))
	if err != nil {
		return xerrors.Errorf("unable to parse untyped text array: %w", err)
	}

	var elements []pgtype.Value

	if len(uta.Elements) > 0 {
		elements = make([]pgtype.Value, len(uta.Elements))

		for i, s := range uta.Elements {
			elem, err := ga.newElement()
			if err != nil {
				return xerrors.Errorf("unable to create array element: %w", err)
			}
			var elemSrc []byte
			if s != "NULL" {
				elemSrc = []byte(s)
			}

			decoder, err := textDecoder(elem)
			if err != nil {
				ga.logger.Warn(err.Error())
				genericText := new(pgtype.GenericText)
				elem = genericText
				decoder = genericText
			}

			if err = decoder.DecodeText(ci, elemSrc); err != nil {
				return xerrors.Errorf("unable to decode text element: %w", err)
			}

			elements[i] = elem
		}
	}

	ga.elements = elements
	ga.dimensions = uta.Dimensions
	ga.status = pgtype.Present

	return nil
}

func (ga *GenericArray) DecodeBinary(ci *pgtype.ConnInfo, src []byte) error {
	if src == nil {
		ga.setNil()
		return nil
	}

	var arrayHeader pgtype.ArrayHeader
	rp, err := arrayHeader.DecodeBinary(ci, src)
	if err != nil {
		return xerrors.Errorf("unable to decode binary array header: %w", err)
	}

	var elements []pgtype.Value

	if len(arrayHeader.Dimensions) == 0 {
		ga.elements = elements
		ga.dimensions = arrayHeader.Dimensions
		ga.status = pgtype.Present
		return nil
	}

	elementCount := arrayHeader.Dimensions[0].Length
	for _, d := range arrayHeader.Dimensions[1:] {
		elementCount *= d.Length
	}

	elements = make([]pgtype.Value, elementCount)

	for i := range elements {
		elem, err := ga.newElement()
		if err != nil {
			return xerrors.Errorf("unable to create array element: %w", err)
		}
		elemLen := int(int32(binary.BigEndian.Uint32(src[rp:])))
		rp += 4
		var elemSrc []byte
		if elemLen >= 0 {
			elemSrc = src[rp : rp+elemLen]
			rp += elemLen
		}

		decoder, err := binaryDecoder(elem)
		if err == nil {
			err = decoder.DecodeBinary(ci, elemSrc)
		}

		if err != nil {
			return xerrors.Errorf("unable to decode binary element: %w", err)
		}

		elements[i] = elem
	}

	ga.elements = elements
	ga.dimensions = arrayHeader.Dimensions
	ga.status = pgtype.Present

	return nil
}

func (ga *GenericArray) EncodeText(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	if ga.source != nil {
		return append(buf, ga.source...), nil
	}

	switch ga.status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, xerrors.New("array is undefined")
	}

	if len(ga.dimensions) == 0 {
		return append(buf, '{', '}'), nil
	}

	buf = pgtype.EncodeTextArrayDimensions(buf, ga.dimensions)

	// dimElemCounts is the multiples of elements that each array lies on. For
	// example, a single dimension array of length 4 would have a dimElemCounts of
	// [4]. A multi-dimensional array of lengths [3,5,2] would have a
	// dimElemCounts of [30,10,2]. This is used to simplify when to render a '{'
	// or '}'.
	dimElemCounts := make([]int, len(ga.dimensions))
	dimElemCounts[len(ga.dimensions)-1] = int(ga.dimensions[len(ga.dimensions)-1].Length)
	for i := len(ga.dimensions) - 2; i > -1; i-- {
		dimElemCounts[i] = int(ga.dimensions[i].Length) * dimElemCounts[i+1]
	}

	inElemBuf := make([]byte, 0, 32)
	for i, elem := range ga.elements {
		if i > 0 {
			buf = append(buf, ',')
		}

		for _, dec := range dimElemCounts {
			if i%dec == 0 {
				buf = append(buf, '{')
			}
		}

		var elemBuf []byte
		elemEncoder, err := textEncoder(elem)
		if err == nil {
			elemBuf, err = elemEncoder.EncodeText(ci, inElemBuf)
		}

		if err != nil {
			return nil, xerrors.Errorf("unable to text encode element: %w", err)
		}

		if elemBuf == nil {
			buf = append(buf, `NULL`...)
		} else {
			buf = append(buf, pgtype.QuoteArrayElementIfNeeded(string(elemBuf))...)
		}

		for _, dec := range dimElemCounts {
			if (i+1)%dec == 0 {
				buf = append(buf, '}')
			}
		}
	}

	return buf, nil
}

func (ga *GenericArray) EncodeBinary(ci *pgtype.ConnInfo, buf []byte) ([]byte, error) {
	switch ga.status {
	case pgtype.Null:
		return nil, nil
	case pgtype.Undefined:
		return nil, xerrors.New("array is undefined")
	}

	arrayHeader := pgtype.ArrayHeader{
		Dimensions: ga.dimensions,
		ElementOID: int32(ga.elementOID),
	}

	for i := range ga.elements {
		if ga.elements[i].Get() == nil {
			arrayHeader.ContainsNull = true
			break
		}
	}

	buf = arrayHeader.EncodeBinary(ci, buf)

	for i := range ga.elements {
		sp := len(buf)
		buf = pgio.AppendInt32(buf, -1)

		var elemBuf []byte
		binaryEncoder, err := binaryEncoder(ga.elements[i])
		if err == nil {
			elemBuf, err = binaryEncoder.EncodeBinary(ci, buf)
		}

		if err != nil {
			return nil, xerrors.Errorf("unable to binary encode element: %w", err)
		}

		if elemBuf != nil {
			buf = elemBuf
			pgio.SetInt32(buf[sp:], int32(len(buf[sp:])-4))
		}
	}

	return buf, nil
}

func (ga *GenericArray) Scan(_ interface{}) error {
	return xerrors.New("unsupported")
}

func (ga *GenericArray) Value() (driver.Value, error) {
	return ga.GetValue(nil)
}

func (ga *GenericArray) GetValue(connInfo *pgtype.ConnInfo) (interface{}, error) {
	if buf, err := ga.EncodeText(connInfo, nil); err == nil {
		if buf == nil {
			return nil, nil
		}
		return string(buf), nil
	} else {
		return nil, xerrors.Errorf("unable to text encode generic array: %w", err)
	}
}

func (ga *GenericArray) ExtractValue(connInfo *pgtype.ConnInfo) (interface{}, error) {
	xdArray, err := ga.Unpack(connInfo)
	if err != nil {
		ga.logger.Warn("unable to unpack generic array", log.Error(err))
		return ga.GetValue(connInfo)
	}

	if xdArray != nil {
		return xdArray.Data, nil
	}

	return nil, nil
}
