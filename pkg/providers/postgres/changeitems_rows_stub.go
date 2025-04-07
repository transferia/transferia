package postgres

import (
	"encoding/json"

	"github.com/jackc/pgconn"
	"github.com/jackc/pgproto3/v2"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
)

// Sniffer you may replace Fetch with Sniffer to get raw data.
func (f *ChangeItemsFetcher) Sniffer() (items []abstract.ChangeItem, err error) {
	st := new(stubRows)
	f.rows.Next()
	st.Template = f.template
	st.ParseSchema = f.parseSchema
	st.Data = append(st.Data, f.rows.RawValues())
	st.Fields = yslices.Map(f.rows.FieldDescriptions(), func(fd pgproto3.FieldDescription) FieldDescription {
		return FieldDescription{
			Name:                 string(fd.Name),
			TableOID:             fd.TableOID,
			TableAttributeNumber: fd.TableAttributeNumber,
			DataTypeOID:          fd.DataTypeOID,
			DataTypeSize:         fd.DataTypeSize,
			TypeModifier:         fd.TypeModifier,
			Format:               fd.Format,
		}
	})
	rawStub, err := json.Marshal(st)
	if err != nil {
		return nil, xerrors.Errorf("unable to marshal: %w", err)
	}
	logger.Log.Infof("raw data: \n\n%v\n\n", rawStub)
	return nil, abstract.NewFatalError(xerrors.New("sniff fetcher"))
}

type FieldDescription struct {
	Name                 string
	TableOID             uint32
	TableAttributeNumber uint16
	DataTypeOID          uint32
	DataTypeSize         int16
	TypeModifier         int32
	Format               int16
}

type stubRows struct {
	Data        [][][]byte            `json:"data"`
	Fields      []FieldDescription    `json:"fields"`
	ParseSchema abstract.TableColumns `json:"parse_schema"`
	Template    abstract.ChangeItem   `json:"template"`
	iter        int
	pgFields    []pgproto3.FieldDescription
	limit       int
}

func (s *stubRows) init() {
	s.pgFields = yslices.Map(s.Fields, func(fd FieldDescription) pgproto3.FieldDescription {
		return pgproto3.FieldDescription{
			Name:                 []byte(fd.Name),
			TableOID:             fd.TableOID,
			TableAttributeNumber: fd.TableAttributeNumber,
			DataTypeOID:          fd.DataTypeOID,
			DataTypeSize:         fd.DataTypeSize,
			TypeModifier:         fd.TypeModifier,
			Format:               fd.Format,
		}
	})
}

func (s *stubRows) Close() {
}

func (s *stubRows) Err() error {
	return nil
}

func (s *stubRows) FieldDescriptions() []pgproto3.FieldDescription {
	return s.pgFields
}

func (s *stubRows) Next() bool {
	if s.limit > s.iter {
		s.iter++
		return true
	}
	return false
}

func (s *stubRows) RawValues() [][]byte {
	return s.Data[0]
}

func (s *stubRows) CommandTag() pgconn.CommandTag {
	panic("not supported")
}

func (s *stubRows) Scan(dest ...interface{}) error {
	panic("not supported")
}

func (s *stubRows) Values() ([]interface{}, error) {
	panic("not supported")
}
