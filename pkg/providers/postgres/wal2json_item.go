//go:build !disable_postgres_provider

package postgres

import (
	"github.com/jackc/pgtype"
	"github.com/transferia/transferia/pkg/abstract"
)

// This is basically an abstract.ChangeItem, but with two additional fields:
// * columntypeoids
// * oldkeys.keytypeoids
// Note that historically abstract.ChangeItem's structure was exactly the same
// as the structure of objects we received from wal2json, since PostgreSQL was
// one of the first endpoints implemented in DataTransfer. Later ChangeItem had
// to encompass other databases and become "abstract". There is no point in
// adding any more PostgreSQL specifics into it, so we use Wal2JSONItem for
// parsing PostgreSQL changes from wal2json now.
type Wal2JSONItem struct {
	ID             uint32               `json:"id" yson:"id"`
	LSN            uint64               `json:"nextlsn" yson:"nextlsn"`
	CommitTime     uint64               `json:"commitTime" yson:"commitTime"`
	Counter        int                  `json:"txPosition" yson:"txPosition"`
	Kind           abstract.Kind        `json:"kind" yson:"kind"`
	Schema         string               `json:"schema" yson:"schema"`
	Table          string               `json:"table" yson:"table"`
	PartID         string               `json:"part" yson:"-"`
	ColumnNames    []string             `json:"columnnames" yson:"columnnames"`
	ColumnValues   []interface{}        `json:"columnvalues,omitempty" yson:"columnvalues"`
	TableSchema    []abstract.ColSchema `json:"table_schema,omitempty" yson:"table_schema"`
	OldKeys        OldKeysType          `json:"oldkeys" yson:"oldkeys"`
	TxID           string               `json:"tx_id" yson:"-"`
	Query          string               `json:"query" yson:"-"`
	Size           abstract.EventSize   `json:"-" yson:"-"`
	ColumnTypeOIDs []pgtype.OID         `json:"columntypeoids"`
}

type OldKeysType struct {
	abstract.OldKeysType
	KeyTypeOids []pgtype.OID `json:"keytypeoids"`
}

func (w *Wal2JSONItem) toChangeItem() abstract.ChangeItem {
	return abstract.ChangeItem{
		ID:           w.ID,
		LSN:          w.LSN,
		CommitTime:   w.CommitTime,
		Counter:      w.Counter,
		Kind:         w.Kind,
		Schema:       w.Schema,
		Table:        w.Table,
		PartID:       w.PartID,
		ColumnNames:  w.ColumnNames,
		ColumnValues: w.ColumnValues,
		TableSchema:  nil,
		OldKeys:      w.OldKeys.OldKeysType,
		TxID:         w.TxID,
		Query:        w.Query,
		Size:         w.Size,
	}
}
