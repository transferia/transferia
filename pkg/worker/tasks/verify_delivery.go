package tasks

import (
	"time"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
)

func pingSinker(s abstract.AsyncSink) error {
	dropItem := []abstract.ChangeItem{
		{
			CommitTime:   uint64(time.Now().UnixNano()),
			Kind:         abstract.DropTableKind,
			Table:        "_ping",
			ColumnValues: []interface{}{"_ping"},
		},
	}

	err := <-s.AsyncPush(dropItem)
	if err != nil {
		return xerrors.Errorf("sinker unable to push drop item: %w", err)
	}

	err = <-s.AsyncPush([]abstract.ChangeItem{
		{
			Kind:         abstract.InsertKind,
			Table:        "_ping",
			ColumnNames:  []string{"k", "_dummy"},
			ColumnValues: []interface{}{1, "nothing"},
			TableSchema: abstract.NewTableSchema([]abstract.ColSchema{
				{
					ColumnName: "k",
					DataType:   "int32",
					PrimaryKey: true,
				}, {
					ColumnName: "_dummy",
					DataType:   "string",
				},
			}),
		},
	})
	if err != nil {
		return xerrors.Errorf("unable to push: %w", err)
	}

	if err := <-s.AsyncPush(dropItem); err != nil {
		return xerrors.Errorf("sinker unable to push drop item: %w", err)
	}

	return nil
}
