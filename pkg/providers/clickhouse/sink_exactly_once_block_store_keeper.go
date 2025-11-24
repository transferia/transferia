package clickhouse

import (
	"database/sql"
	"fmt"

	"github.com/doublecloud/transfer/library/go/core/xerrors"
	"github.com/doublecloud/transfer/pkg/abstract"
)

var (
	_ InsertBlockStore = (*KeeperBlockStore)(nil)
)

type KeeperBlockStore struct {
	table string
	db    *sql.DB
}

func (k *KeeperBlockStore) Get(id abstract.TablePartID) (*InsertBlock, InsertBlockStatus, error) {
	lastBlockRow := k.db.QueryRow(fmt.Sprintf(`
	SELECT min_offset, max_offset, status
	FROM %s
		WHERE part_id = ?;
	`, k.table), id.FqtnWithPartID())
	var block InsertBlock
	var status InsertBlockStatus
	if err := lastBlockRow.Scan(&block.min, &block.max, &status); err != nil {
		if xerrors.Is(err, sql.ErrNoRows) {
			return nil, InsertBlockStatusEmpty, nil
		}
		return nil, "", xerrors.Errorf("unable to get block: %w", err)
	}
	return &block, status, nil
}

func (k *KeeperBlockStore) Set(id abstract.TablePartID, block *InsertBlock, status InsertBlockStatus) error {
	_, err := k.db.Exec(fmt.Sprintf(`
	ALTER TABLE  %s
	UPDATE min_offset = ?, max_offset = ?, status = ?
	WHERE part_id = ?;
	`, k.table), block.min, block.max, status, id.FqtnWithPartID())
	if err != nil {
		return xerrors.Errorf("unable to set block: %w", err)
	}
	return nil
}

func NewKeeperBlockStore(
	table string,
	db *sql.DB,
) (*KeeperBlockStore, error) {
	ddl := fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %[1]s
(
    part_id    String,
    min_offset UInt64,
    max_offset UInt64,
    status     String
)
ENGINE = KeeperMap('/%[1]s', 128)
PRIMARY KEY part_id
`, table)
	_, err := db.Exec(ddl)
	if err != nil {
		return nil, xerrors.Errorf("unable to init keeper block store table: %s: %w", ddl, err)
	}
	return &KeeperBlockStore{
		table: table,
		db:    db,
	}, nil
}
