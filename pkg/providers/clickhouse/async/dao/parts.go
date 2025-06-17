//go:build !disable_clickhouse_provider

package dao

import (
	"context"
	"fmt"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/providers/clickhouse/async/model/db"
	"go.ytsaurus.tech/library/go/core/log"
)

type PartsDAO struct {
	db  db.Client
	lgr log.Logger
}

func (d *PartsDAO) AttachTablePartsTo(dstDB, dstTable, srcDB, srcTable string) error {
	d.lgr.Infof("Attaching partitions from %s.%s to %s.%s", srcDB, srcTable, dstDB, dstTable)
	partitions, err := d.getPartitionList(srcDB, srcTable)
	if err != nil {
		return xerrors.Errorf("error getting table %s partitions: %w", srcTable, err)
	}
	d.lgr.Info(fmt.Sprintf("Got %d partitions for table", len(partitions)),
		log.String("table", srcTable), log.Strings("partitions", partitions))

	for _, p := range partitions {
		q := fmt.Sprintf(`ALTER TABLE "%s"."%s" ATTACH PARTITION ID '%s' FROM "%s"."%s"`,
			dstDB, dstTable, p, srcDB, srcTable)
		d.lgr.Info(fmt.Sprintf("Attaching partition %s", p), log.String("sql", q))

		err := backoff.RetryNotify(
			func() error {
				_, err := d.db.ExecContext(context.Background(), q)
				return err
			},
			backoff.NewExponentialBackOff(backoff.WithMaxElapsedTime(10*time.Minute)),
			func(err error, dur time.Duration) {
				d.lgr.Error(fmt.Sprintf("Got Attach Partition error, retrying after %v", dur), log.Error(err))
			},
		)
		if err != nil {
			return xerrors.Errorf("error attaching table partition: %w", err)
		}
		d.lgr.Info(fmt.Sprintf("Attached partition %s", p), log.String("sql", q))
	}
	return nil
}

func (d *PartsDAO) getPartitionList(dbName, table string) ([]string, error) {
	q := fmt.Sprintf(`SELECT DISTINCT partition_id FROM system.parts WHERE database = '%s' and table = '%s'`,
		dbName, table)
	rows, err := d.db.QueryContext(context.Background(), q)
	if err != nil {
		return nil, xerrors.Errorf("partitions query error: %w", err)
	}
	defer rows.Close()
	var partitions []string
	for rows.Next() {
		var p string
		if err := rows.Scan(&p); err != nil {
			return nil, xerrors.Errorf("error scanning partitions result: %w", err)
		}
		partitions = append(partitions, p)
	}
	if err := rows.Err(); err != nil {
		return nil, xerrors.Errorf("error reading partitions result: %w", err)
	}
	return partitions, nil
}

func NewPartsDAO(db db.Client, lgr log.Logger) *PartsDAO {
	return &PartsDAO{
		db:  db,
		lgr: lgr,
	}
}
