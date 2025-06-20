//go:build !disable_postgres_provider

package postgres

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/transferia/transferia/internal/config"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"go.ytsaurus.tech/library/go/core/log"
)

const (
	lockRenewInterval = 2 * time.Second
)

var ErrConsumerLocked = errors.New("keeper: Consumer already locked")

type Config struct {
	config.DBConfig
}

func KeeperDDL(schema string) string {
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS "%s"."%s" (
    consumer TEXT PRIMARY KEY,
    locked_till TIMESTAMPTZ,
    locked_by TEXT,
    UNIQUE (consumer, locked_by)
)`, schema, TableConsumerKeeper)
}

type Keeper struct {
	CloseSign chan bool

	instance string
	schema   string
	logger   log.Logger
	once     sync.Once
	conn     *pgxpool.Pool
}

type (
	txOp  func(conn pgx.Tx) error
	sqlOp func(conn *pgxpool.Pool) error
)

func (k *Keeper) tx(operation txOp) error {
	tx, err := k.conn.Begin(context.TODO())
	if err != nil {
		return xerrors.Errorf("unable to begin transaction: %w", err)
	}
	if err := operation(tx); err != nil {
		if err := tx.Rollback(context.TODO()); err != nil {
			k.logger.Warn("Unable to rollback", log.Error(err))
		}

		return xerrors.Errorf("unable to execute operation: %w", err)
	}

	if err := tx.Commit(context.TODO()); err != nil {
		return xerrors.Errorf("unable to commit transaction: %w", err)
	}

	return nil
}

func (k *Keeper) do(operation sqlOp) error {
	if err := operation(k.conn); err != nil {
		return err
	}
	return nil
}

func (k *Keeper) Init(sink abstract.AsyncSink) error {
	return <-sink.AsyncPush([]abstract.ChangeItem{{
		CommitTime:   uint64(time.Now().UnixNano()),
		Kind:         "pg:DDL", // TODO: Replace with generic kind
		ColumnValues: []interface{}{KeeperDDL(k.schema)},
	}})
}

func (k *Keeper) init() error {
	return k.tx(func(tx pgx.Tx) error {
		var keeperSchemaExists bool
		if err := tx.QueryRow(context.TODO(), "SELECT EXISTS(SELECT * FROM information_schema.schemata WHERE schema_name = $1)", k.schema).Scan(&keeperSchemaExists); err != nil {
			return xerrors.Errorf("failed to check existence of the service schema %q: %w", k.schema, err)
		}

		if !keeperSchemaExists {
			if _, err := tx.Exec(context.TODO(), fmt.Sprintf(`CREATE SCHEMA %s`, k.schema)); err != nil {
				return xerrors.Errorf("failed to create service schema %q: %w", k.schema, err)
			}
		}

		if _, err := tx.Exec(context.TODO(), KeeperDDL(k.schema)); err != nil {
			return xerrors.Errorf("failed to ensure existence of the consumer keeper service table: %w", err)
		}

		return nil
	})
}

func (k *Keeper) tryLock(consumer string) error {
	return k.tx(func(conn pgx.Tx) error {
		row := conn.QueryRow(context.TODO(), fmt.Sprintf(`
select exists(
   select *
   	from "%s"."%s"
   where consumer = ($1) and (locked_till > (now()-interval'30 second') and locked_by != ($2))
);`, k.schema, TableConsumerKeeper),
			consumer,
			k.instance)

		var exists bool
		err := row.Scan(&exists)
		if err != nil {
			return xerrors.Errorf("unable to scan consumer keeper row: %w", err)
		}

		if exists {
			return ErrConsumerLocked
		}

		if _, err = conn.Exec(context.TODO(), fmt.Sprintf(`
insert into "%s"."%s" (consumer, locked_till, locked_by) values (($1), now(), ($2))
on conflict (consumer) do update set locked_till = EXCLUDED.locked_till, locked_by = EXCLUDED.locked_by
;
`, k.schema, TableConsumerKeeper), consumer, k.instance); err != nil {
			if strings.Contains(err.Error(), "in a read-only transaction") {
				k.logger.Warn("unable to update consumer keeper table, source in read-only mode. If it is an anomaly please check you cluster", log.Error(err))
				return nil
			}
			return xerrors.Errorf("unable update consumer keeper table: %w", err)
		}

		return nil
	})
}

func (k *Keeper) Lock(consumer string) (chan bool, error) {
	err := k.do(func(conn *pgxpool.Pool) error {
		rows, err := conn.Query(context.TODO(), fmt.Sprintf(`
   select locked_by
   	from "%s"."%s"
   where consumer = ($1) and (locked_till > now() and locked_by != ($2))`, k.schema, TableConsumerKeeper),
			consumer,
			k.instance)
		if err != nil {
			return xerrors.Errorf("failed to select consumer keeper locks: %w", err)
		}
		defer rows.Close()

		var host string

		for rows.Next() {
			err := rows.Scan(&host)
			if err != nil {
				return xerrors.Errorf("unable to scan consumer keeper lock row: %w", err)
			}

			if len(host) > 0 {
				return fmt.Errorf("consumer taken by %v", host)
			}
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	k.logger.Info("Initial lock acquired, start polling process")
	stopCh := make(chan bool)

	go func() {
		defer close(stopCh)
		defer k.Stop()
		for {
			select {
			case <-k.CloseSign:
				return
			default:
			}

			err := k.tryLock(consumer)
			if err != nil {
				k.logger.Warn("Keeper try lock failed", log.Error(err))
				return
			}

			time.Sleep(lockRenewInterval)
		}
	}()

	return stopCh, nil
}

func (k *Keeper) Stop() {
	k.once.Do(func() {
		close(k.CloseSign)
	})
}

func NewKeeper(conn *pgxpool.Pool, logger log.Logger, schema string) (*Keeper, error) {
	instance, err := os.Hostname()
	if err != nil {
		return nil, xerrors.Errorf("unable to get host name: %w", err)
	}

	kpr := Keeper{
		instance:  instance,
		conn:      conn,
		logger:    logger,
		schema:    schema,
		once:      sync.Once{},
		CloseSign: make(chan bool),
	}

	if err := kpr.init(); err != nil {
		return nil, xerrors.Errorf("unable to initialize consumer keeper: %w", err)
	}

	return &kpr, nil
}
