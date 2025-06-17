//go:build !disable_postgres_provider

package postgres

import (
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors/coded"
	"go.ytsaurus.tech/library/go/core/log"
)

func VerifyPostgresTablesNames(tables []string) error {
	for _, table := range tables {
		if !strings.Contains(table, ".") {
			return xerrors.New("verify_tables: must specify full table name (including schema).")
		}
	}
	return nil
}

func VerifyPostgresTables(src *PgSource, transfer *model.Transfer, lgr log.Logger) error {
	if len(src.DBTables) == 0 {
		return nil
	}
	storage, err := NewStorage(src.ToStorageParams(transfer))
	if err != nil {
		lgr.Error("Unable to create storage", log.Error(err))
		return xerrors.Errorf("unable to create storage: %w", err)
	}
	defer storage.Close()
	tables, err := storage.TableList(nil)
	if err != nil {
		return xerrors.Errorf("unable to get table map from storage: %w", err)
	}
	missed := make([]string, 0)
	exist := make(abstract.TableMap)
	for tID, tInfo := range tables {
		if src.Include(tID) {
			exist[tID] = tInfo
		}
	}
	includeTables := src.DBTables
	objects := transfer.DataObjects
	if objects != nil && len(objects.IncludeObjects) > 0 {
		includeTables = objects.IncludeObjects
	}
	for _, table := range includeTables {
		found := false
		for existID := range exist {
			existDescription := abstract.TableDescription{
				Name:   existID.Name,
				Schema: existID.Namespace,
				Filter: "",
				EtaRow: 0,
				Offset: 0,
			}
			if existDescription.Same(table) {
				found = true
			}
		}
		if !found {
			if strings.HasSuffix(table, ".*") {
				continue
			}
			missed = append(missed, table)
		}
	}
	if len(missed) > 0 {
		return xerrors.Errorf("Tables not found. Missed: %v", missed)
	}
	if noKeysTables := exist.NoKeysTables(); len(noKeysTables) > 0 {
		return coded.Errorf(NoPrimaryKeyCode, "unable to check primary keys: %v", noKeysTables)
	}
	return nil
}
