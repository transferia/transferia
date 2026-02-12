package postgres

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"time"

	"github.com/cenkalti/backoff/v4"
	"github.com/jackc/pgx/v4"
	"github.com/transferia/transferia/internal/logger"
	"github.com/transferia/transferia/library/go/core/metrics"
	"github.com/transferia/transferia/library/go/core/xerrors"
	yaslices "github.com/transferia/transferia/library/go/slices"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/coordinator"
	"github.com/transferia/transferia/pkg/abstract/model"
	"github.com/transferia/transferia/pkg/errors/coded"
	"github.com/transferia/transferia/pkg/errors/codes"
	"github.com/transferia/transferia/pkg/middlewares"
	sink_factory "github.com/transferia/transferia/pkg/sink"
	"github.com/transferia/transferia/pkg/util"
	"github.com/transferia/transferia/pkg/util/set"
	"go.ytsaurus.tech/library/go/core/log"
	"golang.org/x/exp/slices"
)

type pgDumpItem struct {
	Name   string
	Typ    string
	Owner  string
	Body   string
	Schema string
}

var typesExistsQuery = `SELECT EXISTS (
SELECT t.typname as type
FROM pg_type t
LEFT JOIN pg_catalog.pg_namespace n ON n.oid = t.typnamespace
WHERE (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid))
AND NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)
AND n.nspname NOT IN ('pg_catalog', 'information_schema')
);`

func (i *pgDumpItem) TableDescription() (*abstract.TableDescription, error) {
	scanner := bufio.NewScanner(bytes.NewReader([]byte(i.Body)))
	for scanner.Scan() {
		row := scanner.Text()
		if strings.HasPrefix(row, "CREATE TABLE ") {
			parts := strings.Split(row, " ")
			if len(parts) > 3 {
				fqtn := strings.Split(parts[2], ".")
				if len(fqtn) > 1 {
					schema := strings.ReplaceAll(fqtn[0], "\"", "")
					name := strings.ReplaceAll(fqtn[1], "\"", "")
					return &abstract.TableDescription{
						Name:   name,
						Schema: schema,
						Filter: "",
						EtaRow: 0,
						Offset: 0,
					}, nil
				}
			}
		}
	}

	return nil, xerrors.New("Not found `CREATE TABLE` line")
}

func ApplyCommands(commands []*pgDumpItem, transfer model.Transfer, task *model.TransferOperation, registry metrics.Registry, types ...string) error {
	if _, ok := transfer.Dst.(*PgDestination); !ok {
		return nil
	}
	sink, err := sink_factory.MakeAsyncSink(&transfer, task, logger.Log, registry, coordinator.NewFakeClient(), middlewares.MakeConfig(middlewares.WithNoData))
	if err != nil {
		return err
	}
	defer sink.Close()
	allowedType := map[string]bool{}
	for _, typ := range types {
		allowedType[typ] = true
	}
	// we need to move schema always, since all items somewhat bounded to it
	allowedType["SCHEMA"] = true
	for _, command := range commands {
		if !allowedType[command.Typ] {
			continue
		}
		logger.Log.Infof("Try to apply PostgreSQL DDL of type '%v', name '%v'.'%v'", command.Typ, command.Schema, command.Name)
		if err := <-sink.AsyncPush([]abstract.ChangeItem{{
			CommitTime:   uint64(time.Now().UnixNano()),
			Kind:         abstract.PgDDLKind,
			ColumnValues: []interface{}{command.Body},
		}}); err != nil {
			if isAlreadyExistsError(err) {
				logger.Log.Warnf("Object(type '%v', name '%v'.'%v') already exists or is already performed", command.Typ, command.Schema, command.Name)
				continue
			}
			if command.Typ == "PRIMARY_KEY" {
				if strings.Contains(err.Error(), "multiple primary keys for table") {
					logger.Log.Warn(
						fmt.Sprintf("Multiple primary keys for '%v', name '%v'.'%v'", command.Typ, command.Schema, command.Name),
						log.Error(err))
					continue
				}
			}
			logger.Log.Error(
				fmt.Sprintf("Unable to apply DDL of type '%v', name '%v'.'%v'", command.Typ, command.Schema, command.Name),
				log.String("query", command.Body),
				log.Error(err),
			)
			// If destination has functions from extensions in user schema missing, pg returns 42883
			if IsPgError(err, ErrcUndefinedFunction) {
				return coded.Errorf(codes.PostgresUndefinedFunction,
					"Unable to apply DDL of type '%v', name '%v'.'%v', error: %w",
					command.Typ, command.Schema, command.Name, err)
			}
			// If schema is missing, map 3F000
			if IsPgError(err, ErrcSchemaDoesNotExists) {
				return coded.Errorf(codes.PostgresSchemaDoesNotExist,
					"Unable to apply DDL of type '%v', name '%v'.'%v', error: %w",
					command.Typ, command.Schema, command.Name, err)
			}
			return coded.Errorf(codes.PostgresDDLApplyFailed,
				"Unable to apply DDL of type '%v', name '%v'.'%v', error: %w",
				command.Typ, command.Schema, command.Name, err)
		}
	}
	return nil
}

func isAlreadyExistsError(err error) bool {
	msg := err.Error()
	return strings.Contains(msg, "already exists") || strings.Contains(msg, "is already a partition")
}

func formatFqtn(in string) (string, error) {
	tableID, err := abstract.NewTableIDFromStringPg(in, false)
	if err != nil {
		return "", xerrors.Errorf("failed to parse: %w", err)
	}
	return tableID.Fqtn(), nil
}

func PostgresDumpConnString(src *PgSource) (string, model.SecretString, error) {

	config, err := GetConnParamsFromSrc(logger.Log, src)
	if err != nil {
		return "", "", err
	}
	logger.Log.Infof("Getting dump conn string for master host '%s'", config.Host)

	if config.HasTLS {
		customCaPath := "./customRootCA.crt"
		if err := os.WriteFile(customCaPath, []byte(config.CACertificates), 0o664); err != nil {
			return "", "", xerrors.Errorf("failed to write a custom SSL root certificate into a local file: %w", err)
		}
		return fmt.Sprintf("host=%v port=%v dbname=%v user=%v sslmode=verify-full sslrootcert=%v", config.Host, config.Port, config.Database, config.User, customCaPath), config.Password, nil
	} else {
		return fmt.Sprintf("host=%v port=%v dbname=%v user=%v", config.Host, config.Port, config.Database, config.User), config.Password, nil
	}
}

// resolveTablesIncluded returns intersection of source and transfer include lists.
// When both are empty, returns nil. When only one is set, returns that list.
func resolveTablesIncluded(src *PgSource, transfer *model.Transfer) ([]abstract.TableID, error) {
	fromSrc := make([]abstract.TableID, 0, len(src.DBTables))
	for _, table := range src.DBTables {
		parsed, err := abstract.ParseTableID(table)
		if err != nil {
			return nil, xerrors.Errorf("failed to parse source include directive '%s': %w", table, err)
		}
		fromSrc = append(fromSrc, *parsed)
	}
	fromTransfer := make([]abstract.TableID, 0)
	if transfer.DataObjects != nil {
		for _, table := range transfer.DataObjects.GetIncludeObjects() {
			parsed, err := abstract.ParseTableID(table)
			if err != nil {
				return nil, xerrors.Errorf("failed to parse transfer include directive '%s': %w", table, err)
			}
			fromTransfer = append(fromTransfer, *parsed)
		}
	}
	return abstract.TableIDsIntersection(fromSrc, fromTransfer), nil
}

func pgDumpSchemaArgs(src *PgSource, tablesIncluded []abstract.TableID, seqsIncluded []abstract.TableID, seqsExcluded []abstract.TableID) ([]string, error) {
	args := make([]string, 0)
	args = append(args,
		"--no-publications",
		"--no-subscriptions",
		"--format=plain",
		"--no-owner",
		"--schema-only",
	)

	if len(tablesIncluded) > 0 {
		for _, table := range tablesIncluded {
			args = append(args, "-t", table.Fqtn())
		}
		for _, t := range src.AuxTables() {
			args = append(args, "-t", t)
		}
		for _, seq := range seqsIncluded {
			args = append(args, "-t", seq.Fqtn())
		}
	}

	for _, t := range src.ExcludeWithGlobals() {
		if len(t) == 0 {
			// TM-1964
			continue
		}
		arg, err := formatFqtn(t)
		if err != nil {
			return nil, xerrors.Errorf("failed to format directive '%s': %w", t, err)
		}
		args = append(args, "-T", arg)
	}
	for _, seq := range seqsExcluded {
		args = append(args, "-T", seq.Fqtn())
	}

	return args, nil
}

// dumpSequenceValues produces SEQUENCE SET pg_dump events which transmit the current state of all sequences in the given list from the source database
func dumpSequenceValues(ctx context.Context, conn *pgx.Conn, sequences []abstract.TableID) ([]*pgDumpItem, error) {
	result := make([]*pgDumpItem, 0)
	for _, seq := range sequences {
		lastValue, isCalled, err := GetCurrentStateOfSequence(ctx, conn, seq)
		if err != nil {
			return nil, xerrors.Errorf("failed to get current state of SEQUENCE %s: %w", seq.String(), err)
		}
		seqItem := &pgDumpItem{
			Name:   seq.Name,
			Typ:    string(SequenceSet),
			Owner:  "",
			Body:   fmt.Sprintf("SELECT pg_catalog.setval('%s', %d, %t);", seq.Fqtn(), lastValue, isCalled),
			Schema: seq.Namespace,
		}
		result = append(result, seqItem)
	}
	return result, nil
}

// sourceInPgPg returns a non-nil object only for homogenous PG-PG transfers
func sourceInPgPg(transfer *model.Transfer) *PgSource {
	var src *PgSource
	var srcIsPG bool
	var dstIsPG bool
	src, srcIsPG = transfer.Src.(*PgSource)
	_, dstIsPG = transfer.Dst.(*PgDestination)
	if !(srcIsPG && dstIsPG) {
		return nil
	}
	return src
}

// ExtractPgDumpSchema returns the dump ONLY for homogenous PG-PG transfers. It also logs its actions
func ExtractPgDumpSchema(transfer *model.Transfer) ([]*pgDumpItem, error) {
	src := sourceInPgPg(transfer)
	if src == nil {
		return nil, nil
	}

	logger.Log.Info("Schema will be extracted by pg_dump for a PostgreSQL-PostgreSQL transfer")
	pgdump, err := loadPgDumpSchema(context.Background(), src, transfer)
	if err != nil {
		return nil, xerrors.Errorf("failed to extract schema from the source PostgreSQL by pg_dump: %w", err)
	}
	logger.Log.Info("Successfully extracted schema from PostgreSQL source by pg_dump", log.Int("len", len(pgdump)))
	return pgdump, nil
}

// ApplyPgDumpPreSteps takes the given dump and applies pre-steps defined in transfer source ONLY for homogenous PG-PG transfers. It also logs its actions
func ApplyPgDumpPreSteps(pgdump []*pgDumpItem, transfer *model.Transfer, task *model.TransferOperation, registry metrics.Registry) error {
	if len(pgdump) == 0 {
		return nil
	}
	src := sourceInPgPg(transfer)
	if src == nil {
		return nil
	}

	if err := ApplyCommands(pgdump, *transfer, task, registry, src.PreSteps.List()...); err != nil {
		return xerrors.Errorf("failed to apply schema pre-steps (%v) in the destination PostgreSQL: %w", src.PreSteps.List(), err)
	}
	logger.Log.Info("Successfully applied schema pre-steps in the destination PostgreSQL", log.Array("steps", src.PreSteps.List()))
	return nil
}

// ApplyPgDumpPostSteps takes the given dump and applies post-steps defined in transfer source ONLY for homogenous PG-PG transfers. It also logs its actions
func ApplyPgDumpPostSteps(pgdump []*pgDumpItem, transfer *model.Transfer, task *model.TransferOperation, registry metrics.Registry) error {
	if len(pgdump) == 0 {
		return nil
	}
	src := sourceInPgPg(transfer)
	if src == nil {
		return nil
	}

	if err := ApplyCommands(pgdump, *transfer, task, registry, src.PostSteps.List()...); err != nil {
		return xerrors.Errorf("failed to apply schema post-steps (%v) in the destination PostgreSQL: %w", src.PostSteps.List(), err)
	}
	logger.Log.Info("Successfully applied schema post-steps in the destination PostgreSQL", log.Array("steps", src.PostSteps.List()))
	return nil
}

// extract type name from query looked as CREATE TYPE <typename> ...
func extractTypeName(createTypeSQL string) string {
	cutSQL := strings.TrimPrefix(createTypeSQL, "\n--\n") // erase pg_dump redundant symbols
	cutSQL = strings.TrimPrefix(cutSQL, "CREATE TYPE ")   // <typename> ...
	parts := splitSQLBySeparator(cutSQL, " ")             // ["<typename>", ...]
	return parts[0]
}

func determineExcludedTypes(allTypes []*pgDumpItem, allowedTypes []*pgDumpItem) *set.Set[string] {
	excludedTypes := set.New[string]()
	for _, t := range allTypes {
		typeName := extractTypeName(t.Body)
		if t.Schema == "public" {
			cutName := strings.TrimPrefix(typeName, "public.")
			excludedTypes.Add(cutName)
		}
		excludedTypes.Add(typeName)
	}
	for _, t := range allowedTypes {
		typeName := extractTypeName(t.Body)
		if t.Schema == "public" {
			cutName := strings.TrimPrefix(typeName, "public.")
			excludedTypes.Remove(cutName)
		}
		excludedTypes.Remove(typeName)
	}

	return excludedTypes
}

// loadPgDumpSchema actually loads the schema from PostgreSQL source using a storage constructed in-place
func loadPgDumpSchema(ctx context.Context, src *PgSource, transfer *model.Transfer) ([]*pgDumpItem, error) {
	storage, err := NewStorage(src.ToStorageParams(transfer))
	if err != nil {
		return nil, xerrors.Errorf("failed to create a PostgreSQL Storage object: %w", err)
	}
	defer storage.Close()

	tx, err := storage.Conn.BeginTx(ctx, pgx.TxOptions{IsoLevel: pgx.ReadCommitted, AccessMode: pgx.ReadWrite, DeferrableMode: pgx.NotDeferrable})
	if err != nil {
		return nil, xerrors.Errorf("failed to BEGIN transaction: %w", err)
	}
	defer func() {
		// we always ROLLBACK the schema retrieval transaction, it should not change anything in the source database even though it is read-write. So no rollbacks usage is necessary
		err := tx.Rollback(ctx)
		if err != nil {
			logger.Log.Warn("failed to ROLLBACK transaction", log.Error(err))
		}
	}()

	connString, secretPass, err := PostgresDumpConnString(src)
	if err != nil {
		return nil, xerrors.Errorf("failed to build PostgreSQL connection string: %w", err)
	}

	seqs, err := listAllSequences(ctx, src, tx.Conn())
	if err != nil {
		return nil, xerrors.Errorf("failed to list all SEQUENCEs: %w", err)
	}
	tablesIncluded, err := resolveTablesIncluded(src, transfer)
	if err != nil {
		return nil, xerrors.Errorf("unable to resolve included tables: %w", err)
	}
	seqsIncluded, seqsExcluded := filterSequences(seqs, abstract.NewIntersectionIncludeable(src, transfer))

	hasTableFilter := len(tablesIncluded) > 0
	userDefinedItems, err := dumpDefinedItems(connString, secretPass, src, hasTableFilter)
	if err != nil {
		return nil, xerrors.Errorf("failed to dump defined items: %w", err)
	}

	tablesSchemas := set.New[string]()
	for _, tableID := range tablesIncluded {
		tablesSchemas.Add(tableID.Namespace)
	}

	var result []*pgDumpItem
	var userDefinedBeforeTables []*pgDumpItem
	var userDefinedAfterTables []*pgDumpItem
	if len(userDefinedItems) > 0 {
		userDefinedFiltered, err := filterUserDefinedItemsInOrder(ctx, userDefinedItems, src, tablesSchemas)
		if err != nil {
			return nil, xerrors.Errorf("unable to filter user defined items: %w", err)
		}
		for _, d := range userDefinedFiltered {
			switch d.Typ {
			case string(Function), string(Cast):
				userDefinedAfterTables = append(userDefinedAfterTables, d)
			default:
				userDefinedBeforeTables = append(userDefinedBeforeTables, d)
			}
		}
		result = append(result, userDefinedBeforeTables...)
	}

	pgDumpArgs, err := pgDumpSchemaArgs(src, tablesIncluded, seqsIncluded, seqsExcluded)
	if err != nil {
		return nil, xerrors.Errorf("failed to compose arguments for pg_dump: %w", err)
	}
	dump, err := execPgDump(src.PgDumpCommand, connString, secretPass, pgDumpArgs)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute pg_dump to get schema: %w", err)
	}
	if len(tablesIncluded) == 0 {
		result = append(result, dump...)
	} else {
		result = append(result, filterDump(dump, abstract.NewIntersectionIncludeable(src, transfer))...)
	}

	result = append(result, userDefinedAfterTables...)

	if shouldDumpSequenceValues(src) {
		sequenceValuesDump, err := dumpSequenceValues(ctx, tx.Conn(), seqsIncluded)
		if err != nil {
			return nil, xerrors.Errorf("failed to dump current SEQUENCE values: %w", err)
		}
		result = append(result, sequenceValuesDump...)
	}

	return result, nil
}

// shouldDumpSequenceValues returns true only when at least one phase (Pre or Post) creates Sequence and sets their current values (SequenceSet).
func shouldDumpSequenceValues(src *PgSource) bool {
	preSeqSet := src.PreSteps.SequenceSet == nil || *src.PreSteps.SequenceSet
	postSeqSet := src.PostSteps.SequenceSet == nil || *src.PostSteps.SequenceSet
	return (src.PreSteps.Sequence && preSeqSet) || (src.PostSteps.Sequence && postSeqSet)
}

func resolveExcludedTypes(ctx context.Context, dump []*pgDumpItem, src *PgSource, tablesSchemas *set.Set[string]) (*set.Set[string], error) {
	allTypes := yaslices.Filter(dump, func(i *pgDumpItem) bool { return i.Typ == "TYPE" })
	allowedTypes, err := dumpUserDefinedTypes(ctx, allTypes, src, tablesSchemas)
	if err != nil {
		return nil, err
	}
	return determineExcludedTypes(allTypes, allowedTypes), nil
}

// filterUserDefinedItemsInOrder filters the ordered user-defined dump (COLLATION, TYPE, FUNCTION, CAST)
// preserving pg_dump order so that dependency order (e.g. function before domain type) is kept.
func filterUserDefinedItemsInOrder(ctx context.Context, dump []*pgDumpItem, src *PgSource, tablesSchemas *set.Set[string]) ([]*pgDumpItem, error) {
	if len(dump) == 0 || tablesSchemas.Empty() || src == nil {
		return nil, nil
	}
	pre := src.PreSteps
	post := src.PostSteps
	if pre == nil && post == nil {
		return nil, nil
	}
	wantType := (pre != nil && pre.Type) || (post != nil && post.Type)
	wantCollation := (pre != nil && pre.Collation) || (post != nil && post.Collation)
	wantFunction := (pre != nil && pre.Function) || (post != nil && post.Function)
	wantCast := (pre != nil && pre.Cast) || (post != nil && post.Cast)

	excludedTypes, err := resolveExcludedTypes(ctx, dump, src, tablesSchemas)
	if err != nil {
		return nil, err
	}

	result := make([]*pgDumpItem, 0, len(dump))
	for _, d := range dump {
		switch d.Typ {
		case string(Collation):
			if wantCollation && tablesSchemas.Contains(d.Schema) {
				result = append(result, d)
			}
		case string(Type):
			if wantType && tablesSchemas.Contains(d.Schema) {
				result = append(result, d)
			}

		case string(Function):
			if wantFunction && tablesSchemas.Contains(d.Schema) && isAllowedFunction(d, excludedTypes) {
				result = append(result, d)
			}
		case string(Cast):
			if wantCast && isAllowedCast(d.Body, excludedTypes, tablesSchemas) {
				result = append(result, d)
			}
		}
	}
	return result, nil
}

// listAllSequences constructs a pg Storage in-place and obtains all (accessible) SEQUENCEs
func listAllSequences(ctx context.Context, src *PgSource, conn *pgx.Conn) (SequenceMap, error) {
	if !src.PreSteps.Sequence && !src.PostSteps.Sequence {
		return make(SequenceMap), nil
	}

	return ListSequencesWithDependants(ctx, conn, src.KeeperSchema)
}

// filterSequences separates the given sequences into included and excluded ones by applying the given filter
func filterSequences(sequences SequenceMap, filter abstract.Includeable) (included []abstract.TableID, excluded []abstract.TableID) {
	for _, sequenceInfo := range sequences {
		sequenceIncluded := false
		if len(sequenceInfo.DependentTables) == 0 {
			// special case for a SEQUENCE which is not used by any table
			sequenceIncluded = filter.Include(*abstract.NewTableID(sequenceInfo.SequenceID.Namespace, ""))
		}
		for _, table := range sequenceInfo.DependentTables {
			if filter.Include(table) {
				logger.Log.Info("Sequence included", log.String("sequence", sequenceInfo.SequenceID.Fqtn()), log.String("table", table.Fqtn()))
				sequenceIncluded = true
				break
			}
		}
		if sequenceIncluded {
			included = append(included, sequenceInfo.SequenceID)
		} else {
			excluded = append(excluded, sequenceInfo.SequenceID)
		}
	}
	slices.SortStableFunc(included, abstract.TableID.Less)
	slices.SortStableFunc(excluded, abstract.TableID.Less)
	return included, excluded
}

func dumpUserDefinedTypes(ctx context.Context, dumpedTypes []*pgDumpItem, src *PgSource, tablesSchemas *set.Set[string]) ([]*pgDumpItem, error) {
	if tablesSchemas.Empty() || (!src.PreSteps.Type && !src.PostSteps.Type) {
		return nil, nil
	}

	if containsUserDefinedTypes, err := pgContainsUserDefinedTypes(ctx, src); !containsUserDefinedTypes || err != nil {
		return nil, err
	}

	result := make([]*pgDumpItem, 0)

	for _, d := range dumpedTypes {
		if tablesSchemas.Contains(d.Schema) {
			result = append(result, d)
		}
	}

	return result, nil
}

func dumpCollations(collations []*pgDumpItem, tablesSchemas *set.Set[string]) []*pgDumpItem {
	result := make([]*pgDumpItem, 0, len(collations))

	for _, c := range collations {
		if tablesSchemas.Contains(c.Schema) {
			result = append(result, c)
		}
	}

	return result
}

// parse and validate types in cast
func isAllowedCast(createCastSQL string, excludedTypes *set.Set[string], tablesSchemas *set.Set[string]) bool {
	cleanedStatement := strings.TrimPrefix(createCastSQL, "\n--\nCREATE CAST (")
	parts := splitSQLBySeparator(cleanedStatement, " AS ")

	if len(parts) < 2 {
		logger.Log.Warnf("unsupported definition for create cast : %v", createCastSQL)
		return false
	}

	sourceType := parts[0]
	if excludedTypes.Contains(sourceType) {
		return false
	}

	partsByBracket := splitSQLBySeparator(parts[1], ") ")
	targetType := partsByBracket[0]
	if excludedTypes.Contains(targetType) {
		return false
	}
	// check if create cast with function
	if len(partsByBracket) < 2 {
		return false
	}
	partsByFunction := splitSQLBySeparator(partsByBracket[1], "FUNCTION ")
	if len(partsByFunction) == 1 {
		return true
	}
	schemaPart := splitSQLBySeparator(partsByFunction[1], ".")

	return tablesSchemas.Contains(schemaPart[0])
}

func dumpDefinedItems(connString string, connPass model.SecretString, src *PgSource, hasTableFilter bool) ([]*pgDumpItem, error) {
	if !hasTableFilter {
		return nil, nil
	}
	args := []string{
		"--no-publications",
		"--no-subscriptions",
		"--format=plain",
		"--no-owner",
		"--schema-only",
		"-T",
		"*.*",
	}

	dump, err := execPgDump(src.PgDumpCommand, connString, connPass, args)
	if err != nil {
		return nil, xerrors.Errorf("failed to execute pg_dump to get user-defined entities: %w", err)
	}
	return dump, nil
}

// strings.Split without considering the separator inside the quotes
func splitSQLBySeparator(SQL string, sep string) []string {
	result := make([]string, 0)

	parts := strings.Split(SQL, sep)

	cur := ""
	for _, i := range parts {
		qouteCntBefore := strings.Count(cur, "\"")
		if qouteCntBefore%2 != 0 {
			cur += sep
		}
		cur += i
		quoteCntAfter := strings.Count(cur, "\"")
		if quoteCntAfter%2 == 0 {
			result = append(result, cur)
			cur = ""
		}
	}

	if cur != "" {
		result = append(result, cur)
	}

	return result
}

func isArgMode(argPart string) bool {
	return argPart == "IN" || argPart == "OUT" || argPart == "INOUT" || argPart == "VARIADIC"
}

// parse args in ... FUNCTION <functionName>([argMode1] [argName1] arg1, [argMode2] [argName2] arg2, ..., [argMode] [argName] arg) ... argMode and argName is optional
func extractFunctionArgsTypes(functionBody string) ([]string, bool) {
	nameWithoutCloseBracket := splitSQLBySeparator(functionBody, ")")
	argsParts := splitSQLBySeparator(nameWithoutCloseBracket[0], "(")
	if len(argsParts) < 2 {
		return nil, false
	}
	argsWithNames := splitSQLBySeparator(argsParts[1], ", ")

	result := make([]string, 0, len(argsWithNames))
	for _, argWithName := range argsWithNames {
		splitArg := splitSQLBySeparator(argWithName, " ")
		if len(splitArg) == 0 {
			return nil, false
		}
		if isArgMode(splitArg[0]) {
			splitArg = splitArg[1:]
		}
		if len(splitArg) == 0 {
			return nil, false
		}

		argType := splitArg[0]
		if len(splitArg) == 2 {
			argType = splitArg[1]
		}
		argType = strings.TrimSuffix(argType, "[]")
		result = append(result, argType)
	}

	// return arg1, arg_2, ..., arg
	return result, true
}

// function is allowed if all types of args and the returned type are allowed
func isAllowedFunction(function *pgDumpItem, excludedTypes *set.Set[string]) bool {
	argsTypes, isOk := extractFunctionArgsTypes(function.Body)
	if !isOk {
		logger.Log.Warnf("unable to extract function args types for %v", function.Body)
		return false
	}
	for _, t := range argsTypes {
		if excludedTypes.Contains(t) {
			return false
		}
	}

	parts := splitSQLBySeparator(function.Body, "RETURNS")
	if len(parts) < 2 {
		logger.Log.Warnf("unsupported definition for create function : %v", function.Body)
		return false
	}

	partWithReturnedType := strings.TrimSpace(parts[1])

	returnedType := splitSQLBySeparator(partWithReturnedType, "\n")[0]
	returnedType = strings.TrimSuffix(returnedType, "[]")

	return !excludedTypes.Contains(returnedType)
}

func dumpFunctions(functions []*pgDumpItem, src *PgSource, excludedTypes *set.Set[string], schemas *set.Set[string]) []*pgDumpItem {
	if schemas.Empty() || (!src.PreSteps.Function && !src.PostSteps.Function) {
		return nil
	}

	result := make([]*pgDumpItem, 0)

	for _, f := range functions {
		if schemas.Contains(f.Schema) && isAllowedFunction(f, excludedTypes) {
			result = append(result, f)
		}
	}

	return result
}

func dumpCasts(definedCasts []*pgDumpItem, src *PgSource, excludedTypes *set.Set[string], tablesSchemas *set.Set[string]) []*pgDumpItem {
	if tablesSchemas.Empty() || (!src.PreSteps.Cast && !src.PostSteps.Cast) {
		return nil
	}
	result := make([]*pgDumpItem, 0, len(definedCasts))

	for _, c := range definedCasts {
		if isAllowedCast(c.Body, excludedTypes, tablesSchemas) {
			result = append(result, c)
		}
	}

	return result
}

func filterDump(dump []*pgDumpItem, filter abstract.Includeable) []*pgDumpItem {
	result := make([]*pgDumpItem, 0, len(dump))
	createdIndexes := set.New[string]()

	for _, i := range dump {
		switch i.Typ {
		case "TABLE_ATTACH":
			catSQL := strings.TrimPrefix(i.Body, "\n--\nALTER TABLE ONLY ")
			splitSQL := splitSQLBySeparator(catSQL, " ATTACH")
			parentTable := splitSQL[0]

			tableID, err := abstract.NewTableIDFromStringPg(parentTable, false)
			if err != nil {
				logger.Log.Warnf("unable to parse table id from %s: %s", parentTable, err.Error())
				continue
			}

			if !filter.Include(*tableID) {
				logger.Log.Infof("table attachment for %s skipped", parentTable)
				continue
			}
		case "INDEX":
			catSQL := strings.TrimPrefix(i.Body, "\n--\nCREATE INDEX ")
			splitSQL := splitSQLBySeparator(catSQL, " ON ")
			indexFullName := i.Schema + "." + splitSQL[0]

			if createdIndexes.Contains(indexFullName) {
				continue
			}
			createdIndexes.Add(indexFullName)
		case "INDEX_ATTACH":
			catSQL := strings.TrimPrefix(i.Body, "\n--\nALTER INDEX ")
			splitSQL := splitSQLBySeparator(catSQL, " ATTACH")
			indexFullName := splitSQL[0]

			if !createdIndexes.Contains(indexFullName) {
				continue
			}
		}

		result = append(result, i)
	}

	return result
}

func execPgDump(pgDump []string, connString string, password model.SecretString, args []string) ([]*pgDumpItem, error) {
	if len(pgDump) == 0 {
		pgDump = []string{"pg_dump"}
	}

	commandArgs := []string{}
	if len(pgDump) > 1 {
		commandArgs = append(commandArgs, pgDump[1:]...)
	}
	commandArgs = append(commandArgs, connString)
	commandArgs = append(commandArgs, args...)
	command := exec.Command(pgDump[0], commandArgs...)
	if password != "" {
		command.Env = append(os.Environ(), fmt.Sprintf("PGPASSWORD=%s", password))
	}
	var stdout, stderr bytes.Buffer
	command.Stdout = &stdout
	command.Stderr = &stderr

	logger.Log.Info("Run pg_dump", log.String("path", command.Path), log.Strings("args", command.Args))
	if err := command.Run(); err != nil {
		stderrBytes := stderr.Bytes()
		if bytes.Contains(stderrBytes, []byte("permission denied")) {
			// Map to coded error for better UX and docs linking
			err = abstract.NewFatalError(coded.Errorf(codes.PostgresPgDumpPermissionDenied, "failed to execute pg_dump: %w", err))
		}
		return nil, xerrors.Errorf("failed to execute pg_dump. STDERR:\n%s\nerror: %w", string(truncate(string(stderrBytes), 2000)), err)
	}
	pgDumpOut := parsePgDumpOut(&stdout)
	logPgDumpOut(pgDumpOut)
	return pgDumpOut, nil
}

func pgContainsUserDefinedTypes(ctx context.Context, src *PgSource) (bool, error) {
	var result bool
	conn, err := MakeConnPoolFromSrc(src, logger.Log)
	if err != nil {
		return false, xerrors.Errorf("failed to create a connection pool: %w", err)
	}
	defer conn.Close()

	checkType := func() error {
		err := conn.QueryRow(ctx, typesExistsQuery).Scan(&result)
		if err != nil {
			return xerrors.Errorf("failed to check user-defined types existence: %w", err)
		}
		return nil
	}
	err = backoff.Retry(checkType, backoff.WithMaxRetries(util.NewExponentialBackOff(), 3))
	if err != nil {
		return false, err
	}
	return result, nil
}

func isPgDumpItemValid(item *pgDumpItem) bool {
	return item != nil && item.Typ != ""
}

func parsePgDumpOut(out io.Reader) []*pgDumpItem {
	var res []*pgDumpItem
	scanner := bufio.NewScanner(out)
	var current *pgDumpItem
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "--") {
			// At start of section with dump item meta
			if isPgDumpItemValid(current) {
				res = append(res, current)
			}
			current = new(pgDumpItem)
			if !scanner.Scan() {
				return res
			}
			nameLine := scanner.Text()
			nameLine = strings.ReplaceAll(nameLine, "--", "")
			parts := strings.Split(nameLine, ";")
			for _, p := range parts {
				kv := strings.Split(p, ":")
				switch strings.TrimSpace(kv[0]) {
				case "Name":
					current.Name = strings.TrimSpace(kv[1])
				case "Type":
					current.Typ = strings.ReplaceAll(strings.TrimSpace(kv[1]), " ", "_")
				case "Schema":
					current.Schema = strings.TrimSpace(kv[1])
				case "Owner":
					current.Owner = strings.TrimSpace(kv[1])
				}
			}
			_ = scanner.Scan()
		}
		if len(line) == 0 {
			continue
		}
		if current != nil {
			current.Body = current.Body + "\n" + line
			if current.Typ == "CONSTRAINT" && strings.Contains(line, "PRIMARY KEY") {
				current.Typ = "PRIMARY_KEY"
			}
		}
	}
	if isPgDumpItemValid(current) {
		res = append(res, current)
	}
	stat := map[string]int{}
	for _, item := range res {
		stat[item.Typ]++
	}
	for k, count := range stat {
		logger.Log.Infof("Found commands with type: %v - %v", k, count)
	}
	return res
}

func logPgDumpOut(items []*pgDumpItem) {
	for _, item := range items {
		jsonStr, _ := json.Marshal(*item)
		logger.Log.Infof("pg_dump item: %s", jsonStr)
	}
}
