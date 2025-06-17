//go:build !disable_mysql_provider

package mysql

import (
	"fmt"
	"strings"

	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/util"
)

const (
	batchSize        = 1 * 1024 * 1024
	maxDeleteInBatch = 500
	DisableFKQuery   = "SET FOREIGN_KEY_CHECKS=0;\n"
	EnableFKQuery    = "SET FOREIGN_KEY_CHECKS=1;\n"
)

type sinkQuery struct {
	query    string
	parallel bool
}

func newSinkQuery(query string, parallel bool) *sinkQuery {
	return &sinkQuery{
		query:    query,
		parallel: parallel,
	}
}

type keyChecksStatus int

const (
	keyChecksNoStatus = keyChecksStatus(0)
	keyChecksEnabled  = keyChecksStatus(1)
	keyChecksDisabled = keyChecksStatus(2)
)

type queryBuilder interface {
	TryAddItem(item *abstract.ChangeItem, wantedKind abstract.Kind) bool
	BuildQuery() *sinkQuery
	NeedKeyChecks() keyChecksStatus
}

type insertQueryBuilder struct {
	uniqConstraints    map[string]bool
	tableID            abstract.TableID
	columns            []abstract.ColSchema
	columnNameToIndex  map[string]int
	columnNamesEscaped []string
	conflictUpdate     string
	batch              []string
	batchSize          int
}

func newInsertQueryBuilder(
	uniqConstraints map[string]bool,
	tableID abstract.TableID,
	columns []abstract.ColSchema,
	columnNameToIndex map[string]int,
	columnNamesEscaped []string,
	conflictUpdate string,
) *insertQueryBuilder {
	return &insertQueryBuilder{
		uniqConstraints:    uniqConstraints,
		tableID:            tableID,
		columns:            columns,
		columnNameToIndex:  columnNameToIndex,
		columnNamesEscaped: columnNamesEscaped,
		conflictUpdate:     conflictUpdate,
		batch:              []string{},
		batchSize:          0,
	}
}

func (b *insertQueryBuilder) TryAddItem(item *abstract.ChangeItem, wantedKind abstract.Kind) bool {
	if wantedKind != abstract.InsertKind {
		return false
	}

	part := buildPartOfQueryInsert(&b.columns, item, &b.columnNameToIndex)
	partSize := len(part)
	if b.batchSize != 0 && partSize+b.batchSize > batchSize {
		return false
	}

	b.batch = append(b.batch, part)
	b.batchSize += partSize
	return true
}

func (b *insertQueryBuilder) BuildQuery() *sinkQuery {
	if len(b.batch) == 0 {
		return nil
	}

	if b.uniqConstraints[b.tableID.Fqtn()] {
		return newSinkQuery(fmt.Sprintf(
			"REPLACE `%v`.`%v` (%v) VALUES\n%v;",
			b.tableID.Namespace,
			b.tableID.Name,
			strings.Join(b.columnNamesEscaped, ","),
			strings.Join(b.batch, ",\n"),
		), false)
	} else {
		if len(b.conflictUpdate) != 0 {
			return newSinkQuery(fmt.Sprintf(
				"INSERT INTO `%v`.`%v` (%v) VALUES\n%v\nON DUPLICATE KEY UPDATE \n %v\n;",
				b.tableID.Namespace,
				b.tableID.Name,
				strings.Join(b.columnNamesEscaped, ","),
				strings.Join(b.batch, ",\n"),
				b.conflictUpdate,
			), true)
		} else {
			return newSinkQuery(fmt.Sprintf(
				"INSERT IGNORE INTO `%v`.`%v` (%v) VALUES\n%v;",
				b.tableID.Namespace,
				b.tableID.Name,
				strings.Join(b.columnNamesEscaped, ","),
				strings.Join(b.batch, ",\n"),
			), true)
		}
	}
}

func (b *insertQueryBuilder) NeedKeyChecks() keyChecksStatus {
	return keyChecksDisabled
}

type updateQueryBuilder struct {
	tableID           abstract.TableID
	columns           []abstract.ColSchema
	columnNameToIndex map[string]int
	batch             []string
	batchSize         int
}

func newUpdateQueryBuilder(
	tableID abstract.TableID,
	columns []abstract.ColSchema,
	columnNameToIndex map[string]int,
) *updateQueryBuilder {
	return &updateQueryBuilder{
		tableID:           tableID,
		columns:           columns,
		columnNameToIndex: columnNameToIndex,
		batch:             []string{},
		batchSize:         0,
	}
}

func (b *updateQueryBuilder) TryAddItem(item *abstract.ChangeItem, wantedKind abstract.Kind) bool {
	if wantedKind != abstract.UpdateKind {
		return false
	}

	query, notEmpty := buildQueryUpdate(b.tableID, &b.columns, item, &b.columnNameToIndex)
	if !notEmpty {
		return true
	}

	querySize := len(query)
	if b.batchSize != 0 && querySize+b.batchSize > batchSize {
		return false
	}

	b.batch = append(b.batch, query)
	b.batchSize += querySize
	return true
}

func (b *updateQueryBuilder) BuildQuery() *sinkQuery {
	if len(b.batch) == 0 {
		return nil
	}

	return newSinkQuery(strings.Join(b.batch, "\n"), false)
}

func (b *updateQueryBuilder) NeedKeyChecks() keyChecksStatus {
	return keyChecksNoStatus
}

type deleteQueryBuilder struct {
	skipKeyChecks     bool
	tableID           abstract.TableID
	columns           []abstract.ColSchema
	columnNameToIndex map[string]int
	batch             []string
}

func newDeleteQueryBuilder(
	skipKeyChecks bool,
	tableID abstract.TableID,
	columns []abstract.ColSchema,
	columnNameToIndex map[string]int,
) *deleteQueryBuilder {
	return &deleteQueryBuilder{
		skipKeyChecks:     skipKeyChecks,
		tableID:           tableID,
		columns:           columns,
		columnNameToIndex: columnNameToIndex,
		batch:             []string{},
	}
}

func (b *deleteQueryBuilder) TryAddItem(item *abstract.ChangeItem, wantedKind abstract.Kind) bool {
	if wantedKind != abstract.DeleteKind {
		return false
	}

	if len(b.batch) != 0 && len(b.batch)+1 > maxDeleteInBatch {
		return false
	}

	part := buildPartOfQueryDeleteCondition(&b.columns, item, &b.columnNameToIndex)
	b.batch = append(b.batch, part)
	return true
}

func (b *deleteQueryBuilder) BuildQuery() *sinkQuery {
	if len(b.batch) == 0 {
		return nil
	}

	return newSinkQuery(fmt.Sprintf(
		"DELETE FROM `%v`.`%v` WHERE %v;",
		b.tableID.Namespace,
		b.tableID.Name,
		strings.Join(b.batch, " OR "),
	), false)
}

func (b *deleteQueryBuilder) NeedKeyChecks() keyChecksStatus {
	if b.skipKeyChecks {
		return keyChecksNoStatus
	}
	return keyChecksEnabled
}

type keyChecksHandler struct {
	currentStatus keyChecksStatus
}

func newKeyChecksHandler() *keyChecksHandler {
	return &keyChecksHandler{
		currentStatus: keyChecksNoStatus,
	}
}

func (h *keyChecksHandler) statusToQuery() (*sinkQuery, error) {
	switch h.currentStatus {
	case keyChecksDisabled:
		return newSinkQuery(DisableFKQuery, false), nil
	case keyChecksEnabled:
		return newSinkQuery(EnableFKQuery, false), nil
	default:
		return nil, xerrors.Errorf("Unknown status: %v", h.currentStatus)
	}
}

func (h *keyChecksHandler) applyStatus(status keyChecksStatus) (*sinkQuery, error) {
	if status == keyChecksNoStatus {
		return nil, nil
	}

	switch h.currentStatus {
	case keyChecksNoStatus:
	case keyChecksDisabled:
		if status == keyChecksDisabled {
			return nil, nil
		}
	case keyChecksEnabled:
		if status == keyChecksEnabled {
			return nil, nil
		}
	default:
		return nil, xerrors.Errorf("Unknown handler status: %v", h.currentStatus)
	}

	h.currentStatus = status
	query, err := h.statusToQuery()
	if err != nil {
		return nil, xerrors.Errorf("Can't get query for status '%v': %w", h.currentStatus, err)
	}
	return query, nil
}

type queriesBuilder struct {
	sink               *sinker
	table              abstract.TableID
	columns            []abstract.ColSchema
	columnNameToIndex  map[string]int
	columnNamesEscaped []string
	conflictUpdate     string

	keyChecks      *keyChecksHandler
	currentBuilder queryBuilder
	queries        []sinkQuery
}

func newQueriesBuilder(sink *sinker, table abstract.TableID, columns []abstract.ColSchema) *queriesBuilder {
	return &queriesBuilder{
		sink:               sink,
		table:              table,
		columns:            columns,
		columnNameToIndex:  columnNameToIndex(&columns),
		columnNamesEscaped: MakeArrBacktickedColumnNames(&columns),
		conflictUpdate:     strings.Join(buildStatementOnDuplicateKeyUpdate(&columns), ",\n"),
		keyChecks:          nil,
		currentBuilder:     nil,
		queries:            nil,
	}
}

func (b *queriesBuilder) buildQuery() error {
	if b.currentBuilder == nil {
		return nil
	}

	query := b.currentBuilder.BuildQuery()
	if query != nil {
		keyChecksQuery, err := b.keyChecks.applyStatus(b.currentBuilder.NeedKeyChecks())
		if err != nil {
			return xerrors.Errorf("Can't apply builder '%T' key checks status '%v': %w",
				b.currentBuilder, b.currentBuilder.NeedKeyChecks(), err)
		}
		if keyChecksQuery != nil {
			b.queries = append(b.queries, *keyChecksQuery)
		}
		b.queries = append(b.queries, *query)
	}
	return nil
}

func (b *queriesBuilder) addItem(builderFactory func() queryBuilder, item *abstract.ChangeItem, wantedKind abstract.Kind) error {
	if b.currentBuilder == nil {
		b.currentBuilder = builderFactory()
	}
	if added := b.currentBuilder.TryAddItem(item, wantedKind); !added {
		if err := b.buildQuery(); err != nil {
			return xerrors.Errorf("Can't build query, builder '%T', wanted kind '%v': %w", b.currentBuilder, wantedKind, err)
		}
		b.currentBuilder = builderFactory()
		if added = b.currentBuilder.TryAddItem(item, wantedKind); !added {
			return xerrors.Errorf("Can't add first item to builder '%T', wanted kind '%v'", b.currentBuilder, wantedKind)
		}
	}
	return nil
}

func (b *queriesBuilder) BuildQueries(items []abstract.ChangeItem) ([]sinkQuery, error) {
	b.keyChecks = newKeyChecksHandler()
	b.queries = []sinkQuery{}

	createInsertQueryBuilder := func() queryBuilder {
		return newInsertQueryBuilder(b.sink.uniqConstraints, b.table, b.columns, b.columnNameToIndex, b.columnNamesEscaped, b.conflictUpdate)
	}
	createUpdateQueryBuilder := func() queryBuilder {
		return newUpdateQueryBuilder(b.table, b.columns, b.columnNameToIndex)
	}
	createDeleteQueryBuilder := func() queryBuilder {
		return newDeleteQueryBuilder(b.sink.config.SkipKeyChecks, b.table, b.columns, b.columnNameToIndex)
	}

	for _, item := range items {
		switch item.Kind {
		case abstract.InsertKind, abstract.UpdateKind:
			pKeyChanged := item.KeysChanged()
			if len(item.ColumnNames) != len(b.columns) || pKeyChanged {
				// We have toasted changeItem, it must tracked as pure update instead upsert
				// TODO - do we really should care in mysql-sink about pg-toast column?
				//     looks like pg-source should convert insert-to-update in toasted-case
				if err := b.addItem(createUpdateQueryBuilder, &item, abstract.UpdateKind); err != nil {
					return nil, xerrors.Errorf("Can't process update item: %w", err)
				}
			} else {
				if err := b.addItem(createInsertQueryBuilder, &item, abstract.InsertKind); err != nil {
					return nil, xerrors.Errorf("Can't process insert item: %w", err)
				}
			}
		case abstract.DeleteKind:
			if err := b.addItem(createDeleteQueryBuilder, &item, abstract.DeleteKind); err != nil {
				return nil, xerrors.Errorf("Can't process delete item: %w", err)
			}
		}
	}
	if err := b.buildQuery(); err != nil {
		return nil, xerrors.Errorf("Can't process remaining builder: %w", err)
	}

	return b.queries, nil
}

func (s *sinker) buildQueries(
	table abstract.TableID,
	columns []abstract.ColSchema,
	items []abstract.ChangeItem,
) ([]sinkQuery, error) {
	builder := newQueriesBuilder(s, table, columns)
	queries, err := builder.BuildQueries(items)
	if err != nil {
		return nil, xerrors.Errorf("Can't build queries: %w", err)
	}
	return queries, nil
}

func columnNameToIndex(tableSchema *[]abstract.ColSchema) map[string]int {
	rev := make(map[string]int)
	for i, v := range *tableSchema {
		rev[v.ColumnName] = i
	}
	return rev
}

// Build condition for DELETE query
//
//	changeItem.Kind passed to function input - should be "delete"
//
// If there are present primary key - condition includes only primaryKey: "(`pKey0`=pVal0 AND `pKey1`=pVal1, ...)"
// If there are no primary key - condition includes all keys&values in changeItem: "(`key0`=val0 AND `key1`=val1, ...)"
func buildPartOfQueryDeleteCondition(tableSchema *[]abstract.ColSchema, changeItem *abstract.ChangeItem, colNameToIndexInTableSchema *map[string]int) string {
	currentConditions := make([]string, len(changeItem.OldKeys.KeyNames))
	for i, column := range changeItem.OldKeys.KeyNames {
		currentConditions[i] = fmt.Sprintf(
			"`%v`=%v",
			changeItem.OldKeys.KeyNames[i],
			CastToMySQL(changeItem.OldKeys.KeyValues[i], (*tableSchema)[(*colNameToIndexInTableSchema)[column]]),
		)
	}
	return "(" + strings.Join(currentConditions, " AND ") + ")"
}

func buildQueryUpdate(table abstract.TableID, tableSchema *[]abstract.ColSchema, changeItem *abstract.ChangeItem, colNameToIndexInTableSchema *map[string]int) (string, bool) {
	predicate := make([]string, 0)
	for i, column := range changeItem.OldKeys.KeyNames {
		columnIndex := (*colNameToIndexInTableSchema)[column]
		if (*tableSchema)[columnIndex].PrimaryKey {
			value := CastToMySQL(changeItem.OldKeys.KeyValues[i], (*tableSchema)[(*colNameToIndexInTableSchema)[column]])
			predicate = append(predicate, fmt.Sprintf("`%v` = %v", column, value))
		}
	}
	if len(predicate) == 0 { // does not have primary keys
		for i, column := range changeItem.OldKeys.KeyNames {
			value := CastToMySQL(changeItem.OldKeys.KeyValues[i], (*tableSchema)[(*colNameToIndexInTableSchema)[column]])
			predicate = append(predicate, fmt.Sprintf("`%v` = %v", column, value))
		}
	}

	setters := make([]string, 0)
	oldColumnValueByName := make(map[string]interface{})
	for i, column := range changeItem.OldKeys.KeyNames {
		oldColumnValueByName[column] = changeItem.OldKeys.KeyValues[i]
	}
	for i, column := range changeItem.ColumnNames {
		newValueCasted := CastToMySQL(changeItem.ColumnValues[i], (*tableSchema)[(*colNameToIndexInTableSchema)[column]])
		if oldValue, ok := oldColumnValueByName[column]; ok {
			oldValueCasted := CastToMySQL(oldValue, (*tableSchema)[(*colNameToIndexInTableSchema)[column]])
			if oldValueCasted != newValueCasted {
				setters = append(setters, fmt.Sprintf("`%v` = %v", column, newValueCasted))
			}
		} else {
			setters = append(setters, fmt.Sprintf("`%v` = %v", column, newValueCasted))
		}
	}

	if len(predicate) != 0 && len(setters) != 0 {
		update := fmt.Sprintf(
			"UPDATE IGNORE `%v`.`%v` SET %v WHERE %v;",
			table.Namespace,
			table.Name,
			strings.Join(setters, ", "),
			strings.Join(predicate, " AND "),
		)
		return update, true
	}
	return "", false
}

// When we are here, guaranteed len(tableSchema)==len(ColumnNames)
func buildPartOfQueryInsert(tableSchema *[]abstract.ColSchema, changeItem *abstract.ChangeItem, colNameToIndexInTableSchema *map[string]int) string {
	vals := make([]string, len(changeItem.ColumnNames))
	for i, n := range changeItem.ColumnNames {
		vals[(*colNameToIndexInTableSchema)[n]] = CastToMySQL(changeItem.ColumnValues[i], (*tableSchema)[(*colNameToIndexInTableSchema)[n]])
	}
	return fmt.Sprintf("(%v)", strings.Join(vals, ","))
}

func breakArrOfStringIntoBatchesByMaxSumLen(strings []string, maxSumLength int) [][]string {
	batches := make([][]string, 0)
	batchLength := 0
	lastIndex := 0
	for i, currString := range strings {
		if batchLength+len(currString) > maxSumLength {
			if i != 0 { // see tests - without it, if 0th elem more than maxSumLength - will be generated empty batch
				batches = append(batches, strings[lastIndex:i])
			}
			lastIndex = i
			batchLength = 0
		} else {
			batchLength += len(currString)
		}
	}
	if lastIndex < len(strings) {
		batches = append(batches, strings[lastIndex:])
	}
	return batches
}

func buildDeleteQueries(table abstract.TableID, deleteConditions []string) []string {
	var result []string
	for i := 0; i < len(deleteConditions); i += maxDeleteInBatch {
		end := i + maxDeleteInBatch

		if end > len(deleteConditions) {
			end = len(deleteConditions)
		}
		deleteQuery := fmt.Sprintf(
			"DELETE FROM `%v`.`%v` WHERE %v;",
			table.Namespace,
			table.Name,
			strings.Join(deleteConditions[i:end], " OR "),
		)
		result = append(result, deleteQuery)
	}
	return result
}

func buildStatementOnDuplicateKeyUpdate(tableSchema *[]abstract.ColSchema) []string {
	conflictUpd := make([]string, 0)
	for _, col := range *tableSchema {
		if !col.PrimaryKey && col.Expression == "" { // if we have "usual" field (not a pkey & not a generated column)
			conflictUpd = append(conflictUpd, fmt.Sprintf("`%v` = VALUES(`%v`)", col.ColumnName, col.ColumnName))
		}
	}
	return conflictUpd
}

func setFqtn(ddl string, sourceTableID, destinationTableID abstract.TableID, needSetDB bool) string {
	if sourceTableID == destinationTableID && !needSetDB {
		return ddl
	}

	res := ""
	for _, statement := range util.SplitStatements(ddl) {
		if statement == "" {
			continue
		}
		var fixedStatement string
		idx, sourceFqtn := findFqtn(statement, sourceTableID)
		if idx == -1 {
			fixedStatement = statement
		} else {
			fixedStatement = strings.Replace(
				statement,
				sourceFqtn,
				fmt.Sprintf("`%v`.`%v`", destinationTableID.Namespace, destinationTableID.Name),
				1,
			)
		}
		res += fmt.Sprintf("%v;", fixedStatement)
	}
	return res
}

func findFqtn(ddl string, table abstract.TableID) (int, string) {
	variants := []string{
		fmt.Sprintf("`%v`.`%v`", table.Namespace, table.Name),
		fmt.Sprintf("`%v.%v`", table.Namespace, table.Name),
		fmt.Sprintf("%v.%v", table.Namespace, table.Name),
		fmt.Sprintf("`%v`", table.Name),
		fmt.Sprintf("%v", table.Name),
	}

	for _, fqtn := range variants {
		idx := strings.Index(ddl, fqtn)
		if idx != -1 && isFqtn(ddl, fqtn, idx) {
			return idx, fqtn
		}
	}
	return -1, ""
}

func isFqtn(ddl, fqtn string, i int) bool {
	separateSymbols := " ;,.()[]{}\\/\n"
	if i > 0 && !strings.ContainsAny(ddl[i-1:i], separateSymbols) {
		return false
	}
	if i+len(fqtn) < len(ddl) && !strings.ContainsAny(ddl[i+len(fqtn):i+len(fqtn)+1], separateSymbols) {
		return false
	}
	return true
}
