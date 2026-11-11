package ch_eo_common

import (
	"context"
	"database/sql"
	sql_driver "database/sql/driver"
	"fmt"
	"io"
	"net"
	"net/url"
	"strconv"
	"strings"
	"syscall"
	"time"

	clickhouse_go "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/antlr4-go/antlr/v4"
	"github.com/blang/semver/v4"
	"github.com/cenkalti/backoff/v4"
	"github.com/transferia/transferia/library/go/core/xerrors"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_errors "github.com/transferia/transferia/pkg/providers/clickhouse/errors"
	"github.com/transferia/transferia/pkg/providers/clickhouse/httpclient"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
	"github.com/transferia/transferia/pkg/providers/clickhouse/schema/ddl_parser/clickhouse_lexer"
	"github.com/transferia/transferia/pkg/util/token_regexp"
	"github.com/transferia/transferia/pkg/util/token_regexp/op"
	"github.com/twmb/murmur3"
)

// This classification is only for operations whose repetition cannot add or
// remove data. In particular, a network error never makes an INSERT retry safe.
func IsRetryableReadError(err error) bool {
	if err == nil || xerrors.Is(err, context.Canceled) || xerrors.Is(err, context.DeadlineExceeded) || abstract.IsFatal(err) {
		return false
	}
	var serverError *clickhouse_go.Exception
	if xerrors.As(err, &serverError) {
		switch serverError.Code {
		case 95, 209, 210, 279: // socket read, socket timeout, network error, connection attempts failed
			return true
		default:
			return false
		}
	}
	var responseError *httpclient.ResponseError
	if xerrors.As(err, &responseError) {
		return responseError.StatusCode == 502 || responseError.StatusCode == 503 || responseError.StatusCode == 504
	}
	if xerrors.Is(err, io.EOF) || xerrors.Is(err, io.ErrUnexpectedEOF) || xerrors.Is(err, sql_driver.ErrBadConn) || xerrors.Is(err, net.ErrClosed) || xerrors.Is(err, clickhouse_go.ErrAcquireConnTimeout) {
		return true
	}
	if xerrors.Is(err, syscall.ECONNRESET) || xerrors.Is(err, syscall.EPIPE) || xerrors.Is(err, syscall.ECONNREFUSED) {
		return true
	}
	var networkError net.Error
	return xerrors.As(err, &networkError)
}

func Murmur128Hex(value string) string {
	firstHalf, secondHalf := murmur3.StringSum128(value)
	return fmt.Sprintf("%016x%016x", firstHalf, secondHalf)
}

// FindTablePartInsertBatchEnd returns the exclusive end of consecutive inserts for one table part.
// The item at batchBegin must be an Insert.
func FindTablePartInsertBatchEnd(items []abstract.ChangeItem, batchBegin int) int {
	firstItem := items[batchBegin]
	batchEnd := batchBegin + 1
	for {
		if batchEnd >= len(items) {
			break
		}
		nextItem := items[batchEnd]
		if nextItem.Kind != abstract.InsertKind {
			break
		}
		if nextItem.Schema != firstItem.Schema || nextItem.Table != firstItem.Table || nextItem.PartID != firstItem.PartID {
			break
		}
		batchEnd++
	}
	return batchEnd
}

func QuoteIdentifier(value string) string {
	return "`" + strings.ReplaceAll(strings.ReplaceAll(value, "\\", "\\\\"), "`", "\\`") + "`"
}

func QuoteStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, "\\", "\\\\"), "'", "\\'") + "'"
}

// ExtractTableDefinitionParts parses DDL and extracts:
//
//   - columns part of DDL, for example: (id UInt64, name String, created_at DateTime DEFAULT now())
//   - engine, for example: ReplicatedMergeTree('/path', 'replica')
//   - tail - ORDER BY, PARTITION BY, SETTINGS, ...
func ExtractTableDefinitionParts(ddl string) (string, string, string, error) {
	tokens, err := significantSQLTokens(ddl, "CREATE TABLE")
	if err != nil {
		return "", "", "", err
	}
	headerMatches := token_regexp.NewTokenRegexp(
		[]any{
			"create", "table",
			op.Opt(op.Seq("if", "not", "exists")),
			op.Opt(op.Seq(op.AnyToken(), ".")), op.AnyToken(),
			op.Opt(op.Seq("uuid", op.AnyToken())),
			op.Opt(op.Seq("on", "cluster", op.AnyToken())),
			// Include ENGINE in the capture to retain comments after the columns.
			op.CapturingGroup(op.MatchParentheses(), "engine"),
			op.Opt("="),
		},
	).MatchAll(tokens)
	if headerMatches.Size() == 0 {
		return "", "", "", xerrors.New("unsupported CREATE TABLE syntax")
	}
	headerMatch := headerMatches.Index(0)
	engineTokenIndex := headerMatch.Length()
	if engineTokenIndex == len(tokens) || tokens[engineTokenIndex].GetTokenType() != clickhouse_lexer.ClickHouseLexerIDENTIFIER {
		return "", "", "", xerrors.New("missing table engine")
	}
	engineName := tokens[engineTokenIndex].GetText()
	if strings.HasPrefix(engineName, "`") || strings.HasPrefix(engineName, `"`) {
		return "", "", "", xerrors.New("unsupported quoted table engine")
	}
	engineMatch := token_regexp.NewTokenRegexp(
		[]any{
			op.AnyToken(), op.Opt(op.MatchParentheses()),
		},
	).MatchAll(tokens[engineTokenIndex:]).Index(0)
	afterEngineTokenIndex := engineTokenIndex + engineMatch.Length()
	if afterEngineTokenIndex < len(tokens) && (tokens[afterEngineTokenIndex].GetText() == "(" || tokens[afterEngineTokenIndex].GetText() == ")") {
		return "", "", "", xerrors.New("unbalanced engine arguments")
	}
	columnsAndEngineKeyword := headerMatch.CapturingGroups().GroupToText(ddl, 0)
	columnDefinitionsSQL := strings.TrimSpace(columnsAndEngineKeyword[:len(columnsAndEngineKeyword)-len("engine")])
	// ANTLR positions count runes, not bytes; slicing ddl directly corrupts
	// non-ASCII identifiers and string literals.
	ddlRunes := []rune(ddl)
	engineEndRuneIndex := tokens[afterEngineTokenIndex-1].GetStop() + 1
	engineExpression := string(ddlRunes[tokens[engineTokenIndex].GetStart():engineEndRuneIndex])
	tableClausesSQL := strings.TrimSpace(string(ddlRunes[engineEndRuneIndex:]))
	return columnDefinitionsSQL, engineExpression, tableClausesSQL, nil
}

func BuildCreateTableDDL(databaseName string, tableName string, tableUUID string, templateDDL string, engineExpression string) (string, error) {
	columnDefinitionsSQL, originalEngineExpression, tableClausesSQL, err := ExtractTableDefinitionParts(templateDDL)
	if err != nil {
		return "", xerrors.Errorf("BuildCreateTableDDL: %w", err)
	}
	if engineExpression == "" {
		engineExpression = originalEngineExpression
	}
	// A line comment after the columns must not swallow the new ENGINE clause.
	return fmt.Sprintf("CREATE TABLE %s.%s UUID %s %s\nENGINE = %s\n%s", QuoteIdentifier(databaseName), QuoteIdentifier(tableName), QuoteStringLiteral(tableUUID), columnDefinitionsSQL, engineExpression, tableClausesSQL), nil
}

// ValidateSnapshotTableDDL checks ClickHouse's canonical create_table_query
// before building a temporary replacement. Only a replication path is
// rename-sensitive; comments, defaults and the replica name do not affect it.
func ValidateSnapshotTableDDL(ddl string) error {
	columns, engine, clauses, err := ExtractTableDefinitionParts(ddl)
	if err != nil {
		return xerrors.Errorf("read snapshot table definition: %w", err)
	}
	columnTokens, err := significantSQLTokens(columns, "snapshot columns")
	if err != nil {
		return xerrors.Errorf("read snapshot columns: %w", err)
	}
	clauseTokens, err := significantSQLTokens(clauses, "snapshot table clauses")
	if err != nil {
		return xerrors.Errorf("read snapshot table clauses: %w", err)
	}
	// Column declarations begin with a name; the table tail begins with a clause.
	if hasSnapshotTTLClause(columnTokens[1:len(columnTokens)-1], true) || hasSnapshotTTLClause(clauseTokens, false) {
		return xerrors.New("target has unsupported TTL")
	}
	engineTokens, err := significantSQLTokens(engine, "snapshot engine arguments")
	if err != nil {
		return xerrors.Errorf("read snapshot engine arguments: %w", err)
	}
	if !strings.HasPrefix(engineTokens[0].GetText(), "Replicated") || len(engineTokens) < 3 {
		return nil
	}
	if engineTokens[2].GetTokenType() != clickhouse_lexer.ClickHouseLexerSTRING_LITERAL {
		return nil
	}
	explicitReplication := token_regexp.NewTokenRegexp([]any{
		op.AnyToken(), "(", op.CapturingGroup(op.AnyToken()),
	}).MatchAll(engineTokens[:3])
	if explicitReplication.Size() == 0 {
		return xerrors.New("unsupported snapshot replication arguments")
	}
	keeperPath := explicitReplication.Index(0).CapturingGroups().GroupToText(engine, 0)
	if strings.Contains(keeperPath, "{table}") || strings.Contains(keeperPath, "{database}") {
		return xerrors.New("target has rename-sensitive Keeper macros")
	}
	return nil
}

func hasSnapshotTTLClause(tokens []antlr.Token, columnDefinitions bool) bool {
	ttlClause := token_regexp.NewTokenRegexp([]any{"ttl"})
	unaryNot := token_regexp.NewTokenRegexp([]any{"not"})
	elementHeader := token_regexp.NewTokenRegexp([]any{op.Or(
		op.Seq("index", op.AnyToken()), op.Seq("constraint", op.AnyToken(), "check"), op.Seq("projection", op.AnyToken()),
	)})
	expressionPrefix := token_regexp.NewTokenRegexp([]any{op.Or(
		op.Seq("order", "by"), op.Seq("partition", "by"), op.Seq("sample", "by"), op.Seq("primary", "key"),
		op.Seq("not", op.Or("in", "like", "ilike", "between", "regexp", "rlike")), op.Seq("global", op.Opt("not"), "in"),
		"default", "alias", "materialized", "ephemeral", "and", "or", "between", "in", "like", "ilike", "regexp", "rlike", "is", "as", "collate",
	)})
	operator := token_regexp.NewTokenRegexp([]any{op.Or(
		"->", "*", ":", ",", "||", "-", ".", "==", "=", ">=", ">", "<=", "<", "!=", "<>", "%", "+", "?", "/",
	)})
	expectOperand := columnDefinitions
	atElementStart := columnDefinitions
	nestingDepth := 0
	for tokenIndex := 0; tokenIndex < len(tokens); tokenIndex++ {
		// Nested expressions and types cannot contain a column or table TTL clause.
		switch tokens[tokenIndex].GetText() {
		case "(", "[", "{":
			nestingDepth++
			continue
		case ")", "]", "}":
			nestingDepth--
			if nestingDepth == 0 {
				expectOperand = false
			}
			continue
		}
		if nestingDepth != 0 {
			continue
		}
		if atElementStart {
			atElementStart = false
			if matches := elementHeader.MatchAll(tokens[tokenIndex:min(tokenIndex+3, len(tokens))]); matches.Size() != 0 {
				tokenIndex += matches.Index(0).Length() - 1
				expectOperand = true
				continue
			}
		}
		atElementStart = columnDefinitions && tokens[tokenIndex].GetText() == ","
		currentToken := tokens[tokenIndex : tokenIndex+1]
		if !expectOperand && ttlClause.MatchAll(currentToken).Size() != 0 {
			return true
		}
		if !expectOperand {
			if matches := expressionPrefix.MatchAll(tokens[tokenIndex:min(tokenIndex+3, len(tokens))]); matches.Size() != 0 {
				tokenIndex += matches.Index(0).Length() - 1
				expectOperand = true
				continue
			}
		}
		if expectOperand && unaryNot.MatchAll(currentToken).Size() != 0 {
			continue
		}
		// Keywords can be ordinary identifiers: ORDER BY default TTL ... must
		// finish the default operand, whereas column DEFAULT ttl starts one.
		expectOperand = operator.MatchAll(currentToken).Size() != 0
	}
	return false
}

func MakeEngineNonReplicated(engineExpression string) (string, error) {
	tokens, err := significantSQLTokens(engineExpression, "engine arguments")
	if err != nil {
		return "", err
	}
	// Check balanced parentheses before capturing arbitrary argument tokens.
	syntaxMatches := token_regexp.NewTokenRegexp(
		[]any{
			op.AnyToken(), op.Opt(op.MatchParentheses()),
		},
	).MatchAll(tokens)
	if syntaxMatches.Size() == 0 || syntaxMatches.Index(0).Length() != len(tokens) {
		return "", xerrors.Errorf("unsupported engine syntax %q", engineExpression)
	}
	engineName := tokens[0].GetText()
	switch engineName {
	case "MergeTree", "ReplicatedMergeTree", "ReplacingMergeTree", "ReplicatedReplacingMergeTree",
		"VersionedCollapsingMergeTree", "ReplicatedVersionedCollapsingMergeTree":
	default:
		return "", xerrors.Errorf("unsupported snapshot table engine %q", engineExpression)
	}

	if len(tokens) == 1 {
		return strings.TrimPrefix(engineName, "Replicated") + "()", nil
	}
	enginePattern := []any{
		op.AnyToken(), "(",
		op.CapturingGroup(op.Star(op.AnyToken())),
		")",
	}
	hasExplicitReplicationArgs := strings.HasPrefix(engineName, "Replicated") && len(tokens) > 3 && tokens[2].GetTokenType() == clickhouse_lexer.ClickHouseLexerSTRING_LITERAL
	if hasExplicitReplicationArgs {
		// Explicit Keeper path and replica precede optional version/deletion
		// columns. Without string literals, all arguments belong to the engine.
		enginePattern = []any{
			op.AnyToken(), "(",
			op.AnyToken(), ",", op.AnyToken(),
			op.CapturingGroup(
				op.Opt(
					op.Seq(
						",",
						op.Plus(op.AnyToken()),
					),
				),
			),
			")",
		}
		if len(tokens) < 6 || tokens[4].GetTokenType() != clickhouse_lexer.ClickHouseLexerSTRING_LITERAL {
			return "", xerrors.Errorf("unsupported replication arguments in %q", engineExpression)
		}
	}
	engineMatches := token_regexp.NewTokenRegexp(enginePattern).MatchAll(tokens)
	if engineMatches.Size() == 0 || engineMatches.Index(0).Length() != len(tokens) {
		return "", xerrors.Errorf("unsupported engine arguments in %q", engineExpression)
	}
	nonReplicatedEngineArgs := engineMatches.Index(0).CapturingGroups().GroupToText(engineExpression, 0)
	if hasExplicitReplicationArgs {
		// The separator is captured together with the remaining arguments.
		nonReplicatedEngineArgs = strings.TrimSpace(strings.TrimPrefix(nonReplicatedEngineArgs, ","))
	}

	engineName = strings.TrimPrefix(engineName, "Replicated")
	if engineName == "MergeTree" && nonReplicatedEngineArgs != "" {
		return "", xerrors.New("legacy MergeTree arguments are unsupported")
	}
	return engineName + "(" + nonReplicatedEngineArgs + ")", nil
}

func ExistingSessionParams(sessionID string) url.Values {
	return url.Values{"session_id": {sessionID}, "session_check": {"1"}}
}

// A profile constraint cannot be fixed by retrying a temporary table creation.
func classifySnapshotQueryError(statement string, err error) error {
	var exception *clickhouse_go.Exception
	if strings.HasPrefix(statement, "CREATE TABLE ") && xerrors.As(err, &exception) && (exception.Code == 164 || exception.Code == 452) {
		return abstract.NewFatalError(xerrors.Errorf("exactly-once snapshot cannot create temporary ClickHouse tables: the user profile must allow database_replicated_allow_only_replicated_engine=0, database_replicated_allow_explicit_uuid=1 and database_replicated_allow_replicated_engine_arguments=1: %w", err))
	}
	return err
}

// Each shard has its own Keeper path; every replica uses the same CREATE DDL.
func BuildReplicatedEngineWithMacros(localTableEngine string, transferID string, generationID string) string {
	keeperPath := "/clickhouse/tables/__dt_snapshot/" + transferID + "_" + generationID + "_{shard}"
	return BuildReplicatedEngineSQL(localTableEngine, keeperPath, "{replica}")
}

// BuildReplicatedEngineSQL preserves engine arguments and uses the supplied replication identity.
func BuildReplicatedEngineSQL(localTableEngine, keeperPath, replicaName string) string {
	engineName, engineArguments := localTableEngine, ""
	argumentsAt := strings.IndexByte(localTableEngine, '(')
	if argumentsAt >= 0 {
		engineName = localTableEngine[:argumentsAt]
		engineArguments = strings.TrimSuffix(localTableEngine[argumentsAt+1:], ")")
	}
	engine := "Replicated" + engineName + "(" + QuoteStringLiteral(keeperPath) + ", " + QuoteStringLiteral(replicaName)
	if engineArguments != "" {
		engine += ", " + engineArguments
	}
	return engine + ")"
}

// CommandRequiresDatabaseDDLFence classifies the SQL generated by this sink.
// The session probe stays local. ATTACH PARTITION ... FROM is
// parsed by ClickHouse as REPLACE_PARTITION with replace=false, so it can enter
// the Replicated database's DDL queue just like REPLACE PARTITION. The exception
// for ATTACH_PARTITION in shouldReplicateQuery applies to ATTACH without FROM.
func CommandRequiresDatabaseDDLFence(sqlStatement string) bool {
	return sqlStatement != "SELECT 1 FORMAT Null"
}

// Only repeat read/synchronization barriers. Each attempt discovers replicas
// again, so a host lost between discovery and a read can be excluded safely.
func RetryReplicaBarrier(ctx context.Context, barrier func() error) error {
	for retry := 0; ; retry++ {
		err := barrier()
		if err == nil {
			return nil
		}
		if retry == 2 || ctx.Err() != nil || (!IsRetryableReadError(err) && !xerrors.Is(err, context.DeadlineExceeded)) {
			return err
		}
	}
}

// ClickHouse escapes non-word bytes in metadata filenames as %XX. Its table
// name limit applies to that escaped length, not to the SQL identifier length.
func escapedTableNameLength(tableName string) uint64 {
	var length uint64
	for _, nameByte := range []byte(tableName) {
		if nameByte >= 'a' && nameByte <= 'z' || nameByte >= 'A' && nameByte <= 'Z' || nameByte >= '0' && nameByte <= '9' || nameByte == '_' {
			length++
		} else {
			length += 3
		}
	}
	return length
}

func snapshotTableNameHint(tableName string) string {
	var hint strings.Builder
	for _, character := range tableName {
		if hint.Len() == 16 {
			break
		}
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '_' {
			hint.WriteByte(byte(character))
		} else {
			hint.WriteByte('_')
		}
	}
	return strings.Trim(hint.String(), "_")
}

func SnapshotTemporaryTablePrefix(transferID string, operationID string, sourceTableID abstract.TableID, maxEscapedTableNameLength uint64) (string, error) {
	for _, identifier := range []string{transferID, operationID} {
		if identifier == "" {
			return "", xerrors.New("snapshot table names require transfer and operation IDs")
		}
		if !validSnapshotIdentifier(identifier) {
			return "", xerrors.New("snapshot transfer and operation IDs must contain only ASCII letters, digits or hyphens")
		}
	}
	fixedPrefix := "__dt_" + transferID + "_" + operationID + "_"
	sourceTableIDHash := Murmur128Hex(sourceTableID.Namespace + "\x00" + sourceTableID.Name)
	tableNameHint := snapshotTableNameHint(sourceTableID.Name)
	// Loading names contain the logical part hash and a fresh attempt hash.
	// This is longer than a candidate name, including escaped UUID hyphens.
	reservedSuffixLength := uint64(32 + 1 + 32 + len("_loading"))
	for {
		prefix := fixedPrefix
		if tableNameHint != "" {
			prefix += tableNameHint + "_"
		}
		prefix += sourceTableIDHash + "_"
		if escapedTableNameLength(prefix)+reservedSuffixLength <= maxEscapedTableNameLength {
			return prefix, nil
		}
		if tableNameHint == "" {
			return "", xerrors.Errorf("snapshot temporary table name exceeds ClickHouse limit %d even without the table hint; full transfer/operation IDs and hashes cannot be shortened", maxEscapedTableNameLength)
		}
		tableNameHint = tableNameHint[:len(tableNameHint)-1]
	}
}

func validSnapshotIdentifier(identifier string) bool {
	if identifier == "" {
		return false
	}
	for _, character := range identifier {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' {
			continue
		}
		return false
	}
	return true
}

// Older ClickHouse versions do not expose getMaxTableNameLengthForDatabase.
// Reserve both the .sql.detached metadata suffix and database.UUID.sql on the
// standard 255-byte filename limit. CREATE still reports filesystem errors.
func LegacyMaximumTableNameLength(databaseName string) uint64 {
	const fileNameLimit = uint64(255)
	droppedMetadataOverhead := escapedTableNameLength(databaseName) + 42
	if droppedMetadataOverhead >= fileNameLimit {
		return 0
	}
	return min(fileNameLimit-uint64(len(".sql.detached")), fileNameLimit-droppedMetadataOverhead)
}

// An attempt keeps its UUID when loading is renamed to ready. A reassignment
// uses a new attempt key, so delayed DDL cannot affect the replacement's data.
func PartTableUUID(tableNamePrefix string, attemptKey string, identityHostName string) string {
	digest := Murmur128Hex(tableNamePrefix + "\x00" + attemptKey + "\x00" + identityHostName)
	return digest[:8] + "-" + digest[8:12] + "-" + digest[12:16] + "-" + digest[16:20] + "-" + digest[20:]
}

func PartAttemptKeyFromTableName(tableNamePrefix string, tableName string) (string, bool) {
	if !strings.HasPrefix(tableName, tableNamePrefix) {
		return "", false
	}
	suffix := strings.TrimPrefix(tableName, tableNamePrefix)
	if !strings.HasSuffix(suffix, "_loading") && !strings.HasSuffix(suffix, "_ready") {
		return "", false
	}
	attemptKey := strings.TrimSuffix(strings.TrimSuffix(suffix, "_loading"), "_ready")
	if len(attemptKey) != 65 || attemptKey[32] != '_' {
		return "", false
	}
	for index, character := range attemptKey {
		if index == 32 {
			continue
		}
		if character >= '0' && character <= '9' || character >= 'a' && character <= 'f' {
			continue
		}
		return "", false
	}
	return attemptKey, true
}

func ValidateSnapshotConfiguration(transfer *model.Transfer, operationID string, cleanupOnly bool) (*clickhouse_model.ChDestination, error) {
	if transfer == nil {
		return nil, xerrors.New("missing snapshot transfer")
	}
	dstConfig, ok := transfer.Dst.(*clickhouse_model.ChDestination)
	if !ok || dstConfig == nil {
		return nil, xerrors.New("snapshot destination must be ClickHouse")
	}
	if cleanupOnly {
		return dstConfig, nil
	}
	if err := dstConfig.ValidateExactlyOnceAltNames(); err != nil {
		return nil, abstract.NewFatalError(err)
	}
	if transfer.AsyncOperations {
		return nil, abstract.NewFatalError(xerrors.New("ExactlyOnceSnapshot does not support AsyncOperations: cleanup requires a separate scheduled task"))
	}
	if transfer.RegularSnapshot != nil && len(transfer.RegularSnapshot.Incremental) != 0 {
		return nil, abstract.NewFatalError(xerrors.New("ExactlyOnceSnapshot does not support incremental snapshot cursors: cursor advancement must be committed with publication"))
	}
	if operationID == "" {
		return nil, abstract.NewFatalError(xerrors.New("exactly-once snapshot requires an operation ID"))
	}
	if dstConfig.TTL != "" || dstConfig.Rotation != nil || dstConfig.ForceJSONMode || transfer.HasPublicTransformation() {
		return nil, abstract.NewFatalError(xerrors.New("exactly-once snapshots currently require native INSERT, no TTL/rotation and no transformations"))
	}
	// Source-specific table-name normalization preserves the rows and is safe
	// for snapshot bookkeeping. Other internal transformations remain forbidden.
	if transfer.Transformation != nil {
		for _, transformation := range transfer.Transformation.ExtraTransformers {
			if transformation.Type() != "ydb-path-relativizer-transformer" {
				return nil, abstract.NewFatalError(xerrors.New("exactly-once snapshots do not support row-changing source transformations"))
			}
		}
	}
	return dstConfig, nil
}

// Keep the ordinary sink's version parsing and bounded read retry policy.
func ReadSnapshotServerVersion(connection *sql.DB) (semver.Version, error) {
	var versionText string
	err := backoff.Retry(
		func() error {
			ctx, cancel := context.WithTimeout(context.Background(), clickhouse_errors.ClickhouseReadTimeout)
			defer cancel()

			return connection.QueryRowContext(ctx, "SELECT version();").Scan(&versionText)
		}, backoff.WithMaxRetries(backoff.NewExponentialBackOff(), 3),
	)
	if err != nil {
		return semver.Version{}, xerrors.Errorf("unable to select clickhouse version: %w", err)
	}
	versionParts := strings.Split(versionText, ".")
	if len(versionParts) < 4 {
		return semver.Version{}, xerrors.Errorf("unexpected version: %s", versionText)
	}
	majorVersion, err := strconv.ParseUint(versionParts[0], 10, 64)
	if err != nil {
		return semver.Version{}, xerrors.Errorf("unable to parse major version: %w", err)
	}
	minorVersion, err := strconv.ParseUint(versionParts[1], 10, 64)
	if err != nil {
		return semver.Version{}, xerrors.Errorf("unable to parse minor version: %w", err)
	}
	patchVersion, _ := strconv.ParseUint(versionParts[2], 10, 64)
	return semver.Version{Major: majorVersion, Minor: minorVersion, Patch: patchVersion}, nil
}

func WaitOrCancel(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return xerrors.Errorf("WaitOrCancel: %w", ctx.Err())
	case <-timer.C:
		return nil
	}
}

// significantSQLTokens excludes whitespace/comments and rejects unterminated quotes.
func significantSQLTokens(statement, description string) ([]antlr.Token, error) {
	tokens := clickhouse_lexer.StringToTokens(statement)
	significantTokens := tokens[:0]
	for _, token := range tokens {
		if token.GetChannel() != antlr.TokenDefaultChannel {
			continue
		}
		switch token.GetTokenType() {
		case clickhouse_lexer.ClickHouseLexerBACKQUOTE,
			clickhouse_lexer.ClickHouseLexerQUOTE_DOUBLE,
			clickhouse_lexer.ClickHouseLexerQUOTE_SINGLE:
			return nil, xerrors.Errorf("unclosed quote in %s", description)
		}
		significantTokens = append(significantTokens, token)
	}
	return significantTokens, nil
}
