package ch_eo_common

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"github.com/transferia/transferia/pkg/abstract"
	"github.com/transferia/transferia/pkg/abstract/model"
	clickhouse_model "github.com/transferia/transferia/pkg/providers/clickhouse/model"
)

func TestSnapshotCleanupAllowsAmbiguousAltNames(test *testing.T) {
	destination := new(clickhouse_model.ChDestination)
	destination.ExactlyOnceSnapshot = true
	destination.AltNamesList = []model.AltName{{From: "orders-live", To: "a"}, {From: "orders_live", To: "b"}}
	transfer := new(model.Transfer)
	transfer.Dst = destination

	config, err := ValidateSnapshotConfiguration(transfer, "", true)
	require.NoError(test, err)
	require.Same(test, destination, config)
}

func TestExtractTableDefinitionPartsTokens(test *testing.T) {
	for _, tc := range []struct {
		name    string
		header  string
		columns string
		engine  string
		tail    string
	}{
		{
			name:    "engine without arguments",
			header:  "CREATE TABLE t",
			columns: "(id UInt64)",
			engine:  "MergeTree",
			tail:    "ORDER BY id",
		},
		{
			name:    "qualified name with UUID and cluster",
			header:  "CREATE TABLE IF NOT EXISTS `db`.`a ( ENGINE)` UUID 'old' ON CLUSTER '{cluster}'",
			columns: "(`id` UInt64, value Array(Tuple(String, Decimal(10, 2))))",
			engine:  "ReplicatedMergeTree('/tables/{shard}', '{replica}')",
			tail:    "PARTITION BY intDiv(id, 10) ORDER BY id SETTINGS index_granularity = 8192",
		},
		{
			name:    "quoted brackets and keywords",
			header:  `CREATE TABLE "ENGINE)"`,
			columns: "(`ENGINE(` UInt64, x String DEFAULT 'ENGINE=Fake(x)', y String DEFAULT 'a)''b(', z UInt8 MATERIALIZED if(`ENGINE(` > 0, 1, 0))",
			engine:  "ReplicatedReplacingMergeTree('/a)b(', 'r''1', `ENGINE(`)",
			tail:    "ORDER BY `ENGINE(`",
		},
		{
			name:    "unicode and multiline expressions",
			header:  "create table `\u0431\u0430\u0437\u0430`.`\u0434\u0430\u043d\u043d\u044b\u0435` uuid 'old'",
			columns: "(\n `\u043a\u043b\u044e\u0447` UInt64,\n `\u0442\u0435\u043a\u0441\u0442` String DEFAULT '\u043f\u0440\u0438\u0432\u0435\u0442 \U0001f994 ) ENGINE',\n x UInt8 MATERIALIZED if(`\u043a\u043b\u044e\u0447` > 0, 1, 0)\n)",
			engine:  "ReplacingMergeTree(`\u043a\u043b\u044e\u0447`)",
			tail:    "ORDER BY `\u043a\u043b\u044e\u0447`\nSETTINGS storage_policy = '\u0445\u0440\u0430\u043d\u0438\u043b\u0438\u0449\u0435'",
		},
		{
			name:    "comments around boundaries",
			header:  "CREATE /* ENGINE() */ TABLE t",
			columns: "(id UInt64 /* ) ENGINE=Fake() */) -- columns end",
			engine:  "MergeTree /* ( ENGINE ) */ ()",
			tail:    "-- engine end\nORDER BY id",
		},
	} {
		test.Run(tc.name, func(test *testing.T) {
			ddl := tc.header + "\n" + tc.columns + "\nEnGiNe = " + tc.engine + "\n" + tc.tail
			columns, engine, tail, err := ExtractTableDefinitionParts(ddl)
			require.NoError(test, err)
			require.Equal(test, tc.columns, columns)
			require.Equal(test, tc.engine, engine)
			require.Equal(test, tc.tail, tail)

			rewritten, err := BuildCreateTableDDL("dst", "new", "new-uuid", ddl, "")
			require.NoError(test, err)
			newColumns, newEngine, newTail, err := ExtractTableDefinitionParts(rewritten)
			require.NoError(test, err)
			require.Equal(test, columns, newColumns)
			require.Equal(test, engine, newEngine)
			require.Equal(test, tail, newTail)
		})
	}
}

func TestExtractTableDefinitionPartsRejectsMalformedInput(test *testing.T) {
	for _, ddl := range []string{
		"",
		"SELECT '(id UInt64) ENGINE=MergeTree'",
		"prefix CREATE TABLE t (id UInt64) ENGINE=MergeTree",
		"CREATE TABLE t AS SELECT 1",
		"CREATE TABLE t (id UInt64) ENGINE_X = MergeTree",
		"CREATE TABLE t (id UInt64) ENGINE=",
		"CREATE TABLE t (id UInt64) ENGINE=ORDER BY id",
		"CREATE TABLE t (id UInt64) ENGINE=()",
		"CREATE TABLE t (id UInt64) ENGINE='MergeTree'",
		"CREATE TABLE t (id UInt64) ENGINE=`MergeTree`",
		"CREATE TABLE t (id UInt64 ENGINE=MergeTree",
		"CREATE TABLE t (id UInt64)) ENGINE=MergeTree",
		"CREATE TABLE t (id UInt64) ENGINE=ReplacingMergeTree(if(id > 0, 1, 0)",
		"CREATE TABLE t (id UInt64) ENGINE=MergeTree()) ORDER BY id",
		"CREATE TABLE t (value String DEFAULT 'unclosed) ENGINE=MergeTree",
		"CREATE TABLE t (`unclosed UInt64) ENGINE=MergeTree",
		"CREATE TABLE t (id UInt64) ENGINE=ReplicatedMergeTree('/path', 'unclosed)",
	} {
		test.Run(ddl, func(test *testing.T) {
			_, _, _, err := ExtractTableDefinitionParts(ddl)
			require.Error(test, err)
		})
	}
}

func TestExtractTableDefinitionPartsEngineWithoutEquals(test *testing.T) {
	columns, engine, tail, err := ExtractTableDefinitionParts("CREATE TABLE t (id UInt64) ENGINE MergeTree() ORDER BY id")
	require.NoError(test, err)
	require.Equal(test, "(id UInt64)", columns)
	require.Equal(test, "MergeTree()", engine)
	require.Equal(test, "ORDER BY id", tail)
}

func TestCreateDDLPreservesExpressions(test *testing.T) {
	ddl := "CREATE TABLE `db`.`a ( ENGINE)` UUID 'old' (`id` UInt64, `value` String DEFAULT 'ENGINE=Fake(x)', `x` UInt8 MATERIALIZED if(id > 0, 1, 0)) ENGINE = ReplicatedReplacingMergeTree('/a,b', '{replica}', id, x) PARTITION BY intDiv(id, 10) ORDER BY id SETTINGS index_granularity = 8192"
	columns, engine, tail, err := ExtractTableDefinitionParts(ddl)
	require.NoError(test, err)
	require.Contains(test, columns, "MATERIALIZED if(id > 0, 1, 0)")
	local, err := MakeEngineNonReplicated(engine)
	require.NoError(test, err)
	require.Equal(test, "ReplacingMergeTree(id, x)", local)
	result, err := BuildCreateTableDDL("db", "new", "id", ddl, "MergeTree()")
	require.NoError(test, err)
	require.True(test, strings.HasSuffix(result, tail))
	require.Contains(test, result, columns)
	require.NotContains(test, result, "ReplicatedReplacingMergeTree")
}

func TestMakeEngineNonReplicatedArguments(test *testing.T) {
	for input, expected := range map[string]string{
		"MergeTree":          "MergeTree()",
		"ReplacingMergeTree": "ReplacingMergeTree()",
		"VersionedCollapsingMergeTree(Sign, HitVersion)":                        "VersionedCollapsingMergeTree(Sign, HitVersion)",
		"ReplicatedVersionedCollapsingMergeTree('/a', 'r', Sign, VisitVersion)": "VersionedCollapsingMergeTree(Sign, VisitVersion)",
		"ReplicatedMergeTree()":                                                       "MergeTree()",
		"ReplicatedReplacingMergeTree(version, deleted)":                              "ReplacingMergeTree(version, deleted)",
		"ReplicatedReplacingMergeTree('/a,b', 'r''1', version)":                       "ReplacingMergeTree(version)",
		"ReplicatedReplacingMergeTree('/a', 'r')":                                     "ReplacingMergeTree()",
		"ReplicatedReplacingMergeTree('/a(b),c', 'r\\'1', version)":                   "ReplacingMergeTree(version)",
		"ReplicatedReplacingMergeTree('/a', /* replica */ 'r', `\u0432`)":             "ReplacingMergeTree(`\u0432`)",
		"ReplicatedReplacingMergeTree(version, tuple(deleted))":                       "ReplacingMergeTree(version, tuple(deleted))",
		"ReplicatedReplacingMergeTree('/\u043f\u0443\u0442\u044c', 'r', `\u0432`)":    "ReplacingMergeTree(`\u0432`)",
		"ReplacingMergeTree(tuple(version), tuple(deleted))":                          "ReplacingMergeTree(tuple(version), tuple(deleted))",
		"ReplicatedReplacingMergeTree('/a', 'r', tuple(version), tuple(deleted))":     "ReplacingMergeTree(tuple(version), tuple(deleted))",
		"ReplacingMergeTree(version -- comment\n)":                                    "ReplacingMergeTree(version)",
		"ReplicatedReplacingMergeTree('/a', 'r', /* version */ version -- comment\n)": "ReplacingMergeTree(/* version */ version)",
		"MergeTree(/* comment */)":                                                    "MergeTree()",
		"ReplacingMergeTree(version /* comment */, deleted)":                          "ReplacingMergeTree(version /* comment */, deleted)",
	} {
		actual, err := MakeEngineNonReplicated(input)
		require.NoError(test, err, input)
		require.Equal(test, expected, actual, input)
	}
	for _, input := range []string{
		"", "MergeTreeWithUnknownSemantics()", "MergeTree(date, id, 8192)",
		"ReplacingMergeTree(version", "ReplacingMergeTree(version))", "MergeTree() trailing",
		"ReplicatedMergeTree('/a')", "ReplicatedMergeTree('/a', replica)",
		"ReplicatedMergeTree('/a' 'r')", "ReplicatedMergeTree('/a', 'r',)",
		"ReplicatedMergeTree('/a', 'r)",
		"ReplacingMergeTree((version)", "ReplacingMergeTree(version)(extra)",
		"ReplicatedReplacingMergeTree('/a', 'r', /* missing version */ )",
		"ReplicatedReplacingMergeTree('/a', 'r' version)",
	} {
		actual, err := MakeEngineNonReplicated(input)
		require.Error(test, err, input)
		require.Empty(test, actual, input)
	}
}

func TestValidateSnapshotTableDDLAllowsOrdinaryTTLNamesAndLiterals(test *testing.T) {
	for _, ddl := range []string{
		"CREATE TABLE t (id UInt64 COMMENT 'daily TTL report') ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (id String DEFAULT '{table}:{database} TTL report') ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (`daily TTL report {table}` UInt64) ENGINE=MergeTree ORDER BY `daily TTL report {table}`",
		"CREATE TABLE t (\"TTL\" UInt64) ENGINE=MergeTree ORDER BY \"TTL\"",
		"CREATE TABLE t (id UInt64) ENGINE=MergeTree /* TTL {table} */ ORDER BY id -- TTL {database}\n",
		"CREATE TABLE t (ttl DateTime, id DateTime DEFAULT ttl) ENGINE=MergeTree ORDER BY ttl",
		"CREATE TABLE t (ttl DateTime, id DateTime DEFAULT ttl + INTERVAL 1 DAY) ENGINE=MergeTree ORDER BY id + ttl",
		"CREATE TABLE t (ttl UInt64, id UInt64 DEFAULT ttl + ttl) ENGINE=MergeTree ORDER BY if(id > 0, ttl, ttl)",
		"CREATE TABLE t (ttl UInt64, id UInt64 DEFAULT multiIf(ttl > 0, ttl, ttl)) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (ttl UInt8, id UInt8 DEFAULT NOT ttl) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (ttl Array(UInt64), id UInt64, value UInt8 DEFAULT id NOT IN ttl) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (ttl Array(UInt64), id UInt64, value UInt8 DEFAULT id GLOBAL NOT IN ttl) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (ttl String, id String, value UInt8 DEFAULT id NOT LIKE ttl) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (ttl String, id String, value UInt8 DEFAULT id NOT ILIKE ttl) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (ttl String, id String, value UInt8 DEFAULT id REGEXP ttl) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (ttl String, id String, value UInt8 DEFAULT id NOT RLIKE ttl) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (ttl UInt64, id UInt64 DEFAULT caseWithExpression(ttl, 0, ttl, ttl)) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (ttl DateTime) ENGINE=MergeTree PRIMARY KEY ttl ORDER BY ttl",
		"CREATE TABLE t (ttl UInt8, id UInt8 DEFAULT EXISTS (SELECT 1 WHERE ttl)) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (ttl UInt64, INDEX ttl ttl TYPE minmax GRANULARITY 1) ENGINE=MergeTree ORDER BY ttl",
		"CREATE TABLE t (ttl UInt64, CONSTRAINT ttl CHECK ttl > 0) ENGINE=MergeTree ORDER BY ttl",
		"CREATE TABLE t (ttl UInt64, PROJECTION ttl (SELECT * ORDER BY ttl)) ENGINE=MergeTree ORDER BY ttl",
		"CREATE TABLE t (ttl DateTime, id DateTime ALIAS if(ttl > now(), ttl, now())) ENGINE=MergeTree ORDER BY (ttl, id)",
		"CREATE TABLE t (ttl DateTime, id DateTime MATERIALIZED ttl) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (id UInt64) ENGINE=MergeTree ORDER BY id COMMENT ' TTL {database} '",
		"CREATE TABLE t (id UInt64) ENGINE=MergeTree ORDER BY id SETTINGS storage_policy=' TTL {table} '",
		"CREATE TABLE t (id UInt64) ENGINE=ReplicatedMergeTree('/stable/TTL/path', '{table}-{database}') ORDER BY id",
		"CREATE TABLE t (id UInt64) ENGINE=ReplicatedMergeTree('/stable/path', concat('{table}', '{database}')) ORDER BY id",
		"CREATE TABLE t (id UInt64) ENGINE=ReplicatedMergeTree /* {table} */ ('/stable/path', /* {database} */ 'replica') ORDER BY id",
		"CREATE TABLE t (id UInt64) ENGINE=ReplicatedMergeTree ORDER BY id",
		"CREATE TABLE t (id UInt64) ENGINE=ReplicatedMergeTree() ORDER BY id",
		"CREATE TABLE t (id UInt64, version UInt64) ENGINE=ReplicatedReplacingMergeTree(version) ORDER BY id",
	} {
		test.Run(ddl, func(test *testing.T) {
			require.NoError(test, ValidateSnapshotTableDDL(ddl))
		})
	}
}

func TestValidateSnapshotTableDDLRejectsTTLClauses(test *testing.T) {
	for _, ddl := range []string{
		"CREATE TABLE t (id UInt64, ts DateTime) ENGINE=MergeTree ORDER BY id TTL ts + INTERVAL 1 DAY",
		"CREATE TABLE t (id UInt64, ts DateTime) ENGINE=MergeTree ORDER BY id ttl ts + INTERVAL 1 DAY",
		"CREATE TABLE t (id UInt64, ts DateTime, value String TTL ts + INTERVAL 1 DAY) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (ttl DateTime TTL ttl + INTERVAL 1 DAY) ENGINE=MergeTree ORDER BY ttl",
		"CREATE TABLE t (ttl DateTime, value DateTime DEFAULT ttl TTL ttl + INTERVAL 1 DAY) ENGINE=MergeTree ORDER BY ttl",
		"CREATE TABLE t (ttl DateTime) ENGINE=MergeTree ORDER BY ttl TTL ttl + INTERVAL 1 DAY",
		"CREATE TABLE t (ts DateTime, value UInt64 DEFAULT 0 TTL ts + INTERVAL 1 DAY) ENGINE=MergeTree ORDER BY ts",
		"CREATE TABLE t (ts DateTime, value DateTime DEFAULT CAST(ts AS DateTime) TTL ts + INTERVAL 1 DAY) ENGINE=MergeTree ORDER BY ts",
		"CREATE TABLE t (ttl DateTime, ts DateTime DEFAULT multiIf(ttl > now(), ttl, now()) TTL ts + INTERVAL 1 DAY) ENGINE=MergeTree ORDER BY ts",
		"CREATE TABLE t (key DateTime) ENGINE=MergeTree ORDER BY key TTL key + INTERVAL 1 DAY",
		"CREATE TABLE t (default DateTime) ENGINE=MergeTree ORDER BY default TTL default + INTERVAL 1 DAY",
		"CREATE TABLE t (order DateTime) ENGINE=MergeTree ORDER BY order TTL order + INTERVAL 1 DAY",
		"CREATE TABLE t (key DateTime) ENGINE=MergeTree ORDER BY key DESC TTL key + INTERVAL 1 DAY",
		"CREATE TABLE t (ttl DateTime) ENGINE=MergeTree PRIMARY KEY ttl ORDER BY ttl TTL ttl + INTERVAL 1 DAY",
		"CREATE TABLE t (interval DateTime) ENGINE=MergeTree ORDER BY interval TTL interval + INTERVAL 1 DAY",
		"CREATE TABLE t (exists DateTime) ENGINE=MergeTree ORDER BY exists TTL exists + INTERVAL 1 DAY",
		"CREATE TABLE t (when DateTime) ENGINE=MergeTree ORDER BY when TTL when + INTERVAL 1 DAY",
		"CREATE TABLE t (case DateTime) ENGINE=MergeTree ORDER BY case TTL case + INTERVAL 1 DAY",
		"CREATE TABLE t (ttl Array(UInt64), id UInt64, ts DateTime, value UInt8 DEFAULT id NOT IN ttl TTL ts + INTERVAL 1 DAY) ENGINE=MergeTree ORDER BY id",
		"CREATE TABLE t (id UInt64, ts DateTime) ENGINE=MergeTree ORDER BY id /* before */ TTL /* after */ ts",
	} {
		test.Run(ddl, func(test *testing.T) {
			require.ErrorContains(test, ValidateSnapshotTableDDL(ddl), "TTL")
		})
	}
}

func TestValidateSnapshotTableDDLRejectsRenameSensitiveKeeperPath(test *testing.T) {
	for _, ddl := range []string{
		"CREATE TABLE t (id UInt64) ENGINE=ReplicatedMergeTree('/tables/{table}', 'replica') ORDER BY id",
		"CREATE TABLE t (id UInt64) ENGINE=ReplicatedMergeTree('/tables/{database}/t', 'replica') ORDER BY id",
		"CREATE TABLE t (id UInt64, version UInt64) ENGINE=ReplicatedReplacingMergeTree /* engine */ ( /* path */ '/tables/{table}', /* replica */ 'replica', version) ORDER BY id",
		"CREATE TABLE t (id UInt64) ENGINE=ReplicatedMergeTree('/tables/{table}', concat('replica', '1')) ORDER BY id",
	} {
		test.Run(ddl, func(test *testing.T) {
			require.ErrorContains(test, ValidateSnapshotTableDDL(ddl), "rename-sensitive Keeper macros")
		})
	}
}

func TestCommandRequiresDatabaseDDLFence(test *testing.T) {
	for _, statement := range []string{
		"SELECT 1 FORMAT Null",
	} {
		require.False(test, CommandRequiresDatabaseDDLFence(statement), statement)
	}
	for _, statement := range []string{
		"CREATE TABLE candidate (id UInt64) ENGINE=MergeTree ORDER BY id",
		"DROP TABLE candidate SYNC",
		"RENAME TABLE candidate TO destination",
		"EXCHANGE TABLES candidate AND destination",
		"ALTER TABLE candidate ATTACH PARTITION ID 'all' FROM tmp",
		"ALTER TABLE `db`.`candidate` ATTACH PARTITION ID '202609' FROM `db`.`tmp`",
		"ALTER TABLE `db`.`REPLACE PARTITION` ATTACH PARTITION ID 'all' FROM `db`.`FROM tmp`",
		"ALTER TABLE candidate REPLACE PARTITION ID 'all' FROM tmp",
		"ALTER TABLE `ATTACH PARTITION` REPLACE PARTITION ID 'all' FROM tmp",
		"ALTER TABLE candidate ATTACH PARTITION ID 'all' FROM tmp, REPLACE PARTITION ID 'other' FROM another",
		"ALTER TABLE candidate ADD COLUMN value String",
		"TRUNCATE TABLE destination",
		"",
	} {
		require.True(test, CommandRequiresDatabaseDDLFence(statement), statement)
	}
}

func TestSnapshotTemporaryTablePrefix(test *testing.T) {
	source := abstract.TableID{Namespace: "public", Name: "users/with spaces-" + strings.Repeat("long", 1000)}
	prefix, err := SnapshotTemporaryTablePrefix("dtt0123456789abcdefg", "dtj0123456789abcdefg", source, 255)
	require.NoError(test, err)
	require.Equal(test, "__dt_dtt0123456789abcdefg_dtj0123456789abcdefg_users_with_space_"+Murmur128Hex(source.Namespace+"\x00"+source.Name)+"_", prefix)
	require.NotContains(test, prefix, "/")
	require.NotContains(test, prefix, " ")
	repeated, err := SnapshotTemporaryTablePrefix("dtt0123456789abcdefg", "dtj0123456789abcdefg", source, 255)
	require.NoError(test, err)
	require.Equal(test, prefix, repeated)
	other := source
	other.Namespace = "other"
	different, err := SnapshotTemporaryTablePrefix("dtt0123456789abcdefg", "dtj0123456789abcdefg", other, 255)
	require.NoError(test, err)
	require.NotEqual(test, prefix, different)
}

func TestSnapshotTemporaryNameBudget(test *testing.T) {
	source := abstract.TableID{Name: "abcdefghijklmnop-more"}
	suffix := strings.Repeat("a", 32) + "_" + strings.Repeat("b", 32) + "_loading"
	full, err := SnapshotTemporaryTablePrefix("dtt", "dtj", source, 255)
	require.NoError(test, err)
	exactBudget := escapedTableNameLength(full + suffix)
	exact, err := SnapshotTemporaryTablePrefix("dtt", "dtj", source, exactBudget)
	require.NoError(test, err)
	require.Equal(test, full, exact)
	shorter, err := SnapshotTemporaryTablePrefix("dtt", "dtj", source, exactBudget-1)
	require.NoError(test, err)
	require.Contains(test, shorter, "abcdefghijklmno_")
	require.LessOrEqual(test, escapedTableNameLength(shorter+suffix), exactBudget-1)
	withoutHint, err := SnapshotTemporaryTablePrefix("dtt", "dtj", source, exactBudget-17)
	require.NoError(test, err)
	require.Equal(test, "__dt_dtt_dtj_"+Murmur128Hex("\x00"+source.Name)+"_", withoutHint)
	_, err = SnapshotTemporaryTablePrefix("dtt", "dtj", source, exactBudget-18)
	require.ErrorContains(test, err, "cannot be shortened")
}

func TestSnapshotNamesEscapeAndRejectUnsafeIDs(test *testing.T) {
	require.EqualValues(test, 11, escapedTableNameLength("a-\u0431_"))
	require.EqualValues(test, 210, LegacyMaximumTableNameLength("db1"))
	require.Zero(test, LegacyMaximumTableNameLength(strings.Repeat("x", 300)))
	require.Empty(test, snapshotTableNameHint("\u0442\u0430\u0431\u043b\u0438\u0446\u0430"))
	for _, identifier := range []string{"", "dtj/other", "dtj_other", "dtj'other"} {
		_, err := SnapshotTemporaryTablePrefix("dtt", identifier, abstract.TableID{Name: "events"}, 255)
		require.Error(test, err)
	}
}

func TestPartAttemptTableIdentity(test *testing.T) {
	prefix := "__dt_dtt_dtj_users_" + Murmur128Hex("public\x00users") + "_"
	partDigest := Murmur128Hex("part/with special characters\u0431") + "_" + Murmur128Hex("first attempt")
	for _, suffix := range []string{"_loading", "_ready"} {
		parsed, found := PartAttemptKeyFromTableName(prefix, prefix+partDigest+suffix)
		require.True(test, found)
		require.Equal(test, partDigest, parsed)
	}
	for _, invalid := range []string{"other_" + partDigest + "_ready", prefix + partDigest + "_loading_ready", prefix + strings.Repeat("z", 32) + "_ready", prefix + "candidate_" + partDigest, prefix + strings.Repeat("a", 65) + "_loading", prefix + strings.Repeat("a", 32) + "_" + strings.Repeat("z", 32) + "_ready"} {
		_, found := PartAttemptKeyFromTableName(prefix, invalid)
		require.False(test, found)
	}
	identity := PartTableUUID(prefix, partDigest, "host1")
	require.Len(test, identity, 36)
	require.Equal(test, identity, PartTableUUID(prefix, partDigest, "host1"))
	require.NotEqual(test, identity, PartTableUUID(prefix, partDigest, "host2"))
	require.NotEqual(test, identity, PartTableUUID(prefix, Murmur128Hex("other part")+"_"+Murmur128Hex("first attempt"), "host1"))
	require.NotEqual(test, identity, PartTableUUID(prefix, strings.Split(partDigest, "_")[0]+"_"+Murmur128Hex("second attempt"), "host1"))
}

func TestHash(test *testing.T) {
	// Keep the encoding stable: hashes are persisted in state keys and table names.
	require.Equal(test, "00000000000000000000000000000000", Murmur128Hex(""))
	require.Equal(test, "cbd8a7b341bd9b025b1e906a48ae1d19", Murmur128Hex("hello"))
	require.Equal(test, "342fac623a5ebc8e4cdcbc079642414d", Murmur128Hex("hello, world"))
}

func TestReplicatedEngine(test *testing.T) {
	for _, testCase := range []struct {
		engine   string
		expected string
	}{
		{"MergeTree()", "ReplicatedMergeTree('/keeper/shard_0', 'host-0')"},
		{"VersionedCollapsingMergeTree(Sign, HitVersion)", "ReplicatedVersionedCollapsingMergeTree('/keeper/shard_0', 'host-0', Sign, HitVersion)"},
		{"ReplacingMergeTree()", "ReplicatedReplacingMergeTree('/keeper/shard_0', 'host-0')"},
		{"ReplacingMergeTree(version, deleted)", "ReplicatedReplacingMergeTree('/keeper/shard_0', 'host-0', version, deleted)"},
	} {
		test.Run(testCase.engine, func(test *testing.T) {
			require.Equal(test, testCase.expected, BuildReplicatedEngineSQL(testCase.engine, "/keeper/shard_0", "host-0"))
		})
	}
	require.Equal(test, "ReplicatedReplacingMergeTree('/clickhouse/tables/__dt_snapshot/dtt_dtj_{shard}', '{replica}', version)", BuildReplicatedEngineWithMacros("ReplacingMergeTree(version)", "dtt", "dtj"))
}
