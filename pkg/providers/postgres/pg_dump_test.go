//go:build !disable_postgres_provider

package postgres

import (
	"bytes"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestParseCreateTableDDL(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		item := pgDumpItem{
			Typ: "TABLE",
			Body: `
CREATE TABLE dev.connector (
    id text NOT NULL,
    status text,
    src_id text,
    dst_id text,
    runtime text DEFAULT 'local'::text,
    runtime_spec jsonb,
    responsible jsonb,
    folder_id text,
    author text,
    description text,
    created_at timestamp without time zone DEFAULT now(),
    type text DEFAULT 'SNAPSHOT_AND_INCREMENT'::text,
    name text DEFAULT ''::text,
    is_deleted boolean DEFAULT false
);`,
		}
		table, err := item.TableDescription()
		require.NoError(t, err)
		require.Equal(t, "dev", table.Schema)
		require.Equal(t, "connector", table.Name)
	})
	t.Run("escaped", func(t *testing.T) {
		item := pgDumpItem{
			Typ: "TABLE",
			Body: `
CREATE TABLE "tm-382_1".connector (
    id text NOT NULL,
    status text,
    src_id text,
    dst_id text,
    runtime text DEFAULT 'local'::text,
    runtime_spec jsonb,
    responsible jsonb,
    folder_id text,
    author text,
    description text,
    created_at timestamp without time zone DEFAULT now(),
    type text DEFAULT 'SNAPSHOT_AND_INCREMENT'::text,
    name text DEFAULT ''::text,
    is_deleted boolean DEFAULT false
);`,
		}
		table, err := item.TableDescription()
		require.NoError(t, err)
		require.Equal(t, "tm-382_1", table.Schema)
		require.Equal(t, "connector", table.Name)
	})
}

func TestParsePgDumpOutput(t *testing.T) {
	t.Run("Table", func(t *testing.T) {
		dumpOut := `
--
-- Name: test1; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.test1 (
    id integer NOT NULL,
    name character varying(40)
);`

		items := parsePgDumpOut(bytes.NewReader([]byte(dumpOut)))
		require.True(t, len(items) == 1)
		require.NoError(t, PgObjectType(items[0].Typ).IsValid())
		typ := PgObjectType(items[0].Typ)
		require.True(t, typ == Table)
	})

	t.Run("Sequence", func(t *testing.T) {
		dumpOut := `
--
-- Database: ids_1_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.ids_1_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;`

		items := parsePgDumpOut(bytes.NewReader([]byte(dumpOut)))
		require.True(t, len(items) == 1)
		require.NoError(t, PgObjectType(items[0].Typ).IsValid())
		typ := PgObjectType(items[0].Typ)
		require.True(t, typ == Sequence)
	})

	t.Run("SequenceOwnedBy", func(t *testing.T) {
		dumpOut := `
--
-- Database: ids_1_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.ids_1_seq OWNED BY public.test1.id;`

		items := parsePgDumpOut(bytes.NewReader([]byte(dumpOut)))
		require.True(t, len(items) == 1)
		require.NoError(t, PgObjectType(items[0].Typ).IsValid())
		typ := PgObjectType(items[0].Typ)
		require.True(t, typ == SequenceOwnedBy)
	})

	t.Run("Constraint", func(t *testing.T) {
		dumpOut := `
--
-- Database: __consumer_keeper __consumer_keeper_consumer_locked_by_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.__consumer_keeper
    ADD CONSTRAINT __consumer_keeper_consumer_locked_by_key UNIQUE (consumer, locked_by);`

		items := parsePgDumpOut(bytes.NewReader([]byte(dumpOut)))
		require.True(t, len(items) == 1)
		require.NoError(t, PgObjectType(items[0].Typ).IsValid())
		typ := PgObjectType(items[0].Typ)
		require.True(t, typ == Constraint)
	})

	t.Run("PrimaryKey", func(t *testing.T) {
		dumpOut := `
--
-- Database: test1 test1_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.test1
    ADD CONSTRAINT test1_pkey PRIMARY KEY (id);`

		items := parsePgDumpOut(bytes.NewReader([]byte(dumpOut)))
		require.True(t, len(items) == 1)
		require.NoError(t, PgObjectType(items[0].Typ).IsValid())
		typ := PgObjectType(items[0].Typ)
		require.True(t, typ == PrimaryKey)
	})

	t.Run("FkContraint", func(t *testing.T) {
		dumpOut := `
--
-- Database: items_1 items_1_item_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.items_1
    ADD CONSTRAINT items_1_item_id_fkey FOREIGN KEY (item_id) REFERENCES public.ids_1(id);`

		items := parsePgDumpOut(bytes.NewReader([]byte(dumpOut)))
		require.True(t, len(items) == 1)
		require.NoError(t, PgObjectType(items[0].Typ).IsValid())
		typ := PgObjectType(items[0].Typ)
		require.True(t, typ == FkConstraint)
	})

	t.Run("Collation", func(t *testing.T) {
		dumpOut := `
--
-- Database: french; Type: COLLATION; Schema: public; Owner: -
--

CREATE COLLATION public.french (provider = libc, locale = 'fr_FR.utf8');`

		items := parsePgDumpOut(bytes.NewReader([]byte(dumpOut)))
		require.True(t, len(items) == 1)
		require.NoError(t, PgObjectType(items[0].Typ).IsValid())
		typ := PgObjectType(items[0].Typ)
		require.True(t, typ == Collation)
	})

	t.Run("Trigger", func(t *testing.T) {
		dumpOut := `
--
-- Database: test1 trigger1; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trigger1 BEFORE UPDATE ON public.test1 FOR EACH ROW EXECUTE FUNCTION trigger1_function();`

		items := parsePgDumpOut(bytes.NewReader([]byte(dumpOut)))
		require.True(t, len(items) == 1)
		require.NoError(t, PgObjectType(items[0].Typ).IsValid())
		typ := PgObjectType(items[0].Typ)
		require.True(t, typ == Trigger)
	})

	t.Run("Policy", func(t *testing.T) {
		dumpOut := `
--
-- Database: test1 test_policy; Type: POLICY; Schema: public; Owner: -
--

CREATE POLICY test_policy ON public.test1 USING (((name)::text = 'test'::text));`

		items := parsePgDumpOut(bytes.NewReader([]byte(dumpOut)))
		require.True(t, len(items) == 1)
		require.NoError(t, PgObjectType(items[0].Typ).IsValid())
		typ := PgObjectType(items[0].Typ)
		require.True(t, typ == Policy)
	})

	t.Run("Cast", func(t *testing.T) {
		dumpOut := `
--
-- Database: CAST (character varying AS integer); Type: CAST; Schema: public; Owner: -
--

CREATE CAST (character varying AS integer) WITH FUNCTION public.test_to_int(character varying);`

		items := parsePgDumpOut(bytes.NewReader([]byte(dumpOut)))
		require.True(t, len(items) == 1)
		require.NoError(t, PgObjectType(items[0].Typ).IsValid())
		typ := PgObjectType(items[0].Typ)
		require.True(t, typ == Cast)
	})
}

var buildArgsConfig00 = `
{
  "AltHosts": null,
  "AutoActivate": false,
  "BatchSize": 1000,
  "Cluster": "",
  "CollapseInheritTables": false,
  "DBTables": [
    "cms.FooContents"
  ],
  "Database": "db_creatio_uat",
  "DesiredTableSize": 1073741824,
  "ExcludeDescendants": false,
  "ExcludedTables": null,
  "FlushThresholdSec": 0,
  "Host": "1.1.1.1",
  "IgnoreUnknownTables": false,
  "IgnoreUserTypes": false,
  "IsHomo": false,
  "IsPublic": true,
  "KeeperSchema": "cms",
  "NoLegacy": true,
  "Password": "blablabla",
  "Port": 5432,
  "PostSteps": {
    "Cast": false,
    "Collation": false,
    "Constraint": false,
    "Default": false,
    "FkConstraint": false,
    "Function": false,
    "Index": false,
    "MaterializedView": false,
    "Policy": false,
    "PrimaryKey": false,
    "Rule": false,
    "Sequence": false,
    "SequenceOwnedBy": false,
    "Table": false,
    "Trigger": false,
    "Type": false,
    "View": false
  },
  "PreSteps": {
    "Cast": false,
    "Collation": false,
    "Constraint": false,
    "Default": false,
    "FkConstraint": false,
    "Function": false,
    "Index": false,
    "MaterializedView": false,
    "Policy": false,
    "PrimaryKey": true,
    "Rule": false,
    "Sequence": false,
    "SequenceOwnedBy": false,
    "Table": true,
    "Trigger": false,
    "Type": false,
    "View": false
  },
  "SecurityGroupIDs": [
    "enp752cjcd9kghhufnp5",
    "enp2nq0ncvsf11k88u4n"
  ],
  "SlotByteLagLimit": 9223372036854775807,
  "SlotID": "",
  "SnapshotDegreeOfParallelism": 0,
  "SubNetworkID": "e9b6va15iq26mba8bm55",
  "TLSFile": "",
  "Token": "",
  "UseFakePrimaryKey": false,
  "UsePolling": false,
  "User": "dwh_replic"
}
`

func TestBuildArgs(t *testing.T) {
	var pgSrc PgSource
	err := json.Unmarshal([]byte(buildArgsConfig00), &pgSrc)
	require.NoError(t, err)

	connString, _, err := PostgresDumpConnString(&pgSrc)
	require.NoError(t, err)
	require.Equal(t, `host=1.1.1.1 port=5432 dbname=db_creatio_uat user=dwh_replic`, connString)

	// with DBTables
	args, err := pgDumpSchemaArgs(&pgSrc, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []string{
		`--no-publications`,
		`--no-subscriptions`,
		`--format=plain`,
		`--no-owner`,
		`--schema-only`,
		`-t`,
		`"cms"."FooContents"`,
		`-t`,
		`"cms"."__consumer_keeper"`,
		`-t`,
		`"cms"."__data_transfer_lsn"`,
		`-t`,
		`"cms"."__data_transfer_signal_table"`,
	}, args)

	// without DBTables
	pgSrc.DBTables = []string{}
	args, err = pgDumpSchemaArgs(&pgSrc, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []string{
		`--no-publications`,
		`--no-subscriptions`,
		`--format=plain`,
		`--no-owner`,
		`--schema-only`,
		`-T`,
		`"public"."repl_mon"`,
	}, args)
}
