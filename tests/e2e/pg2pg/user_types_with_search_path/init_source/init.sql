BEGIN;
create schema "testschema";
create type "testschema"."testEnum" as enum ('Value1', 'Value2');

create table "testschema".test (
    id              integer primary key,
    charvar         character varying(256),
    deuch           "testschema"."testEnum"
);
alter database postgres set search_path = "$user", public, "testschema";

INSERT INTO "testschema".test (id, charvar, deuch)
VALUES (1, 'chuvak', 'Value1');

COMMIT;

BEGIN;
SELECT pg_create_logical_replication_slot('testslot', 'wal2json');
COMMIT;
