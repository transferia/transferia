BEGIN;
create schema "testschema";
create type "testschema"."testEnum" as enum ('Value1', 'Value2');

create table "testschema".test (
    id              integer primary key,
    charvar         character varying(256),
    deuch           "testschema"."testEnum"
);
alter database postgres set search_path = "$user", public;
COMMIT ;
