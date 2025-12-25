create schema "woshiPushiMushi";
create type "woshiPushiMushi"."Wut" as enum ('DuGehstMirAufDieEier', 'EinScheissdreckWerdeIchTun');
create type "fancyCamelCaseType" as enum ('zhopa', 'zhepa');

CREATE TYPE udt AS
(
    int_field  int,
    text_field text
);

CREATE TYPE with_nested_udt_array AS
(
    int_field int,
    array_field udt array
);

create table testtable (
    id              integer primary key,
    charvar         character varying(256),
    fancy           "fancyCamelCaseType",
    deuch           "woshiPushiMushi"."Wut",
    udt             udt,
    udt_arr         udt array,
    nested_udt_arr  with_nested_udt_array
);

INSERT INTO testtable (id, charvar, fancy, deuch, udt, udt_arr, nested_udt_arr)
VALUES (1, 'chuvak', 'zhopa', 'DuGehstMirAufDieEier', (1, '123')::udt, ARRAY [(1, 'foo')::udt, (2, 'bar')::udt], (1, ARRAY[(2, 'sometext')::udt]));
