BEGIN;
CREATE TABLE __test_to_shard (
    id bigserial primary key,
    text text
);
COMMIT;
BEGIN;
insert into __test_to_shard (text) select md5(random()::text) from generate_Series(1,100000) as s;
COMMIT;

BEGIN;
CREATE TABLE __test_to_shard_int32 (
                                 "Id" serial primary key,
                                 text text
);
COMMIT;
BEGIN;
insert into __test_to_shard_int32 (text) select md5(random()::text) from generate_Series(1,100000) as s;
COMMIT;

BEGIN;
CREATE TABLE __test_incremental (
    text text primary key,
    cursor integer
);
COMMIT;
BEGIN;
insert into __test_incremental (text, cursor) select md5(random()::text), s.s from generate_Series(1,10) as s;
COMMIT;


CREATE TABLE __test_incremental_ts (
    text text primary key,
    cursor timestamp
);
COMMIT;
BEGIN;
insert into __test_incremental_ts (text, cursor) select md5(random()::text), now() from generate_Series(1,10) as s;
COMMIT;

CREATE TABLE __test_all_keys_are_numeric (
    id            serial,
    bigserial_key bigserial,
    numeric_key   numeric,
    bigint_key    bigint,
    float_key     float,
    double_key    double precision,
    smallint_key  smallint,
    integer_key   integer,
    real_key      real,
    primary key (
        id,
        bigserial_key,
        numeric_key,
        bigint_key,
        float_key,
        double_key,
        smallint_key,
        integer_key, 
        real_key)
);

INSERT INTO __test_all_keys_are_numeric (
    bigserial_key, numeric_key, bigint_key, float_key, double_key,
    smallint_key, integer_key, real_key
)
SELECT
    gs      AS bigserial_key,
    (random() * 1000000)::numeric          AS numeric_key,
    (random() * 1000000000)::bigint        AS bigint_key,
    (random() * 10000)::float              AS float_key,
    (random() * 10000)::double precision   AS double_key,
    (random() * 32767)::smallint           AS smallint_key,
    (random() * 2147483647)::integer       AS integer_key,
    (random() * 10000)::real               AS real_key
FROM generate_series(1, 100000) gs;

create table __test_text_pk (
    serial_key serial,
    txt text,
    primary key (serial_key, txt)
);

insert into __test_text_pk (txt) select md5(random()::text) from generate_Series(1,100000) as s;