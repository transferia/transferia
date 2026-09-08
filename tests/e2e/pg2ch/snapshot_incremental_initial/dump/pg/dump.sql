BEGIN;
CREATE TABLE __test_incremental (
    text text primary key,
    updated_at timestamp
);
COMMIT;
BEGIN;
insert into __test_incremental (text, updated_at)
select md5(random()::text), ('2020-01-01 00:00:00'::timestamp + interval '1 day' * s.s)
from generate_Series(1,2000) as s;
COMMIT;

CREATE TABLE __test_incremental_timestamptz (
    text text primary key,
    updated_at timestamp with time zone
);

INSERT INTO __test_incremental_timestamptz (text, updated_at) VALUES
    ('before', '2022-12-31 00:00:00'),
    ('at boundary', '2023-01-01 00:00:00'),
    ('after', '2023-01-02 00:00:00'),
    ('last', '2023-01-03 00:00:00');

CREATE TABLE __test_incremental_timestamp_literal (
    text text primary key,
    update_time timestamp
);

INSERT INTO __test_incremental_timestamp_literal (text, update_time) VALUES
    ('before', '2000-03-15 00:00:00'),
    ('at boundary', '2000-03-16 00:00:00'),
    ('after', '2000-03-17 00:00:00'),
    ('last', '2000-03-18 00:00:00');
