CREATE TABLE json_special_cases_test (
    i BIGSERIAL PRIMARY KEY,
    j json,
    jb jsonb
);

INSERT INTO json_special_cases_test(j, jb) VALUES
(
    '{"ks": "vs", "ki": 42, "kf": 420.42, "kn": null}', -- j
    '{"ks": "vs", "ki": 42, "kf": 420.42, "kn": null}' -- jb
),
(
    '"Ho Ho Ho my name''s \"SANTA CLAWS\""', -- j
    '"Ho Ho Ho my name''s \"SANTA CLAWS\""' -- jb
),
(
    '"\"String in quotes\""', -- j
    '"\"String in quotes\""' -- jb
),
(
    '"\"\"String in double quotes\"\""', -- j
    '"\"\"String in double quotes\"\""' -- jb
);

---

CREATE TABLE test_timestamp(
    id integer primary key,
    tsz timestamp with time zone,
    ts timestamp without time zone,
    t timestamp not null
);

INSERT INTO test_timestamp VALUES
    (1, '2004-10-19 10:23:54+02', '2004-10-19 10:23:54', '2004-10-19 10:23:54')
;
