CREATE TABLE test_table
(
    id int
);

INSERT INTO test_table
SELECT id
FROM generate_series(1, 100) AS t(id);

---

CREATE TABLE test_timestamp(
    id integer primary key,
    tsz timestamp with time zone,
    ts timestamp without time zone,
    ts6 timestamp(6) with time zone,
    t timestamp not null,
    mydate date
);

INSERT INTO test_timestamp VALUES
    (1, '2004-10-19 10:23:54+02', '2004-10-19 10:23:54', '2004-10-19 10:23:54', '2004-10-19 10:23:54', '1999-03-04'),
    (2, '2004-10-19 10:23:54+02', '2004-10-19 10:23:54', '2004-10-19 10:23:54', '2004-10-19 10:23:54', '1999-03-04');

---

CREATE TABLE test_timestamp2(
    id integer primary key,
    tsz timestamp with time zone,
    ts timestamp without time zone,
    t timestamp not null
);

INSERT INTO test_timestamp2 VALUES
    (1, '2004-10-19 10:23:54+02', '2004-10-19 10:23:54', '2004-10-19 10:23:54'),
    (2, '2004-10-19 10:23:54+02', '2004-10-19 10:23:54', '2004-10-19 10:23:54');
