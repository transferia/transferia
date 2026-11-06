CREATE TABLE __test_num_table
(
    id SERIAL PRIMARY KEY,
    num INT
);

INSERT INTO __test_num_table (num)
SELECT generate_series(1, 10);

ANALYZE __test_num_table;

ALTER TABLE __test_num_table REPLICA IDENTITY FULL;
