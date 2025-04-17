CREATE TABLE __test_num_table
(
    id SERIAL PRIMARY KEY,
    num INT,
    unsupported_pk_type json default null
);

INSERT INTO __test_num_table (num, unsupported_pk_type)
SELECT generate_series(1, 10), '{"a": "b"}';


alter table __test_num_table replica identity full;
