CREATE TABLE test_table (
    id SERIAL PRIMARY KEY,
    data TEXT
);

insert into test_table (data) select 'row before the resume' from generate_series(1, 10);