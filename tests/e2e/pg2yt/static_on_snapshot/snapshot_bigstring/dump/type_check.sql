create table __test (
    id SERIAL PRIMARY KEY,
    name TEXT
);

INSERT INTO __test VALUES (1, REPEAT('x',16777217));