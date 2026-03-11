create table __test (
    id SERIAL PRIMARY KEY,
    name jsonb
);

INSERT INTO __test VALUES (1, jsonb_build_object('value', REPEAT('x',16777206)));