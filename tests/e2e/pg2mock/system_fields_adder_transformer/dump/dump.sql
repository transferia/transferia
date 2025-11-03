CREATE TABLE test (
    i   INT PRIMARY KEY,
    val TEXT
);

CREATE TABLE test_not_transformed (
    i   INT PRIMARY KEY,
    val TEXT
);

INSERT INTO test VALUES
(1, '1'), (2, '2'), (3, '3');

INSERT INTO test_not_transformed VALUES
(1, '1'), (2, '2'), (3, '3');

UPDATE test SET val = '10' WHERE i = 1;
UPDATE test_not_transformed SET val = '10' WHERE i = 1;
