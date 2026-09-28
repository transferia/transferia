CREATE USER dt_test IDENTIFIED BY dt_test_pass;
ALTER USER dt_test QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO dt_test;

CREATE TABLE dt_test.test_snapshot (
    id       NUMBER(10)     PRIMARY KEY,
    name     VARCHAR2(100)  NOT NULL,
    num_val  NUMBER(10, 2),
    ts_val   TIMESTAMP      DEFAULT SYSTIMESTAMP
);

INSERT INTO dt_test.test_snapshot (id, name, num_val) VALUES (1, 'Alice', 100.50);
INSERT INTO dt_test.test_snapshot (id, name, num_val) VALUES (2, 'Bob',   200.25);
INSERT INTO dt_test.test_snapshot (id, name, num_val) VALUES (3, 'Charlie', NULL);
COMMIT;
