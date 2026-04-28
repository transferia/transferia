CREATE USER dt_test IDENTIFIED BY dt_test_pass;
ALTER USER dt_test QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO dt_test;

CREATE TABLE dt_test.no_pk (
    code  VARCHAR2(20) NOT NULL,
    descr VARCHAR2(100)
);

CREATE UNIQUE INDEX dt_test.no_pk_uk ON dt_test.no_pk (code);

INSERT INTO dt_test.no_pk VALUES ('ALPHA', 'First item');
INSERT INTO dt_test.no_pk VALUES ('BETA',  'Second item');
INSERT INTO dt_test.no_pk VALUES ('GAMMA', NULL);
COMMIT;
