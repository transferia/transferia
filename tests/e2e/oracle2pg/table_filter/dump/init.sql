CREATE USER dt_test IDENTIFIED BY dt_test_pass;
ALTER USER dt_test QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO dt_test;

CREATE TABLE dt_test.tbl_a (id NUMBER(10) PRIMARY KEY, val VARCHAR2(100));
CREATE TABLE dt_test.tbl_b (id NUMBER(10) PRIMARY KEY, val VARCHAR2(100));
CREATE TABLE dt_test.tbl_c (id NUMBER(10) PRIMARY KEY, val VARCHAR2(100));

INSERT INTO dt_test.tbl_a VALUES (1, 'a1');
INSERT INTO dt_test.tbl_a VALUES (2, 'a2');
INSERT INTO dt_test.tbl_b VALUES (1, 'b1');
INSERT INTO dt_test.tbl_b VALUES (2, 'b2');
INSERT INTO dt_test.tbl_c VALUES (1, 'c1');
INSERT INTO dt_test.tbl_c VALUES (2, 'c2');
COMMIT;
