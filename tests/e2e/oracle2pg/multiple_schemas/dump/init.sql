CREATE USER dt_test IDENTIFIED BY dt_test_pass;
ALTER USER dt_test QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO dt_test;

CREATE USER dt_test2 IDENTIFIED BY dt_test2_pass;
ALTER USER dt_test2 QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO dt_test2;

CREATE TABLE dt_test.orders (
    id   NUMBER(10)    PRIMARY KEY,
    item VARCHAR2(100) NOT NULL
);

CREATE TABLE dt_test2.clients (
    id   NUMBER(10)    PRIMARY KEY,
    name VARCHAR2(100) NOT NULL
);

INSERT INTO dt_test.orders  VALUES (1, 'Widget');
INSERT INTO dt_test.orders  VALUES (2, 'Gadget');
INSERT INTO dt_test.orders  VALUES (3, 'Doohickey');
INSERT INTO dt_test2.clients VALUES (1, 'Alice');
INSERT INTO dt_test2.clients VALUES (2, 'Bob');
INSERT INTO dt_test2.clients VALUES (3, 'Charlie');
COMMIT;
