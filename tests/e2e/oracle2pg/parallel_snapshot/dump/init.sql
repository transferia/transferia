CREATE USER dt_test IDENTIFIED BY dt_test_pass;
ALTER USER dt_test QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO dt_test;

CREATE TABLE dt_test.big_table (
    id  NUMBER(10)    PRIMARY KEY,
    val VARCHAR2(100) NOT NULL
);

BEGIN
    FOR i IN 1..2000 LOOP
        INSERT INTO dt_test.big_table VALUES (i, 'row_' || TO_CHAR(i));
    END LOOP;
    COMMIT;
END;
