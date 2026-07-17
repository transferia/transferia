CREATE USER dt_test IDENTIFIED BY dt_test_pass;
ALTER USER dt_test QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO dt_test;

CREATE TABLE dt_test.defaults_test (
    id                NUMBER(10)    PRIMARY KEY,
    col_int           NUMBER(10)    DEFAULT 42,
    col_varchar       VARCHAR2(100) DEFAULT 'ACTIVE',
    col_date_sysdate  DATE          DEFAULT SYSDATE,
    col_ts_systime    TIMESTAMP(6)  DEFAULT SYSTIMESTAMP,
    col_cts           TIMESTAMP     DEFAULT CURRENT_TIMESTAMP,
    col_curdate       DATE          DEFAULT CURRENT_DATE,
    col_no_default    VARCHAR2(100),
    col_null_default  VARCHAR2(100) DEFAULT NULL
);

INSERT INTO dt_test.defaults_test VALUES (
    1, 99, 'EXPLICIT',
    TO_DATE('2026-01-15 10:00:00', 'YYYY-MM-DD HH24:MI:SS'),
    TIMESTAMP '2026-01-15 10:00:00',
    TIMESTAMP '2026-01-15 10:00:00',
    TO_DATE('2026-01-15', 'YYYY-MM-DD'),
    'explicit', 'explicit'
);

INSERT INTO dt_test.defaults_test (id) VALUES (2);

COMMIT;
