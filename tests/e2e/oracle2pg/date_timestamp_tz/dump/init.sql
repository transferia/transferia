CREATE USER dt_test IDENTIFIED BY dt_test_pass;
ALTER USER dt_test QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO dt_test;

-- Table with DATE, TIMESTAMP, TIMESTAMP WITH TIME ZONE, and TIMESTAMP WITH LOCAL TIME ZONE.
-- Fixed deterministic values (no SYSDATE) so that assertions are reproducible.
CREATE TABLE dt_test.date_tz_test (
    id         NUMBER(10) PRIMARY KEY,
    col_date   DATE,
    col_ts     TIMESTAMP(6),
    col_tstz   TIMESTAMP WITH TIME ZONE,
    col_tsltz  TIMESTAMP WITH LOCAL TIME ZONE
);

-- Row 1: fixed wall-clock values matching the user-reported scenario.
-- col_date = 2026-04-28 17:00:24 (same as the bug report).
-- col_ts, col_tstz, col_tsltz have sub-second precision to verify TIMESTAMP handling.
INSERT INTO dt_test.date_tz_test VALUES (
    1,
    TO_DATE('2026-04-28 17:00:24', 'YYYY-MM-DD HH24:MI:SS'),
    TIMESTAMP '2026-04-28 17:00:24.123456',
    TIMESTAMP '2026-04-28 17:00:24.123456 +03:00',
    TIMESTAMP '2026-04-28 17:00:24.123456 +03:00'
);

-- Row 2: epoch boundary — verifies that zero/near-zero dates survive the transfer.
INSERT INTO dt_test.date_tz_test VALUES (
    2,
    TO_DATE('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
    TIMESTAMP '1970-01-01 00:00:00.000001',
    TIMESTAMP '1970-01-01 00:00:00.000001 UTC',
    TIMESTAMP '1970-01-01 00:00:00.000001 UTC'
);

-- Row 3: all NULL (except PK) — verifies NULL handling.
INSERT INTO dt_test.date_tz_test (id) VALUES (3);

-- Row 4: SYSDATE/SYSTIMESTAMP — diagnostic only, assertions are skipped because
-- the values depend on container start time and wall clock.
INSERT INTO dt_test.date_tz_test VALUES (
    4, SYSDATE, SYSTIMESTAMP, SYSTIMESTAMP, SYSTIMESTAMP
);

COMMIT;
