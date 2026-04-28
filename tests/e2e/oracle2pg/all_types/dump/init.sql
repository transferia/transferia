CREATE USER dt_test IDENTIFIED BY dt_test_pass;
ALTER USER dt_test QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO dt_test;

-- Original types — proven working in the baseline test.
CREATE TABLE dt_test.all_types (
    id           NUMBER(10)        PRIMARY KEY,
    col_int      NUMBER(10),
    col_decimal  NUMBER(15, 4),
    col_number   NUMBER,
    col_float    FLOAT(53),
    col_bfloat   BINARY_FLOAT,
    col_bdouble  BINARY_DOUBLE,
    col_varchar  VARCHAR2(200),
    col_nvarchar NVARCHAR2(100),
    col_char     CHAR(10),
    col_date     DATE,
    col_ts       TIMESTAMP(6),
    col_tstz     TIMESTAMP WITH TIME ZONE,
    col_raw      RAW(16),
    col_clob     CLOB
);
INSERT INTO dt_test.all_types VALUES (
    1, 42, 1234.5678, 99999999999, 3.14, 2.71, 1.41,
    'hello', N'мир', 'CHAR      ',
    TO_DATE('2024-01-15 10:30:00', 'YYYY-MM-DD HH24:MI:SS'),
    TIMESTAMP '2024-01-15 10:30:00.123456',
    TIMESTAMP '2024-01-15 10:30:00 UTC',
    HEXTORAW('0123456789ABCDEF0123456789ABCDEF'),
    'short clob text'
);
INSERT INTO dt_test.all_types VALUES (
    2, -1, -0.0001, 0, -1.5, -0.5, -0.25,
    'world', N'hello', 'ABC       ',
    TO_DATE('1970-01-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'),
    TIMESTAMP '1970-01-01 00:00:00.000001',
    TIMESTAMP '1970-01-01 00:00:00 UTC',
    HEXTORAW('FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF'),
    'another clob value'
);
INSERT INTO dt_test.all_types (id) VALUES (3);

-- NCHAR — national-charset fixed-length string.
CREATE TABLE dt_test.nchar_types (
    id        NUMBER(10) PRIMARY KEY,
    col_nchar NCHAR(10)
);
INSERT INTO dt_test.nchar_types VALUES (1, N'NCHAR     ');
INSERT INTO dt_test.nchar_types VALUES (2, N'XYZ       ');
INSERT INTO dt_test.nchar_types (id) VALUES (3);

-- TIMESTAMP WITH LOCAL TIME ZONE — stored as UTC, served in session TZ.
CREATE TABLE dt_test.tslocal_types (
    id          NUMBER(10) PRIMARY KEY,
    col_tslocal TIMESTAMP WITH LOCAL TIME ZONE
);
INSERT INTO dt_test.tslocal_types VALUES (1, TIMESTAMP '2024-01-15 10:30:00.123456');
INSERT INTO dt_test.tslocal_types VALUES (2, TIMESTAMP '1970-01-01 00:00:00.000001');
INSERT INTO dt_test.tslocal_types (id) VALUES (3);

-- NCLOB — national-charset LOB; requires TO_CLOB() to normalise OCI LOB type.
CREATE TABLE dt_test.nclob_types (
    id        NUMBER(10) PRIMARY KEY,
    col_nclob NCLOB
);
INSERT INTO dt_test.nclob_types VALUES (1, N'short nclob text');
INSERT INTO dt_test.nclob_types VALUES (2, N'another nclob value');
INSERT INTO dt_test.nclob_types (id) VALUES (3);

-- BLOB — binary LOB; read via DBMS_LOB.SUBSTR to avoid OCI LOB-scan limitations.
CREATE TABLE dt_test.blob_types (
    id       NUMBER(10) PRIMARY KEY,
    col_blob BLOB
);
INSERT INTO dt_test.blob_types VALUES (1, HEXTORAW('0102030405060708090A0B0C0D0E0F10'));
INSERT INTO dt_test.blob_types VALUES (2, HEXTORAW('DEADBEEFCAFEBABE0102030405060708'));
INSERT INTO dt_test.blob_types (id) VALUES (3);

-- LONG: deprecated character type; Oracle allows only one LONG column per table.
CREATE TABLE dt_test.long_text (
    id       NUMBER(10) PRIMARY KEY,
    col_long LONG
);
INSERT INTO dt_test.long_text VALUES (1, 'long character data');
INSERT INTO dt_test.long_text VALUES (2, 'another long value');
INSERT INTO dt_test.long_text (id) VALUES (3);

-- LONG RAW: deprecated binary type; Oracle allows only one LONG RAW column per table.
CREATE TABLE dt_test.long_binary (
    id           NUMBER(10) PRIMARY KEY,
    col_long_raw LONG RAW
);
INSERT INTO dt_test.long_binary VALUES (1, HEXTORAW('0102030405'));
INSERT INTO dt_test.long_binary VALUES (2, HEXTORAW('DEADBEEF'));
INSERT INTO dt_test.long_binary (id) VALUES (3);

COMMIT;
