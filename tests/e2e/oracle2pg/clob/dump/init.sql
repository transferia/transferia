CREATE USER dt_test IDENTIFIED BY dt_test_pass;
ALTER USER dt_test QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO dt_test;

CREATE TABLE dt_test.docs (
    id    NUMBER(10) PRIMARY KEY,
    body  CLOB,
    nbody NCLOB
);

INSERT INTO dt_test.docs VALUES (1, 'short text', 'short ntext');
INSERT INTO dt_test.docs VALUES (2, RPAD('x', 4001, 'x'), RPAD('n', 4001, 'n'));
INSERT INTO dt_test.docs VALUES (3, NULL, NULL);
COMMIT;

CREATE OR REPLACE FUNCTION clob_to_blob(p_clob CLOB) RETURN BLOB IS
  l_blob         BLOB;
  l_dest_offset  INTEGER := 1;
  l_src_offset   INTEGER := 1;
  l_lang_context INTEGER := DBMS_LOB.DEFAULT_LANG_CTX;
  l_warning      INTEGER;
BEGIN
  IF p_clob IS NULL THEN RETURN NULL; END IF;
  DBMS_LOB.CREATETEMPORARY(l_blob, TRUE);
  DBMS_LOB.CONVERTTOBLOB(l_blob, p_clob, DBMS_LOB.LOBMAXSIZE,
    l_dest_offset, l_src_offset, DBMS_LOB.DEFAULT_CSID, l_lang_context, l_warning);
  RETURN l_blob;
END;
