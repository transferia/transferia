CREATE USER dt_shard IDENTIFIED BY dt_shard_pass;
ALTER USER dt_shard QUOTA UNLIMITED ON USERS;
GRANT CONNECT, RESOURCE TO dt_shard;

-- Tables for ROWID-range sharding test. Explicitly allocate multiple extents
-- so that dba_extents returns enough boundary ROWIDs to split the table into
-- multiple parts even with a tiny RowIDBytesPerShard (16 KB in tests).
CREATE TABLE dt_shard.shard_pk (
    id    NUMBER(10) NOT NULL PRIMARY KEY,
    value VARCHAR2(200) NOT NULL
);

CREATE TABLE dt_shard.shard_nopk (
    payload VARCHAR2(200) NOT NULL
);

BEGIN
    FOR i IN 1..1000 LOOP
        INSERT INTO dt_shard.shard_pk   VALUES (i, 'value_' || TO_CHAR(i));
        INSERT INTO dt_shard.shard_nopk VALUES ('payload_' || TO_CHAR(i));
    END LOOP;
    COMMIT;
END;

-- Force multiple extents: ALLOCATE EXTENT is the only reliable way in
-- locally-managed tablespaces (STORAGE clauses are ignored there).
ALTER TABLE dt_shard.shard_pk   ALLOCATE EXTENT;
ALTER TABLE dt_shard.shard_pk   ALLOCATE EXTENT;
ALTER TABLE dt_shard.shard_nopk ALLOCATE EXTENT;
ALTER TABLE dt_shard.shard_nopk ALLOCATE EXTENT;
