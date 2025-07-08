BEGIN;
create table testtable (
    id text primary key,
    id2 varchar(5),
    val integer
);
insert into testtable (id, id2, val) values ('ID0', 'ID2_0', 0);
insert into testtable (id, id2, val) values ('ID1', 'ID2_1', 1);
insert into testtable (id, id2, val) values ('ID2', 'ID2_2', 2);
insert into testtable (id, id2, val) values ('ID3', 'ID2_3', 3);
COMMIT;

BEGIN;
SELECT pg_create_logical_replication_slot('testslot', 'wal2json');
COMMIT;
