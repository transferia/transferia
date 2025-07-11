create table testtable (
    id integer primary key,
    val jsonb
);
insert into testtable (id, val) values (1, '{"key1": "v1"}');
insert into testtable (id, val) values (2, '{"key2": 2}');
insert into testtable (id, val) values (3, '{"key3": "''"}');
insert into testtable (id, val) values (4, '{"key4": "\""}');
