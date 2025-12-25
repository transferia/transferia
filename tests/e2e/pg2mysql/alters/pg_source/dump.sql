create table __test
(
    id   int,
    val1 int,
    val2 varchar,
    primary key (id)
);

insert into __test (id, val1, val2)
values (1, 1, 'a'),
       (2, 2, 'b')
