create table __test
(
    id  int not null,
    val varchar not null default 'foo',
    primary key (id)
);

insert into __test (id, val)
values (1, 'a'),
       (2, 'b');
