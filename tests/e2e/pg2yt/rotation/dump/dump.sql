-- needs to be sure there is db1
create table __test (
    id int,
    ts   timestamp,
    astr varchar(10),
    PRIMARY KEY (id, ts)
);

insert into __test values
(-1, now(), 'astr-1'),
(1, (now() - INTERVAL '1 DAY'), 'astr1'),
(2, (now() - INTERVAL '2 DAYS'), 'astr2'),
(3, (now() - INTERVAL '3 DAYS'), 'astr3');