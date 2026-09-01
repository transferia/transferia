-- rows are split across two different days so that daily rotation
-- (PartSize=1, PartType=d) produces two separate YT tables on snapshot
create table __test (
    id int PRIMARY KEY,
    ts   timestamp,
    astr varchar(50)
);

insert into __test values
(1, TIMESTAMP '2026-07-17 15:30:00', 'astr1'),
(2, TIMESTAMP '2026-07-17 15:30:00', 'astr2'),
(3, TIMESTAMP '2026-07-15 15:30:00', 'astr3'),
(4, TIMESTAMP '2026-07-15 15:30:00', 'astr4'),
(5, TIMESTAMP '2026-07-15 15:30:00', 'astr5');
