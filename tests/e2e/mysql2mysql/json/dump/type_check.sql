create table __test (
    `id` int NOT NULL,
    `val` json NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

insert into __test values (1, '{"а": "1"}');
insert into __test values (2, '"-"');
