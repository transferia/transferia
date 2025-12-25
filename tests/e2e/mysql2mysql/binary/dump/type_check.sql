-- needs to be sure there is db1
create table __test (
    `Id` binary(16) NOT NULL,
    `Version` int(11) NOT NULL,
    `Data` json NOT NULL,
    PRIMARY KEY (`Id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;


insert into __test values (0x8E1CF5E9084080E811ECA1542DE42988, 1, '{"а": "1"}');
insert into __test values (X'DAEBFCCC2D07B6B611ECA15454969110', 2, '{"а": "2"}');
insert into __test values (X'DAEBFCCC2D07B6B611ECA15454969111', 3, '"-"');
