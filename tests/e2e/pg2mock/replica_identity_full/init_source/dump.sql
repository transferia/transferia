CREATE TABLE test (
    value text,
    another text,
    third int
);
ALTER TABLE test REPLICA IDENTITY FULL;

INSERT INTO test (value, another, third) VALUES
('1', 'another', 1),
('2', 'another', 1),
('3', 'another', 1),
('4', 'another', 1),
('5', 'another', 1),
('6', 'another', 1),
('7', 'another', 1),
('8', 'another', 1),
('9', 'another', 1),
('10', 'another', 1),
('11', null, 2)
;

INSERT INTO test (value) VALUES
('12'),
('13'),
('14'),
('15'),
('16'),
('17'),
('18'),
('19'),
('20')
;

INSERT INTO test (value, another) VALUES
('21', 'aaaaa')
;
