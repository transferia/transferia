CREATE TABLE __test1 (
    id integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ts timestamp
) engine = innodb default charset = utf8;

BEGIN;
    SET SESSION time_zone = '+00:00';
    INSERT INTO __test1 (ts) VALUES
        ('2020-12-23 10:11:12'),
        ('2020-12-23 14:15:16');
COMMIT;

CREATE TABLE __test2 (
    id integer NOT NULL AUTO_INCREMENT PRIMARY KEY,
    ts timestamp
) engine = innodb default charset = utf8;

BEGIN;
    SET SESSION time_zone = '+00:00';
    INSERT INTO __test2 (ts) VALUES
        ('2020-12-31 10:00:00'),
        ('2020-12-31 14:00:00');
COMMIT;
