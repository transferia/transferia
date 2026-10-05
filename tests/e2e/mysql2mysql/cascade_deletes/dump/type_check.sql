CREATE TABLE `__test_A` (
    `a_id` integer NOT NULL PRIMARY KEY,
    `a_name` varchar(255) NOT NULL,
    UNIQUE (`a_name`)
) engine=innodb default charset=utf8;

CREATE TABLE `__test_B` (
    `b_id` integer NOT NULL PRIMARY KEY,
    `a_id` integer NOT NULL,
    `b_name` varchar(255) NOT NULL,
    FOREIGN KEY (`a_id`) REFERENCES `__test_A` (`a_id`) ON DELETE CASCADE
) engine=innodb default charset=utf8;

INSERT INTO `__test_A` (`a_id`, `a_name`) VALUES
(
 1, 'John'
)
,
(
 2, 'Andrew'
)
,
(
 3, 'Kate'
)
;

INSERT INTO `__test_B` (`b_id`, `a_id`, `b_name`) VALUES
(
 1, 1, 'just a random string'
)
,
(
 2, 1, 'another random string'
)
,
(
 3, 2, 'abracadabra'
)
;
