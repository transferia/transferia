DROP TABLE IF EXISTS `parted_sharded`;
CREATE TABLE `parted_sharded` (
    `id` INT NOT NULL,
    `v` VARCHAR(64) NOT NULL,
    PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
PARTITION BY HASH(`id`) PARTITIONS 2;

INSERT INTO `parted_sharded` (`id`, `v`) VALUES
    (1, 'row-a'),
    (2, 'row-b'),
    (3, 'row-c'),
    (4, 'row-d');
