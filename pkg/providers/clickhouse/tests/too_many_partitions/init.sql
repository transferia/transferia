CREATE DATABASE IF NOT EXISTS test;

CREATE TABLE test.too_many_partitions_test
(
    `id` Int64,
    `value` String
)
ENGINE = MergeTree()
ORDER BY id
PARTITION BY id;
