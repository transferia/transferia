CREATE DATABASE clickhouse_test;

CREATE TABLE clickhouse_test.sample
(
    `id` UInt32,
    `message` String,
    `date` Date
)
    ENGINE = MergeTree
    Partition By toMonday(date)
    ORDER BY date;

INSERT INTO clickhouse_test.sample
(`id`, `message`, `date`)
VALUES
    (101, 'Hello, ClickHouse!','2024-03-18'),(102, 'Insert a lot of rows per batch','2024-03-17'),(103, 'Sort your data based on your commonly-used queries', '2024-03-16'),  (104, 'Granules are the smallest chunks of data read',      '2024-02-17')
;


