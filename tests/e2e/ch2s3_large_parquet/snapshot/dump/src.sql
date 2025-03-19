CREATE DATABASE IF NOT EXISTS clickhouse_test;

CREATE TABLE IF NOT EXISTS clickhouse_test.sample
(
    `id` UInt32,
    `message` String,
    `date` Date,
    `user_id` UInt32,
    `status` String,
    `amount` Float64,
    `category` String,
    `tag` String,
    `is_active` UInt8,
    `created_at` DateTime,
    `updated_at` DateTime,
    `geo_location` String
)
    ENGINE = MergeTree
    PARTITION BY toMonday(date)
    ORDER BY date;

INSERT INTO clickhouse_test.sample
SELECT
    number AS id,
    concat('Message ', toString(number)) AS message,
    toDate('2024-01-01') + (number % 90) AS date,
    rand() % 100000 AS user_id,
    if(rand() % 2 = 0, 'active', 'inactive') AS status,
    round(rand() % 10000 / 100.0, 2) AS amount,
    if(rand() % 3 = 0, 'A', if(rand() % 3 = 1, 'B', 'C')) AS category,
    ['tag1', 'tag2', 'tag3'][rand() % 3] AS tag,
    rand() % 2 AS is_active,
    now() - INTERVAL (rand() % 1000) SECOND AS created_at,
    now() - INTERVAL (rand() % 500) SECOND AS updated_at,
    concat('Lat:', toString(rand() % 180 - 90), ', Lon:', toString(rand() % 360 - 180)) AS geo_location
FROM numbers(1000000);


