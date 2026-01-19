CREATE DATABASE public;

CREATE TABLE IF NOT EXISTS public.clickhouse_chcustomerprofile
(
    `id` Int64,
    `uuid` UUID,
    `first_name` Nullable(String),
    `last_name` Nullable(String),
    `platform` LowCardinality(String),
    `bot_id` Int64,
    `profile_id` Int64,
    `__data_transfer_commit_time` UInt64,
    `__data_transfer_delete_time` UInt64
)
ENGINE = ReplacingMergeTree(__data_transfer_commit_time)
ORDER BY (bot_id, id);
