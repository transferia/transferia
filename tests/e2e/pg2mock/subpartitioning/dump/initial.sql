CREATE TABLE actions -- 10
(
    added_at            TIMESTAMPTZ NOT NULL,
    external_id         INT        NOT NULL,
    tenant              INT         NOT NULL
) PARTITION BY RANGE (added_at);
-- creating partitions without subpartitions
CREATE TABLE actions_2023 PARTITION OF actions FOR VALUES FROM ('2023-01-01') TO ('2024-01-01'); -- 4
-- creating partitions with subpartitions
CREATE TABLE actions_2024_01 PARTITION OF actions FOR VALUES FROM ('2024-01-01') TO ('2024-02-01') PARTITION BY RANGE (tenant); -- 1
CREATE TABLE actions_2024_01_01 PARTITION OF actions_2024_01 FOR VALUES FROM (1) TO (2); -- 1
CREATE TABLE actions_2024_01_02 PARTITION OF actions_2024_01 FOR VALUES FROM (2) TO (3); -- 0

CREATE TABLE actions_2024_02 PARTITION OF actions FOR VALUES FROM ('2024-02-01') TO ('2024-03-01') PARTITION BY RANGE (tenant); -- 4
CREATE TABLE actions_2024_02_01 PARTITION OF actions_2024_02 FOR VALUES FROM (1) TO (2); -- 2
CREATE TABLE actions_2024_02_02 PARTITION OF actions_2024_02 FOR VALUES FROM (2) TO (3); -- 2

CREATE TABLE actions_2024_03 PARTITION OF actions FOR VALUES FROM ('2024-03-01') TO ('2024-04-01') PARTITION BY RANGE (tenant); -- 1
CREATE TABLE actions_2024_03_01 PARTITION OF actions_2024_03 FOR VALUES FROM (1) TO (2); -- 0
CREATE TABLE actions_2024_03_02 PARTITION OF actions_2024_03 FOR VALUES FROM (2) TO (3); -- 1

INSERT INTO actions(added_at, external_id, tenant)
VALUES
('2023-01-02', 1, 1),
('2023-01-02', 2, 2),
('2023-01-03', 2, 2),
('2024-01-02', 3, 1),
('2024-01-02', 4, 1),
('2024-02-02', 2, 2),
('2024-02-02', 2, 1),
('2024-03-02', 2, 2);