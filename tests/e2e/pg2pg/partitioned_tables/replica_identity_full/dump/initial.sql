CREATE TABLE parttable (
    id int NOT NULL,
    logdate date NOT NULL,
    value text
) PARTITION BY RANGE (logdate);

CREATE TABLE parttable_y2026m01 PARTITION OF parttable
    FOR VALUES FROM ('2026-01-01') TO ('2026-02-01');
CREATE TABLE parttable_y2026m02 PARTITION OF parttable
    FOR VALUES FROM ('2026-02-01') TO ('2026-03-01');

ALTER TABLE parttable REPLICA IDENTITY FULL;
ALTER TABLE parttable_y2026m01 REPLICA IDENTITY FULL;
ALTER TABLE parttable_y2026m02 REPLICA IDENTITY FULL;

INSERT INTO parttable VALUES
    (1, '2026-01-10', 'first partition'),
    (2, '2026-02-10', NULL);
