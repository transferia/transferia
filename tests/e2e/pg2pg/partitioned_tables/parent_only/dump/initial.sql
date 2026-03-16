CREATE TABLE partitioned_table (
    id         int not null,
    logdate    date not null,
    unitsales  int
) PARTITION BY RANGE (logdate);

CREATE TABLE partitioned_table_y2006m02 PARTITION OF partitioned_table
    FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');
CREATE TABLE partitioned_table_y2006m03 PARTITION OF partitioned_table
    FOR VALUES FROM ('2006-03-01') TO ('2006-04-01');
CREATE TABLE partitioned_table_y2006m04 PARTITION OF partitioned_table
    FOR VALUES FROM ('2006-04-01') TO ('2006-05-01');

CREATE TABLE partitioned_table_y2006m05 (
    id         int not null,
    logdate    date not null,
    unitsales  int
);

--CREATE TABLE partitioned_table_y2006m05
--  (LIKE partitioned_table INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE partitioned_table_y2006m05 ADD CONSTRAINT constraint_y2006m05
   CHECK ( logdate >= DATE '2006-05-01' AND logdate < DATE '2006-06-01' );

--ALTER TABLE partitioned_table ATTACH PARTITION partitioned_table_y2006m05
--    FOR VALUES FROM ('2006-05-01') TO ('2006-06-01' );


ALTER TABLE partitioned_table_y2006m02 ADD PRIMARY KEY (id, logdate);
ALTER TABLE partitioned_table_y2006m03 ADD PRIMARY KEY (id, logdate);
ALTER TABLE partitioned_table_y2006m04 ADD PRIMARY KEY (id, logdate);
ALTER TABLE partitioned_table_y2006m05 ADD PRIMARY KEY (id, logdate);

INSERT INTO partitioned_table(id, logdate, unitsales)
VALUES
(1, '2006-02-02', 1),
(2, '2006-02-02', 1),
(3, '2006-03-03', 1),
(4, '2006-03-03', 1),
(5, '2006-03-03', 1),
(10, '2006-04-03', 1),
(11, '2006-04-03', 1),
(12, '2006-04-03', 1);

INSERT INTO partitioned_table_y2006m05(id, logdate, unitsales)
VALUES
(21, '2006-05-01', 1),
(22, '2006-05-02', 1);

ALTER TABLE partitioned_table ATTACH PARTITION partitioned_table_y2006m05
    FOR VALUES FROM ('2006-05-01') TO ('2006-06-01' );

CREATE TABLE not_partitioned (
    id SERIAL PRIMARY KEY,
    val TEXT
);

INSERT INTO not_partitioned (val) VALUES ('foo');

CREATE SCHEMA second_schema;

CREATE TABLE second_schema.partitioned_table_non_public (
    id         int not null,
    logdate    date not null,
    unitsales  int
) PARTITION BY RANGE (logdate);

CREATE TABLE second_schema.partitioned_table_non_public_y2006m02 PARTITION OF second_schema.partitioned_table_non_public
    FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');
CREATE TABLE second_schema.partitioned_table_non_public_y2006m03 PARTITION OF second_schema.partitioned_table_non_public
    FOR VALUES FROM ('2006-03-01') TO ('2006-04-01');

INSERT INTO second_schema.partitioned_table_non_public(id, logdate, unitsales)
VALUES
(101, '2006-02-10', 1),
(102, '2006-02-11', 1),
(103, '2006-03-10', 1);
