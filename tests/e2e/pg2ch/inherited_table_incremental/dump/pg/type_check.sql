CREATE TABLE measurement_declarative (
    id         int not null,
    logdate    date not null,
    unitsales  int
) PARTITION BY RANGE (logdate);

CREATE TABLE measurement_declarative_y2006m02 PARTITION OF measurement_declarative
    FOR VALUES FROM ('2006-02-01') TO ('2006-03-01');
CREATE TABLE measurement_declarative_y2006m03 PARTITION OF measurement_declarative
    FOR VALUES FROM ('2006-03-01') TO ('2006-04-01');
CREATE TABLE measurement_declarative_y2006m04 PARTITION OF measurement_declarative
    FOR VALUES FROM ('2006-04-01') TO ('2006-05-01');

CREATE TABLE measurement_declarative_y2006m05 (
    id         int not null,
    logdate    date not null,
    unitsales  int
);

--CREATE TABLE measurement_declarative_y2006m05
--  (LIKE measurement_declarative INCLUDING DEFAULTS INCLUDING CONSTRAINTS);
ALTER TABLE measurement_declarative_y2006m05 ADD CONSTRAINT constraint_y2006m05
   CHECK ( logdate >= DATE '2006-05-01' AND logdate < DATE '2006-06-01' );

--ALTER TABLE measurement_declarative ATTACH PARTITION measurement_declarative_y2006m05
--    FOR VALUES FROM ('2006-05-01') TO ('2006-06-01' );


ALTER TABLE measurement_declarative_y2006m02 ADD PRIMARY KEY (id, logdate);
ALTER TABLE measurement_declarative_y2006m03 ADD PRIMARY KEY (id, logdate);
ALTER TABLE measurement_declarative_y2006m04 ADD PRIMARY KEY (id, logdate);
ALTER TABLE measurement_declarative_y2006m05 ADD PRIMARY KEY (id, logdate);

INSERT INTO measurement_declarative(id, logdate, unitsales)
VALUES
(1, '2006-02-02', 1),
(2, '2006-02-02', 1),
(3, '2006-03-03', 1),
(4, '2006-03-03', 1),
(5, '2006-03-03', 1),
(10, '2006-04-03', 1),
(11, '2006-04-03', 1),
(12, '2006-04-03', 1);

INSERT INTO measurement_declarative_y2006m05(id, logdate, unitsales)
VALUES
(21, '2006-05-01', 1),
(22, '2006-05-02', 1);

ALTER TABLE measurement_declarative ATTACH PARTITION measurement_declarative_y2006m05
    FOR VALUES FROM ('2006-05-01') TO ('2006-06-01' );
