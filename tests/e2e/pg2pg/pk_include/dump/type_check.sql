CREATE TABLE problems_by_day (
    place_id integer NOT NULL,
    problem text NOT NULL,
    date date NOT NULL,
    numerator integer,
    denominator integer
);
CREATE UNIQUE INDEX problems_by_day_pkey ON problems_by_day (place_id, problem, date) INCLUDE (numerator, denominator);
ALTER TABLE problems_by_day ADD PRIMARY KEY USING INDEX problems_by_day_pkey;

INSERT INTO problems_by_day (place_id, problem, date, numerator, denominator) VALUES
(1, 'pothole', '2024-01-01', 5, 100),
(1, 'pothole', '2024-01-02', 3, 100),
(2, 'graffiti', '2024-01-01', 7, 100);
