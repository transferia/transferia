CREATE TABLE tv_table(i INT PRIMARY KEY, cname TEXT);
CREATE VIEW odd_channels AS SELECT i, cname FROM tv_table WHERE i > 2;
