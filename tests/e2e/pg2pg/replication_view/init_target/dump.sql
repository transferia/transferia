CREATE TABLE tv_table(i INT, cname TEXT);
CREATE VIEW odd_channels AS SELECT i, cname FROM tv_table WHERE i > 2;
