CREATE TABLE test_basic (
    aid    integer   PRIMARY KEY,
    bid    integer,
    value  text
);
CREATE UNIQUE INDEX uindex_basic ON test_basic (bid);


CREATE TABLE test_change_pkey (
    aid    integer   PRIMARY KEY,
    bid    integer,
    value  text
);
CREATE UNIQUE INDEX uindex_change_pkey ON test_change_pkey (bid);
