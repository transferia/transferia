DO
$$
BEGIN
    IF NOT EXISTS (SELECT * FROM pg_type typ
                INNER JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
                WHERE nsp.nspname = current_schema() AND typ.typname = 'my_enum_type')
    THEN
        CREATE TYPE my_enum_type AS ENUM
        (
            'VALUE_ONE',
            'VALUE_TWO',
            'VALUE_THREE'
        );
    END IF;
END;
$$;

create table if not exists public.array_types
(
    __primary_key serial primary key,

    ARR_bl  boolean[],
    -- ARR_b   bit(1)[],
    -- ARR_b8  bit(8)[],
    -- ARR_vb  varbit(8)[],

    ARR_si   smallint[],
    -- ARR_ss   smallserial[],
    ARR_int  integer[],
    -- ARR_aid  serial[],
    ARR_id   bigint[],
    -- ARR_bid  bigserial[],
    ARR_oid_ oid[],

    ARR_real_ real[],
    ARR_d   double precision[],

    ARR_c   char[],
    ARR_str varchar(256)[],

    ARR_CHARACTER_ CHARACTER(4)[],
    ARR_CHARACTER_VARYING_ CHARACTER VARYING(5)[],
    ARR_TIMESTAMPTZ_ TIMESTAMPTZ[], -- timestamptz is accepted as an abbreviation for timestamp with time zone; this is a PostgreSQL extension
    ARR_tst TIMESTAMP WITH TIME ZONE[],
    ARR_TIMETZ_ TIMETZ[],
    ARR_TIME_WITH_TIME_ZONE_ TIME WITH TIME ZONE[],
    -- ARR_iv  interval[],
    -- ARR_ba  bytea[],

    -- ARR_j   json[],
    -- ARR_jb  jsonb[],
    -- ARR_x   xml[],

    ARR_uid uuid[],
    -- ARR_pt  point[],
    ARR_it  inet[],
    -- ARR_INT4RANGE_ INT4RANGE[],
    -- ARR_INT8RANGE_ INT8RANGE[],
    -- ARR_NUMRANGE_ NUMRANGE[],
    -- ARR_TSRANGE_ TSRANGE[],
    -- ARR_TSTZRANGE_ TSTZRANGE[],
    -- ARR_DATERANGE_ DATERANGE[],
    -- ENUM

    -- add, from our /Users/timmyb32r/arc/arcadia/transfer_manager/go/tests/e2e/pg2pg/replication/dump/type_check.sql:
    ARR_f   float[],
    ARR_i   int[],
    ARR_t   text[],

    -- ----------------------------------------------------------------------------------------------------------------

    ARR_DATE_ DATE[],
    ARR_TIME_ TIME[],
    ARR_TIME1 TIME(1)[], -- precision: This is a fractional digits number placed in the seconds’ field. This can be up to six digits. HH:MM:SS.pppppp
    ARR_TIME3 TIME(3)[],
    ARR_TIME6 TIME(6)[],

    ARR_TIMETZ__ TIME WITH TIME ZONE[],
    ARR_TIMETZ1 TIME(1) WITH TIME ZONE[],
    ARR_TIMETZ3 TIME(3) WITH TIME ZONE[],
    ARR_TIMETZ6 TIME(6) WITH TIME ZONE[],

    ARR_TIMESTAMP1 TIMESTAMP(1)[],
    ARR_TIMESTAMP3 TIMESTAMP(3)[],
    ARR_TIMESTAMP6 TIMESTAMP(6)[],
    ARR_TIMESTAMP TIMESTAMP[],

    --NUMERIC(precision) # selects a scale of 0
    --NUMERIC(precision, scale)
    -- 'numeric' type - it's bignum
    -- precision - digits in the whole number, that is, the number of digits to both sides of the decimal point
    -- scale     - count of decimal digits in the fractional part, to the right of the decimal point
    --
    -- example: So the number 23.5141 has a precision of 6 and a scale of 4. Integers can be considered to have a scale of zero
    -- In addition to ordinary numeric values, the numeric type has several special values:
    --     Infinity
    --     -Infinity
    --     NaN
    ARR_NUMERIC_ NUMERIC[],
    ARR_NUMERIC_5 NUMERIC(5)[],
    ARR_NUMERIC_5_2 NUMERIC(5,2)[],

    --DECIMAL
    -- The types decimal and numeric are equivalent
    ARR_DECIMAL_ DECIMAL[],
    ARR_DECIMAL_5 DECIMAL(5)[],
    ARR_DECIMAL_5_2 DECIMAL(5,2)[],

    --ENUM
    ARR_ENUM my_enum_type[]
);

insert into array_types values
(
    default,

    '{true,true}', -- ARR_bl  boolean[],
    -- '{1,1}'    -- ARR_b   bit(1)[],
    -- [io.debezium.relational.TableSchemaBuilder]
    -- org.apache.kafka.connect.errors.DataException: Invalid Java object for schema with type BOOLEAN: class java.util.ArrayList for field: "arr_b"

    -- ARR_b8  bit(8)[],
    -- ARR_vb  varbit(8)[],

    '{1,2}', -- ARR_si   smallint[],
    '{1,2}', -- ARR_int  integer[],
    '{1,2}', -- ARR_id   bigint[],
    '{1,2}', -- ARR_oid_ oid[],

    '{1.45e-10,1.45e-10}',   -- ARR_real_ real[],
    '{3.14e-100,3.14e-100}', -- ARR_d   double precision[],

    '{"1", "1"}', -- ARR_c   char[],
    '{"varchar_example", "varchar_example"}', -- ARR_str varchar(256)[],

    '{"abcd","abcd"}', -- ARR_CHARACTER_ CHARACTER(4)[],
    '{"varc","varc"}', -- ARR_CHARACTER_VARYING_ CHARACTER VARYING(5)[],
    '{"2004-10-19 10:23:54+02","2004-10-19 10:23:54+02"}', -- ARR_TIMESTAMPTZ_ TIMESTAMPTZ[], -- timestamptz is accepted as an abbreviation for timestamp with time zone; this is a PostgreSQL extension
    '{"2004-10-19 11:23:54+02","2004-10-19 11:23:54+02"}', -- ARR_tst TIMESTAMP WITH TIME ZONE[],
    '{"00:51:02.746572-08","00:51:02.746572-08"}',         -- ARR_TIMETZ_ TIMETZ[],
    '{"00:51:02.746572-08","00:51:02.746572-08"}',         -- ARR_TIME_WITH_TIME_ZONE_ TIME WITH TIME ZONE[],

    '{"a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11","a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11"}', -- ARR_uid uuid[],
    '{"192.168.100.128/25","192.168.100.128/25"}', -- ARR_it  inet[],


    '{"1.45e-10","1.45e-10"}',         -- ARR_f   float[],
    '{1,1}',                           -- ARR_i   int[],
    '{"text_example","text_example"}', -- ARR_t   text[],

    '{"January 8, 1999", "January 8, 1999"}', -- DATE_ DATE,

    '{"04:05:06", "04:05:06"}',               -- TIME_ TIME,
    '{"04:05:06.1", "04:05:06.1"}',           -- TIME1 TIME(1),
    '{"04:05:06.123", "04:05:06.123"}',       -- TIME3 TIME(3),
    '{"04:05:06.123456", "04:05:06.123456"}', -- TIME6 TIME(6),

    '{"2020-05-26 13:30:25-04", "2020-05-26 13:30:25-04"}',               -- TIMETZ__ TIME WITH TIME ZONE,
    '{"2020-05-26 13:30:25.5-04", "2020-05-26 13:30:25.5-04"}',           -- TIMETZ1 TIME(1) WITH TIME ZONE,
    '{"2020-05-26 13:30:25.575-04", "2020-05-26 13:30:25.575-04"}',       -- TIMETZ3 TIME(3) WITH TIME ZONE,
    '{"2020-05-26 13:30:25.575401-04", "2020-05-26 13:30:25.575401-04"}', -- TIMETZ6 TIME(6) WITH TIME ZONE,

    '{"2004-10-19 10:23:54.9", "2004-10-19 10:23:54.9"}',           -- TIMESTAMP1 TIMESTAMP(1),
    '{"2004-10-19 10:23:54.987", "2004-10-19 10:23:54.987"}',       -- TIMESTAMP6 TIMESTAMP(3),
    '{"2004-10-19 10:23:54.987654", "2004-10-19 10:23:54.987654"}', -- TIMESTAMP6 TIMESTAMP(6),
    '{"2004-10-19 10:23:54", "2004-10-19 10:23:54"}',               -- TIMESTAMP TIMESTAMP,

    '{"1267650600228229401496703205376","12676506002282294.01496703205376"}', -- NUMERIC_ NUMERIC,
    '{"12345","12345"}',                                                      -- NUMERIC_5 NUMERIC(5),
    '{"123.67","123.67"}',                                                    -- NUMERIC_5_2 NUMERIC(5,2),

    '{"123456","123456"}',                                                   -- DECIMAL_ DECIMAL,
    '{"12345","12345"}',                                                     -- DECIMAL_5 DECIMAL(5),
    '{"123.67","123.67"}',                                                   -- DECIMAL_5_2 DECIMAL(5,2),

    --     '{"a=>1,b=>2","a=>1,b=>2"}',                 -- HSTORE_ HSTORE,
    --     '{"192.168.1.5", "192.168.1.5"}',            -- INET_ INET,
    --     '{"10.1/16","10.1/16"}',                     -- CIDR_ CIDR,
    --     '{"08:00:2b:01:02:03","08:00:2b:01:02:03"}', -- MACADDR_ MACADDR,
    --     '{"Tom","Tom"}'                              -- CITEXT_ CITEXT

    ARRAY ['VALUE_ONE', 'VALUE_THREE']::my_enum_type[]                  --ENUM
);

-- empty value case

insert into array_types values
(
    default,

    '{}', -- ARR_bl  boolean[],
    -- '{1,1}'    -- ARR_b   bit(1)[],
    -- [io.debezium.relational.TableSchemaBuilder]
    -- org.apache.kafka.connect.errors.DataException: Invalid Java object for schema with type BOOLEAN: class java.util.ArrayList for field: "arr_b"

    -- ARR_b8  bit(8)[],
    -- ARR_vb  varbit(8)[],

    '{}', -- ARR_si   smallint[],
    '{}', -- ARR_int  integer[],
    '{}', -- ARR_id   bigint[],
    '{}', -- ARR_oid_ oid[],

    '{}',   -- ARR_real_ real[],
    '{}', -- ARR_d   double precision[],

    '{}', -- ARR_c   char[],
    '{}', -- ARR_str varchar(256)[],

    '{}', -- ARR_CHARACTER_ CHARACTER(4)[],
    '{}', -- ARR_CHARACTER_VARYING_ CHARACTER VARYING(5)[],
    '{}', -- ARR_TIMESTAMPTZ_ TIMESTAMPTZ[], -- timestamptz is accepted as an abbreviation for timestamp with time zone; this is a PostgreSQL extension
    '{}', -- ARR_tst TIMESTAMP WITH TIME ZONE[],
    '{}',         -- ARR_TIMETZ_ TIMETZ[],
    '{}',         -- ARR_TIME_WITH_TIME_ZONE_ TIME WITH TIME ZONE[],

    '{}', -- ARR_uid uuid[],
    '{}', -- ARR_it  inet[],


    '{}',         -- ARR_f   float[],
    '{}',                           -- ARR_i   int[],
    '{}', -- ARR_t   text[],

    '{}', -- DATE_ DATE,

    '{}',               -- TIME_ TIME,
    '{}',           -- TIME1 TIME(1),
    '{}',       -- TIME3 TIME(3),
    '{}', -- TIME6 TIME(6),

    '{}',               -- TIMETZ__ TIME WITH TIME ZONE,
    '{}',           -- TIMETZ1 TIME(1) WITH TIME ZONE,
    '{}',       -- TIMETZ3 TIME(3) WITH TIME ZONE,
    '{}', -- TIMETZ6 TIME(6) WITH TIME ZONE,

    '{}',           -- TIMESTAMP1 TIMESTAMP(1),
    '{}',       -- TIMESTAMP6 TIMESTAMP(3),
    '{}', -- TIMESTAMP6 TIMESTAMP(6),
    '{}',               -- TIMESTAMP TIMESTAMP,

    '{}', -- NUMERIC_ NUMERIC,
    '{}',                                                      -- NUMERIC_5 NUMERIC(5),
    '{}',                                                    -- NUMERIC_5_2 NUMERIC(5,2),

    '{}',                                                   -- DECIMAL_ DECIMAL,
    '{}',                                                     -- DECIMAL_5 DECIMAL(5),
    '{}',                                                   -- DECIMAL_5_2 DECIMAL(5,2),

    --     '{"a=>1,b=>2","a=>1,b=>2"}',                 -- HSTORE_ HSTORE,
    --     '{"192.168.1.5", "192.168.1.5"}',            -- INET_ INET,
    --     '{"10.1/16","10.1/16"}',                     -- CIDR_ CIDR,
    --     '{"08:00:2b:01:02:03","08:00:2b:01:02:03"}', -- MACADDR_ MACADDR,
    --     '{"Tom","Tom"}'                              -- CITEXT_ CITEXT

    '{}'                  --ENUM
);

-- null case

INSERT INTO public.array_types (__primary_key) VALUES (default);
