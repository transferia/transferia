create table if not exists public.numeric_types
(
    __primary_key      serial primary key,

    t_boolean          boolean,

    t_smallint         smallint,
    t_integer          integer,
    t_bigint           bigint,
    t_oid              oid,

    t_decimal          decimal,
    t_decimal_5        decimal(5),
    t_decimal_5_2      decimal(5, 2),

    t_numeric          numeric,

    t_numeric_5        numeric(5),
    t_numeric_5_2      numeric(5, 2),

    t_real             real,
    t_float_4          float4,
    t_float_8          float8,
    t_float_11         float(11),

    t_double_precision double precision,
    t_serial           serial,
    t_bigserial        bigserial,
    t_money            money
);

INSERT INTO public.numeric_types VALUES
(
    default,
    true, -- boolean

    -32768, -- smallint,
    -8388605, -- integer,
    1, -- bigint,
    2, -- oid,
    123456, -- decimal
    12345, -- decimal(5)
    123.23, -- decimal(5,2)

    1267650600228229401496703205376, -- numeric

    12345, -- numeric(5)
    123.67, -- numeric(5,2)

    1.45e-45, -- real, MAX PRECISION
    1.45e-45, -- float4,
    3.14e-324, -- float8,
    3.14e-45, -- float11,
    3.14e-324, -- double precision, MAX PRECISION
    0, -- serial,
    3372036854775807, -- bigserial,
    99.23 --money
);


INSERT INTO public.numeric_types VALUES
(
    default,
    true, -- boolean

    32767, -- smallint,
    8388605, -- integer,
    9223372036854775807, -- bigint,
    2, -- oid,
    123456.123456789012345678901234567890123456789012345678901234567890, -- decimal
    12345, -- decimal(5)
    123.23, -- decimal(5,2)

    123456.123456789012345678901234567890123456789012345678901234567890, -- numeric

    12345, -- numeric(5)
    123.67, -- numeric(5,2)

    123456.123456789012345, -- real,
    123456.123456789012345, -- float4,
    123456.123456789012345, -- float8,
    123456.123456789012345, -- float11,
    123456.123456789012345, -- double precision,
    2147483647, -- serial,
    9223372036854775807, -- bigserial,
    99.23123456 --money
);

INSERT INTO public.numeric_types
(
    t_numeric,
    t_numeric_5,
    t_numeric_5_2
)
VALUES
(
    0.0, -- numeric
    0, -- numeric(5)
    0.0 -- numeric(5,2)
);

INSERT INTO public.numeric_types
(
    t_numeric,
    t_numeric_5,
    t_numeric_5_2
)
VALUES
(
    -123456.123456789012345678901234567890123456789012345678901234567890, -- numeric
    -12345, -- numeric(5)
    -123.67 -- numeric(5,2)
);


-- INSERT INTO public.numeric_types
-- (
--     t_real,
--     t_double_precision
-- )
-- VALUES
-- (
--     'Infinity', -- real,
--     'Infinity' -- double precision
-- );

-- INSERT INTO public.numeric_types
-- (
--     t_real,
--     t_double_precision
-- )
-- VALUES
-- (
--     '-Infinity', -- real,
--     '-Infinity' -- double precision
-- );

-- INSERT INTO public.numeric_types
-- (
--     t_real,
--     t_double_precision
-- )
-- VALUES
-- (
--     'NaN', -- real,
--     'NaN' -- double precision
-- );

-- empty case

-- INSERT INTO public.numeric_types
-- (
--     t_boolean,
--
--     t_smallint,
--     t_integer,
--     t_bigint,
--     t_oid,
--
--     t_decimal,
--     t_decimal_5,
--     t_decimal_5_2,
--
--     t_numeric,
--     t_numeric_5,
--     t_numeric_5_2,
--
--     t_real,
--     t_float_4,
--     t_float_8,
--     t_float_11,
--
--     t_double_precision,
--     t_serial,
--     t_bigserial,
--     t_money
-- )
-- VALUES
-- (
--     false,
--
--     0,
--     0,
--     0,
--     0,
--
--     'NaN',
--     'NaN',
--     'NaN',
--
--     'NaN',
--     'NaN',
--     'NaN',
--
--     'Infinity',
--     '-Infinity',
--     'NaN',
--     'Infinity',
--
--     'NaN',
--     0,
--     0,
--     0::money
-- );

-- null case

INSERT INTO public.numeric_types (__primary_key) VALUES (default);
