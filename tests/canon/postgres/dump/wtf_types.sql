create extension if not exists hstore;
create extension if not exists ltree;
create extension if not exists citext;

create table if not exists  public.wtf_types
(
    __primary_key        serial primary key,

    t_hstore hstore,
    t_iner inet,
    t_cidr cidr,
    t_macaddr macaddr,
    -- macaddr8 not supported by postgresql 9.6 (which is in our recipes)
    -- ltree - should be in special table, i suppose
    t_citext citext,

    j json,
    jb jsonb
);

INSERT INTO public.wtf_types VALUES
(
    default,

    'a=>1,b=>2', -- t_hstore
    '192.168.1.5', -- t_iner
    '10.1/16', -- t_cidr
    '08:00:2b:01:02:03', -- t_macaddr
    'Tom', -- t_citext

    '{"k": "v", "ki": 42, "kf": 1.2, "kn": null, "ks": "Ho Ho Ho my name''s \"SANTA CLAWS\""}', -- j json
    '"\"String in quotes\""' -- jb jsonb
);

-- insert into public.wtf_types (__primary_key, j, jb)
-- values
--     -- null
--     (default, 'null', 'null'),
--
--     -- boolean
--     (default, 'true', 'true'),
--     (default, 'false', 'false'),
--
--     -- integers
--     (default, '0', '0'),
--     (default, '-0', '-0'),
--     (default, '1', '1'),
--     (default, '-1', '-1'),
--     (default, '2147483647', '2147483647'),
--     (default, '-2147483648', '-2147483648'),
--     (default, '9223372036854775807', '9223372036854775807'),
--     (default, '-9223372036854775808', '-9223372036854775808'),
--
--     -- floating point
--     (default, '1.0', '1.0'),
--     (default, '-1.0', '-1.0'),
--     (default, '0.000001', '0.000001'),
--     (default, '123.456789', '123.456789'),
--     (default, '999999999999999999999999999999', '999999999999999999999999999999'),
--
--     -- exponential
--     (default, '1e3', '1e3'),
--     (default, '1E+3', '1E+3'),
--     (default, '1e-3', '1e-3'),
--     (default, '-1.23e10', '-1.23e10'),
--     (default, '1e100', '1e100'),
--
--     -- strings
--     (default, '""', '""'),
--     (default, '"a"', '"a"'),
--     (default, '"hello"', '"hello"'),
--     (default, '"hello world"', '"hello world"'),
--     (default, '"hello\nworld"', '"hello\nworld"'),
--     (default, '"quote: \""', '"quote: \""'),
--     (default, '"slash: \\"', '"slash: \\"'),
--     (default, '"unicode: Привет"', '"unicode: Привет"'),
--     (default, '"emoji 😀"', '"emoji 😀"'),
--     (default, '"null"', '"null"'),
--     (default, '"true"', '"true"'),
--     (default, '"123"', '"123"'),
--
--     -- empty arrays
--     (default, '[]', '[]'),
--
--     -- arrays of primitives
--     (default, '[null]', '[null]'),
--     (default, '[true,false]', '[true,false]'),
--     (default, '[0,1,-1]', '[0,1,-1]'),
--     (default, '["a","b","c"]', '["a","b","c"]'),
--     (default, '[1,"two",true,null]', '[1,"two",true,null]'),
--
--     -- nested arrays
--     (default, '[[1,2],[3,4]]', '[[1,2],[3,4]]'),
--     (default, '[[[1]]]', '[[[1]]]'),
--     (default, '[[]]', '[[]]'),
--     (default, '[{},[]]', '[{},[]]'),
--
--     -- empty objects
--     (default, '{}', '{}'),
--
--     -- simple objects
--     (default, '{"a":1}', '{"a":1}'),
--     (default, '{"a":"b"}', '{"a":"b"}'),
--     (default, '{"bool":true}', '{"bool":true}'),
--     (default, '{"null":null}', '{"null":null}'),
--
--     -- mixed objects
--     (
--         default,
--         '{"string":"value","number":123,"bool":true,"null":null}',
--         '{"string":"value","number":123,"bool":true,"null":null}'
--     ),
--
--     -- nested objects
--     (
--         default,
--         '{"a":{"b":{"c":1}}}',
--         '{"a":{"b":{"c":1}}}'
--     ),
--
--     -- arrays inside objects
--     (
--         default,
--         '{"items":[1,2,3]}',
--         '{"items":[1,2,3]}'
--     ),
--
--     -- objects inside arrays
--     (
--         default,
--         '[{"id":1},{"id":2}]',
--         '[{"id":1},{"id":2}]'
--     ),
--
--     -- object key order test (json vs jsonb)
--     (
--         default,
--         '{"b":2,"a":1}',
--         '{"b":2,"a":1}'
--     ),
--
--     -- duplicate keys (jsonb keeps last value)
--     (
--         default,
--         '{"a":1,"a":2}',
--         '{"a":1,"a":2}'
--     ),
--
--     -- whitespace normalization test
--     (
--         default,
--         '{ "key" : "value" }',
--         '{ "key" : "value" }'
--     ),
--
--     -- escaped characters
--     (
--         default,
--         '"\t\r\n\b\f"',
--         '"\t\r\n\b\f"'
--     ),
--
--     -- unicode escapes
--     (
--         default,
--         '"\u0048\u0065\u006c\u006c\u006f"',
--         '"\u0048\u0065\u006c\u006c\u006f"'
--     ),
--
--     -- deeply mixed structure
--     (
--         default,
--         '{"users":[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}],"active":true,"count":2}',
--         '{"users":[{"id":1,"name":"Alice"},{"id":2,"name":"Bob"}],"active":true,"count":2}'
--     )
-- ;

-- empty case

INSERT INTO public.wtf_types
(
    t_hstore,
    t_iner,
    t_cidr,
    t_macaddr,
    t_citext,
    j,
    jb
)
VALUES
(
    ''::hstore,
    '0.0.0.0'::inet,
    '0.0.0.0/0'::cidr,
    '00:00:00:00:00:00'::macaddr,
    ''::citext,
    '{}'::json,
    '{}'::jsonb
);

-- null case

INSERT INTO public.wtf_types (__primary_key) VALUES (default);
