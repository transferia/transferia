create table if not exists public.text_types
(
    __primary_key        serial primary key,
    t_text               text,

    t_char               char,
    t_varchar_256        varchar(256),

    t_character_         character(4),
    t_character_varying_ character varying(5),

    t_bit_1   bit(1),
    t_bit_8  bit(8),
    t_varbit_8  varbit(8),

    t_bytea              bytea,
    t_uuid               uuid
);

INSERT INTO public.text_types VALUES
(
    default,
    'text_example', -- text
    '1', -- char
    'varchar_example', -- varchar(256)
    'abcd', -- CHARACTER(4)
    'varc', -- CHARACTER VARYING(5)


    b'1', -- bit(1)
    b'10101111', -- bit(8),
    b'10101110', -- varbit(8)
    decode('CAFEBABE', 'hex'), -- bytea
    'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11' -- uuid
);

-- empty case

INSERT INTO public.text_types
(
    t_text,
    t_char,
    t_varchar_256,
    t_character_,
    t_character_varying_,
    t_bit_1,
    t_bit_8,
    t_varbit_8,
    t_bytea
)
VALUES
(
    '',
    '',
    '',
    '',
    '',
    B'0',
    B'00000000',
    B'',
    ''
);

-- null case

INSERT INTO public.text_types (__primary_key) VALUES (default);
