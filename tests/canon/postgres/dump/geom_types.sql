create table if not exists public.geom_types
(
    __primary_key        serial primary key,

    t_point  point,
    t_line line,
    t_lseg lseg,
    t_box box,
    t_path path,
    t_polygon polygon,
    t_circle circle
);


INSERT INTO public.geom_types VALUES
(
    default,
    '( 1 , 2 )',
    '[ ( 1 , 2 ) , ( 2 , 3 ) ]',
    '( ( 1 , 2 ) , ( 2 , 3 ) )',
    '( ( 1 , 1 ) , ( 3 , 3 ) )',
    '[ ( 1 , 1 ) , ( 2 , 2 ) , ( 2 , 3 ) ]',
    '( ( 1 , 1 ) , ( 2 , 2 ) , ( 2 , 3 ), (1 , 1 ) )',
    '( ( 1 , 1 ) , 10 )'
);

-- there are no empty case

-- null case

INSERT INTO public.geom_types (__primary_key) VALUES (default);
