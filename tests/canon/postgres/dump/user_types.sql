-- composite literal

DO
$$
BEGIN
    IF NOT EXISTS (SELECT * FROM pg_type typ
        INNER JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
        WHERE nsp.nspname = current_schema() AND typ.typname = 'user_type_composite_type')
    THEN
        create type user_type_composite_type AS (
            min numeric(10,4),
            max numeric(10,4)
        );
    END IF;
END;
$$;

-- enum

DO
$$
BEGIN
    IF NOT EXISTS (SELECT * FROM pg_type typ
        INNER JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
        WHERE nsp.nspname = current_schema() AND typ.typname = 'user_type_enum')
    THEN
CREATE TYPE user_type_enum AS ENUM
    (
        'VALUE_ONE',
        'VALUE_TWO',
        'VALUE_THREE'
    );
END IF;
END;
$$;

--

create extension if not exists hstore;
-- create extension if not exists ltree;
create extension if not exists citext;

-- TODO - add 'range' user-defined type

-- TODO - add 'domain'

-- DO
-- $$
-- BEGIN
--     IF NOT EXISTS (SELECT * FROM pg_type typ
--         INNER JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
--         WHERE nsp.nspname = current_schema() AND typ.typname = 'user_type_domain_positive_int')
--     THEN
-- CREATE DOMAIN user_type_domain_positive_int AS integer
--     CHECK (VALUE > 0);
-- END IF;
-- END;
-- $$;
--
-- DO
-- $$
-- BEGIN
--     IF NOT EXISTS (SELECT * FROM pg_type typ
--         INNER JOIN pg_namespace nsp ON nsp.oid = typ.typnamespace
--         WHERE nsp.nspname = current_schema() AND typ.typname = 'user_type_domain_email_string')
--     THEN
-- CREATE DOMAIN user_type_domain_email_string AS text
--     CHECK (
--         VALUE ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'
--     );
-- END IF;
-- END;
-- $$;

-- table

create table if not exists public.user_types
(
    __primary_key serial primary key,
    price_limits_col user_type_composite_type,
    enum_col user_type_enum,
    hstore_col hstore,
    citext_col citext
--     domain_int user_type_domain_positive_int,
--     domain_string user_type_domain_email_string
);

insert into user_types
values (
    default,
    '(0.99,9.99)'::user_type_composite_type,
    'VALUE_ONE'::user_type_enum,
    'a=>1,b=>2',
    'It''s a wonderful life'
--     1,
--     'john@example.com'
);

-- empty case

INSERT INTO user_types (__primary_key, citext_col, hstore_col) VALUES (default, '', ''::hstore); -- let's differ 'null' from empty string

-- null case

INSERT INTO user_types (__primary_key) VALUES (default);
