--
-- Name: __english_collation; Type: COLLATION; Schema: public; Owner: -
--

CREATE COLLATION __english_collation (provider = libc, locale = 'en_US.UTF-8');

--
-- Name: __test; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE __test (
    id   integer,
    name character varying(255)
);

--
-- Name: __name_changes(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION __name_changes() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    IF NEW.name <> OLD.name THEN
         INSERT INTO __test(id,name)
         VALUES(OLD.id,OLD.name);
    END IF;

    RETURN NEW;
END;
$$;

--
-- Name: __test __name_changes_trigger; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER __name_changes_trigger BEFORE UPDATE ON __test FOR EACH ROW EXECUTE PROCEDURE __name_changes();

--
-- Name: __test __test_policy; Type: POLICY; Schema: public; Owner: db_user
--

CREATE POLICY __test_policy ON __test USING (((name)::text = 'test'::text));

CREATE VIEW __test_view AS SELECT id, name FROM __test WHERE id > 0;

CREATE MATERIALIZED VIEW __test_materialized_view AS SELECT id, name FROM __test WHERE id < 0;

CREATE INDEX __test_index ON __test  USING btree(name);

CREATE SCHEMA santa;

CREATE TYPE santa.my_enum AS ENUM ('RED', 'BLUE');
CREATE CAST (varchar AS santa.my_enum) WITH INOUT AS IMPLICIT;

CREATE TABLE santa."Ho-Ho-Ho"(i SERIAL PRIMARY KEY, t TEXT, f FLOAT, j santa.my_enum);

INSERT INTO santa."Ho-Ho-Ho"(t, f, j) VALUES ('merry', 1.0, 'BLUE'), ('Christmas', 2.0, 'RED');

CREATE SEQUENCE santa."Rudolf" START 2;

CREATE INDEX hoho_index ON santa."Ho-Ho-Ho" USING btree(t);
ALTER TABLE santa."Ho-Ho-Ho" ADD CONSTRAINT hoho_unq UNIQUE(f);


CREATE TABLE wide_boys(column_1 int, column_2 int) PARTITION BY RANGE (column_2);

-- first type of part attachment
CREATE TABLE wide_boys_part_1 PARTITION OF wide_boys FOR VALUES FROM (0) TO (10);

-- second type of part attachment
CREATE TABLE wide_boys_part_2 (column_1 int primary key, column_2 int);
ALTER TABLE wide_boys ATTACH PARTITION wide_boys_part_2 FOR VALUES FROM (10) TO (25);

-- foreign and primary key
CREATE TABLE table_with_pk (
    id   integer
);
ALTER TABLE table_with_pk ADD CONSTRAINT PK_table_with_pk PRIMARY KEY (id);

CREATE TABLE table_with_fk (
    id integer
);
ALTER TABLE table_with_fk ADD CONSTRAINT FK_table_with_fk FOREIGN KEY (id) REFERENCES table_with_pk(id);

CREATE TYPE santa."my custom type" AS (
    field1 VARCHAR,
    field2 INT
);

CREATE OR REPLACE FUNCTION santa.process_my_custom_type(IN input santa."my custom type", VARIADIC arr INT[])
    RETURNS VARCHAR AS $$
BEGIN
    RETURN 'Field 1: ' || input.field1 || ', Field 2: ' || input.field2;
END;
$$ LANGUAGE plpgsql;

-- this function will be extracted if you transfer tables from public and santa schemas
CREATE OR REPLACE FUNCTION text_to_my_enum(input varchar) RETURNS santa.my_enum AS $$
BEGIN
END;
$$ LANGUAGE plpgsql;

-- ugly names
CREATE SCHEMA ugly;

CREATE TABLE ugly.ugly_table(
    ugly int
);

CREATE TYPE ugly."my "" enum ():.* " AS ENUM ('ugly', 'enum');

CREATE OR REPLACE FUNCTION ugly."function for cast ugly enum"(input ugly."my "" enum ():.* ")
    RETURNS VARCHAR AS $$
BEGIN
END;
$$ LANGUAGE plpgsql;

CREATE CAST (ugly."my "" enum ():.* " AS varchar) WITH FUNCTION ugly."function for cast ugly enum" AS ASSIGNMENT;

-- function cast from other schema
CREATE SCHEMA only_type;
CREATE TABLE only_type.table(a int);
CREATE TYPE only_type.type AS ENUM ('a', 'b');

CREATE FUNCTION ugly.function_with_arg_from_santa(only_type.type, int, boolean)
    RETURNS TEXT AS $$
BEGIN
END;
$$ LANGUAGE plpgsql;
CREATE CAST (only_type.type AS TEXT) WITH FUNCTION ugly.function_with_arg_from_santa(only_type.type, int, boolean) AS ASSIGNMENT;

-- index attach
CREATE SCHEMA ia;
CREATE TABLE ia.ia_table (
    ia integer
)
    PARTITION BY RANGE (ia);

CREATE TABLE ia.ia_part_1 (
    ia integer
);

ALTER TABLE ONLY ia.ia_table ATTACH PARTITION ia.ia_part_1 FOR VALUES FROM (0) TO (10);

CREATE INDEX ia_idx ON ONLY ia.ia_table USING btree (ia);

CREATE INDEX ia_idx_part_1 ON ia.ia_part_1 USING btree (ia);

CREATE INDEX ia_part_1_ia_idx ON ia.ia_part_1 USING btree (ia);

ALTER INDEX ia.ia_idx ATTACH PARTITION ia.ia_part_1_ia_idx;

-- functions with problems
CREATE SCHEMA only_functions;
CREATE TABLE only_functions.table_for_functions (id INT PRIMARY KEY);

CREATE FUNCTION only_functions.regex_quote(_name character varying) 
RETURNS character varying 
LANGUAGE plpgsql IMMUTABLE 
AS $$
BEGIN
    RETURN lower(regexp_replace(_name, '[\"'']', '','g'));
END;
$$;

CREATE SCHEMA schema_with_bad_function;
create table schema_with_bad_function.temp_table(id int);
CREATE OR REPLACE FUNCTION schema_with_bad_function.function_with_comment()
 RETURNS TABLE(id int)
 LANGUAGE plpgsql
AS $function$
begin
--
--     return(select temp_table);
--
    return query select * from temp_table;
END;
$function$
