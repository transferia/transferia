-- If function is applied before table, transfer will fail with "relation dependant_table does not exist".
CREATE TABLE public.dependant_table (
    id   integer PRIMARY KEY,
    name text
);

CREATE FUNCTION public.func_using_table()
RETURNS SETOF public.dependant_table
LANGUAGE sql
STABLE
AS $$
    SELECT id, name FROM public.dependant_table;
$$;
