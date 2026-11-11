CREATE TABLE public.rows (id BIGINT PRIMARY KEY, payload TEXT, optional_value INT);
INSERT INTO public.rows SELECT value, 'row-' || value, CASE WHEN mod(value, 7) = 0 THEN NULL ELSE mod(value, 101) END FROM generate_series(1, 4096) value;
CREATE TABLE public.empty (id BIGINT PRIMARY KEY, payload TEXT);
