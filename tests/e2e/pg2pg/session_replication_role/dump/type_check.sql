CREATE TABLE __test_trigger (
    id          integer PRIMARY KEY,
    value       text    NOT NULL,
    write_count integer NOT NULL DEFAULT 0
);

CREATE FUNCTION bump_write_count() RETURNS trigger AS $$
BEGIN
    IF TG_OP = 'INSERT' THEN
        NEW.write_count := NEW.write_count + 1;
    ELSE
        NEW.write_count := OLD.write_count + 1;
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER bump_write_count_trigger
    BEFORE INSERT OR UPDATE ON __test_trigger
    FOR EACH ROW EXECUTE PROCEDURE bump_write_count();

INSERT INTO __test_trigger (id, value) VALUES (1, 'a');
INSERT INTO __test_trigger (id, value) VALUES (2, 'b');
UPDATE __test_trigger SET value = 'b_updated' WHERE id = 2;
