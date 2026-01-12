CREATE TABLE root_number (
    id     BIGSERIAL PRIMARY KEY,
    amount jsonb NOT NULL
);

INSERT INTO root_number (amount) VALUES
    ('340791.17999998387');

