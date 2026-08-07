-- Create a demo table for personas
CREATE TABLE IF NOT EXISTS personas (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    strength_level INT,
    special_move VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert personas data
INSERT INTO personas (name, strength_level, special_move) VALUES
('Billy Herrington', 100, 'Anvil Drop'),
('Van Darkholme', 95, 'Whip of Submission'),
('Ricardo Milos', 90, 'Twerk of Power'),
('Mark Wolff', 85, 'Wolf Howl Slam'),
('Kazuhiko', 80, 'Smiling Slam');
