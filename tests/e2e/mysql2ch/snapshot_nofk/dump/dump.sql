-- Create table without a primary key
CREATE TABLE no_pk (
   id INT NOT NULL,
   name VARCHAR(100) NOT NULL,
   age INT NOT NULL,
   city VARCHAR(100) NOT NULL,
   email VARCHAR(100) NOT NULL
);

-- Insert 5 unique rows
INSERT INTO no_pk (id, name, age, city, email) VALUES
(1, 'Alice', 30, 'New York', 'alice@example.com'),
(2, 'Bob', 25, 'Los Angeles', 'bob@example.com'),
(3, 'Charlie', 35, 'Chicago', 'charlie@example.com'),
(4, 'Diana', 28, 'San Francisco', 'diana@example.com'),
(5, 'Eve', 40, 'Miami', 'eve@example.com');
