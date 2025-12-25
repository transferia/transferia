CREATE TABLE IF NOT EXISTS some_table(
                                           id BIGINT PRIMARY KEY AUTO_INCREMENT,
                                           name VARCHAR(255),
    description TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    status ENUM('active', 'inactive', 'pending') DEFAULT 'active',
    price DECIMAL(10,2),
    metadata JSON
    ) ENGINE=InnoDB;

INSERT INTO some_table (name, description, status, price, metadata) VALUES
                                                                         ('Product 1', 'Description for product 1', 'active', 10.99, '{"color": "red", "size": "M"}'),
                                                                         ('Product 2', 'Description for product 2', 'inactive', 20.99, '{"color": "blue", "size": "L"}'),
                                                                         ('Product 3', 'Description for product 3', 'pending', 30.99, '{"color": "green", "size": "S"}'),
                                                                         ('Product 4', 'Description for product 4', 'active', 40.99, '{"color": "yellow", "size": "XL"}'),
                                                                         ('Product 5', 'Description for product 5', 'inactive', 50.99, '{"color": "black", "size": "M"}');
