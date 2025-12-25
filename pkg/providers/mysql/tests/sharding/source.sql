CREATE TABLE orders (
    id INT NOT NULL AUTO_INCREMENT,
    order_date DATE NOT NULL,
    customer_name VARCHAR(100),
    amount DECIMAL(10,2),
    PRIMARY KEY (id, order_date)
)
PARTITION BY RANGE (YEAR(order_date)) (
    PARTITION p2022 VALUES LESS THAN (2023),
    PARTITION p2023 VALUES LESS THAN (2024),
    PARTITION p2024 VALUES LESS THAN (2025),
    PARTITION p_future VALUES LESS THAN MAXVALUE
);

INSERT INTO orders (order_date, customer_name, amount) VALUES
   ('2022-05-10', 'Alice', 150.00),
   ('2022-08-21', 'Bob', 200.00),
   ('2023-03-15', 'Charlie', 300.00),
   ('2023-11-05', 'David', 400.00),
   ('2024-01-10', 'Eve', 500.00),
   ('2024-07-18', 'Frank', 600.00);
