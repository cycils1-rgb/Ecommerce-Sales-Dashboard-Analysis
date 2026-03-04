DROP DATABASE IF EXISTS ecommerce_project;
CREATE DATABASE ecommerce_project;
USE ecommerce_project;

-- 1. Customers
CREATE TABLE customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
    customer_city VARCHAR(50),
    customer_state VARCHAR(50)
);

-- 2. Products
CREATE TABLE products (
    product_id VARCHAR(50) PRIMARY KEY,
    product_category_name VARCHAR(100),
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g DECIMAL(10,2),
    product_length_cm DECIMAL(10,2),
    product_height_cm DECIMAL(10,2),
    product_width_cm DECIMAL(10,2)
);

-- 3. Orders
CREATE TABLE orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(30),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    FOREIGN KEY (customer_id) REFERENCES customers(customer_id)
);

-- 4. Order Items
CREATE TABLE order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    PRIMARY KEY (order_id, order_item_id),
    FOREIGN KEY (order_id) REFERENCES orders(order_id),
    FOREIGN KEY (product_id) REFERENCES products(product_id)
);

SHOW VARIABLES LIKE "secure_file_priv"; 

USE ecommerce_project;

-- Disable checks temporarily to empty tables with foreign keys
SET FOREIGN_KEY_CHECKS = 0;
TRUNCATE TABLE order_items;
TRUNCATE TABLE orders;
TRUNCATE TABLE products;
TRUNCATE TABLE customers;
SET FOREIGN_KEY_CHECKS = 1;

USE ecommerce_project;

-- 1. Customers
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_customers_dataset.csv'
INTO TABLE customers
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n' 
IGNORE 1 ROWS;

-- 2. Products (Handling empty numeric values)
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_products_dataset.csv'
INTO TABLE products
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(product_id, product_category_name, @v_name_len, @v_desc_len, @v_photos, @v_weight, @v_length, @v_height, @v_width)
SET 
    product_name_length = NULLIF(@v_name_len, ''),
    product_description_length = NULLIF(@v_desc_len, ''),
    product_photos_qty = NULLIF(@v_photos, ''),
    product_weight_g = NULLIF(@v_weight, ''),
    product_length_cm = NULLIF(@v_length, ''),
    product_height_cm = NULLIF(@v_height, ''),
    product_width_cm = NULLIF(@v_width, '');

-- 3. Orders (Handling empty date values)
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_orders_dataset.csv'
INTO TABLE orders
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS
(order_id, customer_id, order_status, order_purchase_timestamp, @v_app, @v_car, @v_cust, @v_est)
SET 
    order_approved_at = NULLIF(@v_app, ''),
    order_delivered_carrier_date = NULLIF(@v_car, ''),
    order_delivered_customer_date = NULLIF(@v_cust, ''),
    order_estimated_delivery_date = NULLIF(@v_est, '');

-- 4. Order Items
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/olist_order_items_dataset.csv'
INTO TABLE order_items
FIELDS TERMINATED BY ',' ENCLOSED BY '"'
LINES TERMINATED BY '\n'
IGNORE 1 ROWS;

SELECT 'Customers' AS table_name, COUNT(*) AS total_rows FROM customers
UNION ALL
SELECT 'Products', COUNT(*) FROM products
UNION ALL
SELECT 'Orders', COUNT(*) FROM orders
UNION ALL
SELECT 'Order_Items', COUNT(*) FROM order_items;

CREATE OR REPLACE VIEW v_sales_summary AS
SELECT 
    o.order_id,
    o.order_purchase_timestamp,
    c.customer_city,
    c.customer_state,
    p.product_category_name,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) AS total_order_value
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id;

SELECT 
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS month_year,
    ROUND(SUM(oi.price + oi.freight_value), 2) AS monthly_revenue,
    COUNT(o.order_id) AS total_orders -- Added 'o.' here to fix the error
FROM orders o
JOIN order_items oi ON o.order_id = oi.order_id
GROUP BY month_year
ORDER BY month_year;

WITH monthly_sales AS (
    SELECT 
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS month_year,
        ROUND(SUM(oi.price + oi.freight_value), 2) AS revenue
    FROM orders o
    JOIN order_items oi ON o.order_id = oi.order_id
    GROUP BY month_year
)
SELECT 
    month_year,
    revenue,
    LAG(revenue) OVER (ORDER BY month_year) AS previous_month_revenue,
    ROUND(((revenue - LAG(revenue) OVER (ORDER BY month_year)) / LAG(revenue) OVER (ORDER BY month_year)) * 100, 2) AS growth_percentage
FROM monthly_sales
ORDER BY month_year;

CREATE OR REPLACE VIEW v_master_sales AS
SELECT 
    o.order_id,
    o.order_purchase_timestamp,
    o.order_status,
    c.customer_city,
    c.customer_state,
    p.product_category_name,
    oi.price,
    oi.freight_value,
    (oi.price + oi.freight_value) AS total_value
FROM orders o
JOIN customers c ON o.customer_id = c.customer_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id;