-- Create target database for our processed data
CREATE DATABASE customer_analytics;

\c customer_analytics;

-- Create tables for processed data
CREATE TABLE IF NOT EXISTS customer_summary (
    customer_id INT PRIMARY KEY,
    total_spent DECIMAL(10,2),
    transaction_count INT,
    avg_transaction DECIMAL(10,2),
    last_transaction_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS product_category_stats (
    product_category VARCHAR(50) PRIMARY KEY,
    total_revenue DECIMAL(10,2),
    transaction_count INT,
    avg_transaction_amount DECIMAL(10,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);