-- Sales Analytics Database Schema
CREATE DATABASE IF NOT EXISTS sales_db;
USE sales_db;

-- Transactions Table (can handle multiple files)
CREATE TABLE IF NOT EXISTS transactions (
    id INT PRIMARY KEY AUTO_INCREMENT,
    transaction_id INT,
    source_file VARCHAR(100) DEFAULT 'sales_data.csv',
    customer_name VARCHAR(255),
    product_name VARCHAR(255),
    quantity INT,
    price DECIMAL(10, 2),
    sale_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY unique_transaction (transaction_id, source_file)
);

CREATE TABLE revenue_per_product (
    product_name VARCHAR(255),
    total_revenue DECIMAL(15, 2)
);

CREATE TABLE monthly_revenue (
    month INT,
    monthly_revenue DECIMAL(15, 2)
);

CREATE TABLE master_customers (
    customer_id INT PRIMARY KEY AUTO_INCREMENT,
    customer_key VARCHAR(32) UNIQUE,
    original_name VARCHAR(255),
    standardized_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    record_count INT DEFAULT 1
);

CREATE TABLE master_products (
    product_id INT PRIMARY KEY AUTO_INCREMENT,
    product_key VARCHAR(32) UNIQUE,
    original_name VARCHAR(255),
    standardized_name VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    record_count INT DEFAULT 1
);

CREATE TABLE data_quality_summary (
    summary_id INT PRIMARY KEY AUTO_INCREMENT,
    entity_type ENUM('customers', 'products'),
    total_records INT,
    unique_records INT,
    duplicate_records INT,
    standardization_applied INT,
    processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);