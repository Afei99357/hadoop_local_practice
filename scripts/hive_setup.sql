-- Create database for e-commerce analytics
CREATE DATABASE IF NOT EXISTS ecommerce;

USE ecommerce;

-- Basic transactions table
DROP TABLE IF EXISTS transactions;
CREATE TABLE transactions (
    transaction_id STRING,
    customer_id STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    transaction_date DATE,
    transaction_time STRING,
    city STRING,
    payment_method STRING,
    discount DECIMAL(10,2)
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
TBLPROPERTIES ("skip.header.line.count"="1");

-- Partitioned table by year and month for better performance
DROP TABLE IF EXISTS transactions_partitioned;
CREATE TABLE transactions_partitioned (
    transaction_id STRING,
    customer_id STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    transaction_date DATE,
    transaction_time STRING,
    city STRING,
    payment_method STRING,
    discount DECIMAL(10,2)
)
PARTITIONED BY (year INT, month INT)
STORED AS PARQUET;

-- Bucketed table for optimized joins
DROP TABLE IF EXISTS transactions_bucketed;
CREATE TABLE transactions_bucketed (
    transaction_id STRING,
    customer_id STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    transaction_date DATE,
    transaction_time STRING,
    city STRING,
    payment_method STRING,
    discount DECIMAL(10,2)
)
CLUSTERED BY (customer_id) INTO 10 BUCKETS
STORED AS ORC;

-- Create external table pointing to HDFS data
DROP TABLE IF EXISTS transactions_external;
CREATE EXTERNAL TABLE transactions_external (
    transaction_id STRING,
    customer_id STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    quantity INT,
    unit_price DECIMAL(10,2),
    total_amount DECIMAL(10,2),
    transaction_date DATE,
    transaction_time STRING,
    city STRING,
    payment_method STRING,
    discount DECIMAL(10,2),
    year INT,
    month INT,
    day INT
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '/user/hive/warehouse/transactions_external'
TBLPROPERTIES ("skip.header.line.count"="1");