-- Performance Optimization Demo for Hive Tables

USE ecommerce;

-- ========================================
-- 1. PARTITIONING OPTIMIZATION
-- ========================================

-- Load data into partitioned table with dynamic partitioning
SET hive.exec.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;
SET hive.exec.max.dynamic.partitions=1000;
SET hive.exec.max.dynamic.partitions.pernode=1000;

-- Insert data into partitioned table
INSERT OVERWRITE TABLE transactions_partitioned PARTITION(year, month)
SELECT 
    transaction_id,
    customer_id,
    product_id,
    product_name,
    category,
    quantity,
    unit_price,
    total_amount,
    transaction_date,
    transaction_time,
    city,
    payment_method,
    discount,
    year,
    month
FROM transactions_external;

-- Compare query performance: Non-partitioned vs Partitioned
EXPLAIN SELECT * FROM transactions WHERE year = 2024 AND month = 6;
EXPLAIN SELECT * FROM transactions_partitioned WHERE year = 2024 AND month = 6;

-- Query partitioned table (much faster for filtered queries)
SELECT 
    category,
    SUM(total_amount) as revenue,
    COUNT(*) as transactions
FROM transactions_partitioned
WHERE year = 2024 AND month IN (6, 7, 8)
GROUP BY category;

-- Show partitions
SHOW PARTITIONS transactions_partitioned;

-- ========================================
-- 2. BUCKETING OPTIMIZATION
-- ========================================

-- Insert data into bucketed table
INSERT OVERWRITE TABLE transactions_bucketed
SELECT * FROM transactions;

-- Bucketing helps with joins and aggregations on bucketed column
-- This query will be optimized due to bucketing on customer_id
SELECT 
    customer_id,
    COUNT(*) as purchase_count,
    SUM(total_amount) as total_spent
FROM transactions_bucketed
GROUP BY customer_id
HAVING COUNT(*) > 5;

-- Optimized join due to bucketing
SELECT 
    t1.customer_id,
    COUNT(DISTINCT t1.product_id) as unique_products,
    SUM(t1.total_amount) as total_spent
FROM transactions_bucketed t1
JOIN transactions_bucketed t2 
    ON t1.customer_id = t2.customer_id
WHERE t1.transaction_date != t2.transaction_date
GROUP BY t1.customer_id;

-- ========================================
-- 3. FILE FORMAT OPTIMIZATION
-- ========================================

-- Create ORC format table for better compression and performance
DROP TABLE IF EXISTS transactions_orc;
CREATE TABLE transactions_orc
STORED AS ORC
TBLPROPERTIES ("orc.compress"="SNAPPY")
AS SELECT * FROM transactions;

-- Create Parquet format table
DROP TABLE IF EXISTS transactions_parquet;
CREATE TABLE transactions_parquet
STORED AS PARQUET
AS SELECT * FROM transactions;

-- Compare storage sizes
ANALYZE TABLE transactions COMPUTE STATISTICS;
ANALYZE TABLE transactions_orc COMPUTE STATISTICS;
ANALYZE TABLE transactions_parquet COMPUTE STATISTICS;

DESC FORMATTED transactions;
DESC FORMATTED transactions_orc;
DESC FORMATTED transactions_parquet;

-- ========================================
-- 4. QUERY OPTIMIZATION TECHNIQUES
-- ========================================

-- Enable Cost-Based Optimizer (CBO)
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;
SET hive.stats.fetch.partition.stats=true;

-- Collect statistics for better query planning
ANALYZE TABLE transactions_partitioned PARTITION(year, month) COMPUTE STATISTICS;
ANALYZE TABLE transactions_partitioned COMPUTE STATISTICS FOR COLUMNS;

-- Map-side join for small tables
SET hive.auto.convert.join=true;
SET hive.mapjoin.smalltable.filesize=25000000;

-- Create small dimension table
DROP TABLE IF EXISTS product_dim;
CREATE TABLE product_dim (
    product_id STRING,
    product_name STRING,
    category STRING,
    base_price DECIMAL(10,2)
);

-- This will use map-side join automatically
SELECT 
    t.transaction_id,
    t.customer_id,
    p.product_name,
    p.category,
    t.total_amount
FROM transactions_partitioned t
JOIN product_dim p ON t.product_id = p.product_id
WHERE t.year = 2024 AND t.month = 6;

-- ========================================
-- 5. COMPRESSION SETTINGS
-- ========================================

-- Enable intermediate compression
SET hive.exec.compress.intermediate=true;
SET hive.intermediate.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;

-- Enable output compression
SET hive.exec.compress.output=true;
SET mapreduce.output.fileoutputformat.compress=true;
SET mapreduce.output.fileoutputformat.compress.codec=org.apache.hadoop.io.compress.SnappyCodec;

-- ========================================
-- 6. VECTORIZATION
-- ========================================

-- Enable vectorized query execution
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;

-- This query will benefit from vectorization
SELECT 
    category,
    year,
    month,
    SUM(total_amount) as revenue,
    AVG(total_amount) as avg_transaction,
    COUNT(*) as transaction_count
FROM transactions_orc
WHERE year = 2024
GROUP BY category, year, month;

-- ========================================
-- 7. CACHING AND MATERIALIZED VIEWS
-- ========================================

-- Create materialized view for frequently accessed aggregations
DROP MATERIALIZED VIEW IF EXISTS monthly_sales_mv;
CREATE MATERIALIZED VIEW monthly_sales_mv
AS
SELECT 
    year,
    month,
    category,
    city,
    SUM(total_amount) as total_revenue,
    COUNT(*) as transaction_count,
    AVG(total_amount) as avg_transaction,
    COUNT(DISTINCT customer_id) as unique_customers
FROM transactions_partitioned
GROUP BY year, month, category, city;

-- Query will automatically use materialized view
SELECT * FROM monthly_sales_mv 
WHERE year = 2024 AND month = 6;

-- ========================================
-- 8. PERFORMANCE COMPARISON QUERIES
-- ========================================

-- Test 1: Full table scan
EXPLAIN SELECT COUNT(*) FROM transactions;
EXPLAIN SELECT COUNT(*) FROM transactions_partitioned;
EXPLAIN SELECT COUNT(*) FROM transactions_orc;

-- Test 2: Filtered aggregation
EXPLAIN SELECT 
    category, 
    SUM(total_amount) 
FROM transactions 
WHERE year = 2024 AND month = 6 
GROUP BY category;

EXPLAIN SELECT 
    category, 
    SUM(total_amount) 
FROM transactions_partitioned 
WHERE year = 2024 AND month = 6 
GROUP BY category;

-- Test 3: Complex join
EXPLAIN SELECT 
    t1.customer_id,
    COUNT(DISTINCT t1.product_id) as products
FROM transactions t1
JOIN transactions t2 
    ON t1.customer_id = t2.customer_id
GROUP BY t1.customer_id;

EXPLAIN SELECT 
    t1.customer_id,
    COUNT(DISTINCT t1.product_id) as products
FROM transactions_bucketed t1
JOIN transactions_bucketed t2 
    ON t1.customer_id = t2.customer_id
GROUP BY t1.customer_id;