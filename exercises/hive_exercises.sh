#!/bin/bash

echo "=============================================="
echo "HIVE OPTIMIZATION EXERCISES - SQL on Hadoop"
echo "=============================================="
echo ""

echo "🏗️ Exercise 1: Basic Hive Table Creation"
echo "----------------------------------------"
echo "Creating and loading data into Hive tables..."

sudo docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "
-- Show existing databases
SHOW DATABASES;

-- Use default database
USE default;

-- Show existing tables
SHOW TABLES;
"

echo ""
echo "📊 Exercise 2: Partitioned Tables"
echo "---------------------------------"
echo "Creating partitioned table for better query performance..."

sudo docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "
-- Create partitioned table by date
CREATE TABLE IF NOT EXISTS sales_partitioned (
    transaction_id STRING,
    customer_id STRING,
    product_id STRING,
    product_name STRING,
    category STRING,
    price DOUBLE,
    quantity INT,
    total_amount DOUBLE
)
PARTITIONED BY (year INT, month INT)
STORED AS ORC;

-- Show table structure
DESCRIBE FORMATTED sales_partitioned;
"

echo ""
echo "🪣 Exercise 3: Bucketed Tables"
echo "------------------------------"
echo "Creating bucketed table for optimized joins..."

sudo docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "
-- Create bucketed table for customer data
CREATE TABLE IF NOT EXISTS customers_bucketed (
    customer_id STRING,
    total_spent DOUBLE,
    transaction_count INT
)
CLUSTERED BY (customer_id) INTO 4 BUCKETS
STORED AS ORC;

-- Show bucketing info
DESCRIBE FORMATTED customers_bucketed;
"

echo ""
echo "📈 Exercise 4: Query Performance Comparison"
echo "-------------------------------------------"
echo "Running queries to compare performance..."

sudo docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "
-- Query 1: Top categories by revenue
SELECT category, SUM(total_amount) as revenue
FROM transactions
GROUP BY category
ORDER BY revenue DESC
LIMIT 5;

-- Query 2: Monthly sales trend
SELECT 
    MONTH(transaction_date) as month,
    COUNT(*) as transactions,
    SUM(total_amount) as total_sales
FROM transactions
WHERE YEAR(transaction_date) = 2024
GROUP BY MONTH(transaction_date)
ORDER BY month;
"

echo ""
echo "🔧 Exercise 5: File Format Comparison"
echo "-------------------------------------"
echo "Comparing storage formats..."

sudo docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "
-- Create tables with different formats
CREATE TABLE IF NOT EXISTS sales_text
STORED AS TEXTFILE
AS SELECT * FROM transactions LIMIT 1000;

CREATE TABLE IF NOT EXISTS sales_orc
STORED AS ORC
AS SELECT * FROM transactions LIMIT 1000;

CREATE TABLE IF NOT EXISTS sales_parquet
STORED AS PARQUET
AS SELECT * FROM transactions LIMIT 1000;

-- Compare table sizes
ANALYZE TABLE sales_text COMPUTE STATISTICS;
ANALYZE TABLE sales_orc COMPUTE STATISTICS;
ANALYZE TABLE sales_parquet COMPUTE STATISTICS;

-- Show statistics
DESCRIBE FORMATTED sales_text;
DESCRIBE FORMATTED sales_orc;
DESCRIBE FORMATTED sales_parquet;
"

echo ""
echo "🚀 Exercise 6: Query Optimization Techniques"
echo "--------------------------------------------"
echo "Demonstrating optimization techniques..."

sudo docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -e "
-- Enable Cost-Based Optimizer
SET hive.cbo.enable=true;
SET hive.compute.query.using.stats=true;
SET hive.stats.fetch.column.stats=true;

-- Enable vectorization for better performance
SET hive.vectorized.execution.enabled=true;
SET hive.vectorized.execution.reduce.enabled=true;

-- Example optimized query
EXPLAIN
SELECT c.category, 
       COUNT(DISTINCT c.customer_id) as unique_customers,
       SUM(c.total_amount) as total_revenue
FROM transactions c
WHERE c.total_amount > 100
GROUP BY c.category
HAVING SUM(c.total_amount) > 1000
ORDER BY total_revenue DESC;
"

echo ""
echo "✅ Hive Optimization Exercises Complete!"
echo ""
echo "What you learned:"
echo "- Creating partitioned tables for faster queries"
echo "- Using bucketing for optimized joins"
echo "- Comparing file formats (Text vs ORC vs Parquet)"
echo "- Query optimization with CBO"
echo "- Vectorization for columnar processing"