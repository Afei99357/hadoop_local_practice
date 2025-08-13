-- Analytics queries for Hive tables

USE ecommerce;

-- 1. Top selling products
SELECT 
    product_name,
    SUM(quantity) as total_quantity,
    SUM(total_amount) as total_revenue,
    COUNT(*) as transaction_count
FROM transactions
GROUP BY product_name
ORDER BY total_revenue DESC
LIMIT 10;

-- 2. Category performance analysis
SELECT 
    category,
    COUNT(DISTINCT customer_id) as unique_customers,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_transaction_value,
    SUM(quantity) as total_items_sold
FROM transactions
GROUP BY category
ORDER BY total_revenue DESC;

-- 3. Monthly sales trend
SELECT 
    year,
    month,
    COUNT(*) as transaction_count,
    SUM(total_amount) as monthly_revenue,
    AVG(total_amount) as avg_transaction_value
FROM transactions_partitioned
GROUP BY year, month
ORDER BY year, month;

-- 4. City-wise sales distribution
SELECT 
    city,
    COUNT(*) as transaction_count,
    SUM(total_amount) as total_revenue,
    COUNT(DISTINCT customer_id) as unique_customers
FROM transactions
GROUP BY city
ORDER BY total_revenue DESC;

-- 5. Payment method analysis
SELECT 
    payment_method,
    COUNT(*) as transaction_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_transaction_value
FROM transactions
GROUP BY payment_method
ORDER BY transaction_count DESC;

-- 6. Customer purchase frequency
SELECT 
    customer_count,
    COUNT(*) as number_of_customers
FROM (
    SELECT 
        customer_id,
        COUNT(*) as customer_count
    FROM transactions
    GROUP BY customer_id
) customer_purchases
GROUP BY customer_count
ORDER BY customer_count;

-- 7. Discount impact analysis
SELECT 
    CASE 
        WHEN discount = 0 THEN 'No Discount'
        WHEN discount > 0 AND discount <= 10 THEN '1-10%'
        WHEN discount > 10 AND discount <= 20 THEN '11-20%'
        ELSE '>20%'
    END as discount_range,
    COUNT(*) as transaction_count,
    SUM(total_amount) as total_revenue,
    AVG(total_amount) as avg_transaction_value
FROM transactions
GROUP BY 
    CASE 
        WHEN discount = 0 THEN 'No Discount'
        WHEN discount > 0 AND discount <= 10 THEN '1-10%'
        WHEN discount > 10 AND discount <= 20 THEN '11-20%'
        ELSE '>20%'
    END
ORDER BY transaction_count DESC;

-- 8. Time-based shopping patterns
SELECT 
    HOUR(transaction_time) as hour_of_day,
    COUNT(*) as transaction_count,
    SUM(total_amount) as hourly_revenue
FROM transactions
GROUP BY HOUR(transaction_time)
ORDER BY hour_of_day;