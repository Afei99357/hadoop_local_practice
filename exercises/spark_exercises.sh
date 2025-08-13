#!/bin/bash

echo "============================================"
echo "SPARK ON YARN EXERCISES - Advanced Analytics"
echo "============================================"
echo ""

echo "🚀 Exercise 1: Basic Spark DataFrame Operations"
echo "-----------------------------------------------"
echo "Running Spark job for sales analytics..."

sudo docker exec spark-master /spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /jobs/spark_analytics.py

echo ""
echo "📊 Exercise 2: Interactive Spark Shell"
echo "--------------------------------------"
echo "Creating an interactive analysis script..."

sudo docker exec spark-master bash -c "cat > /tmp/interactive_analysis.py << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

# Initialize Spark session
spark = SparkSession.builder \
    .appName('InteractiveAnalysis') \
    .master('spark://spark-master:7077') \
    .config('spark.sql.adaptive.enabled', 'true') \
    .getOrCreate()

# Read data from HDFS
print('Reading transaction data from HDFS...')
df = spark.read.csv('hdfs://namenode:9000/user/data/transactions.csv', 
                     header=True, inferSchema=True)

# Basic DataFrame operations
print('\\n=== Dataset Info ===')
print(f'Total records: {df.count()}')
print(f'Columns: {df.columns}')

# Show schema
print('\\n=== Schema ===')
df.printSchema()

# Sample data
print('\\n=== Sample Data ===')
df.show(5)

# Analytics
print('\\n=== Top Products by Revenue ===')
df.groupBy('product_name') \
  .agg(sum('total_amount').alias('revenue'),
       count('*').alias('transactions')) \
  .orderBy(desc('revenue')) \
  .show(10)

print('\\n=== Daily Sales Trend ===')
df.withColumn('date', to_date('transaction_date')) \
  .groupBy('date') \
  .agg(sum('total_amount').alias('daily_sales'),
       count('*').alias('transactions')) \
  .orderBy('date') \
  .show(10)

print('\\n=== Customer Segmentation ===')
customer_stats = df.groupBy('customer_id') \
    .agg(sum('total_amount').alias('total_spent'),
         count('*').alias('purchase_count'),
         avg('total_amount').alias('avg_purchase'))

customer_stats.select(
    when(col('total_spent') > 1000, 'VIP')
    .when(col('total_spent') > 500, 'Regular')
    .otherwise('Occasional').alias('segment'),
    col('customer_id')
).groupBy('segment').count().show()

spark.stop()
EOF"

echo "Running interactive analysis..."
sudo docker exec spark-master /spark/bin/spark-submit /tmp/interactive_analysis.py

echo ""
echo "⚡ Exercise 3: Spark SQL Analytics"
echo "---------------------------------"
echo "Running SQL queries with Spark..."

sudo docker exec spark-master bash -c "cat > /tmp/spark_sql.py << 'EOF'
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName('SparkSQL') \
    .master('spark://spark-master:7077') \
    .getOrCreate()

# Read and register as temp view
df = spark.read.csv('hdfs://namenode:9000/user/data/transactions.csv', 
                    header=True, inferSchema=True)
df.createOrReplaceTempView('sales')

print('\\n=== Category Performance ===')
spark.sql('''
    SELECT category,
           COUNT(*) as transactions,
           SUM(total_amount) as total_revenue,
           AVG(total_amount) as avg_transaction
    FROM sales
    GROUP BY category
    ORDER BY total_revenue DESC
''').show()

print('\\n=== Hourly Sales Pattern ===')
spark.sql('''
    SELECT HOUR(transaction_date) as hour,
           COUNT(*) as transactions,
           SUM(total_amount) as hourly_sales
    FROM sales
    GROUP BY HOUR(transaction_date)
    ORDER BY hour
''').show(24)

print('\\n=== Product Price Analysis ===')
spark.sql('''
    SELECT product_name,
           MIN(price) as min_price,
           MAX(price) as max_price,
           AVG(price) as avg_price,
           STDDEV(price) as price_stddev
    FROM sales
    GROUP BY product_name
    HAVING COUNT(*) > 5
    ORDER BY avg_price DESC
    LIMIT 10
''').show()

spark.stop()
EOF"

echo "Running Spark SQL analysis..."
sudo docker exec spark-master /spark/bin/spark-submit /tmp/spark_sql.py

echo ""
echo "🔄 Exercise 4: Spark Streaming Simulation"
echo "-----------------------------------------"
echo "Simulating real-time data processing..."

sudo docker exec spark-master bash -c "cat > /tmp/streaming_sim.py << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName('StreamingSimulation') \
    .master('spark://spark-master:7077') \
    .getOrCreate()

# Read batch data but process as if streaming
df = spark.read.csv('hdfs://namenode:9000/user/data/transactions.csv', 
                    header=True, inferSchema=True)

# Simulate streaming by processing in batches
print('Simulating streaming analytics...')
batch_size = 100
total_rows = df.count()

for i in range(0, min(500, total_rows), batch_size):
    batch_df = df.limit(i + batch_size).filter(col('transaction_id') > i)
    
    # Real-time metrics
    metrics = batch_df.agg(
        count('*').alias('transactions'),
        sum('total_amount').alias('revenue'),
        avg('total_amount').alias('avg_transaction')
    ).collect()[0]
    
    print(f'Batch {i//batch_size + 1}: {metrics.transactions} transactions, '
          f'${metrics.revenue:.2f} revenue, ${metrics.avg_transaction:.2f} avg')
    
    time.sleep(1)  # Simulate real-time delay

spark.stop()
EOF"

echo "Running streaming simulation..."
sudo docker exec spark-master /spark/bin/spark-submit /tmp/streaming_sim.py

echo ""
echo "⚡ Exercise 5: Performance Tuning"
echo "---------------------------------"
echo "Demonstrating Spark optimization techniques..."

sudo docker exec spark-master bash -c "cat > /tmp/performance_tuning.py << 'EOF'
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import time

spark = SparkSession.builder \
    .appName('PerformanceTuning') \
    .master('spark://spark-master:7077') \
    .config('spark.sql.adaptive.enabled', 'true') \
    .config('spark.sql.adaptive.coalescePartitions.enabled', 'true') \
    .config('spark.sql.adaptive.skewJoin.enabled', 'true') \
    .getOrCreate()

df = spark.read.csv('hdfs://namenode:9000/user/data/transactions.csv', 
                    header=True, inferSchema=True)

print('\\n=== Without Caching ===')
start = time.time()
result1 = df.groupBy('category').agg(sum('total_amount')).collect()
print(f'Time: {time.time() - start:.2f} seconds')

print('\\n=== With Caching ===')
df.cache()
df.count()  # Trigger cache
start = time.time()
result2 = df.groupBy('category').agg(sum('total_amount')).collect()
print(f'Time: {time.time() - start:.2f} seconds')

print('\\n=== Partition Optimization ===')
print(f'Default partitions: {df.rdd.getNumPartitions()}')
df_repartitioned = df.repartition(4, 'category')
print(f'After repartition: {df_repartitioned.rdd.getNumPartitions()}')

print('\\n=== Broadcast Join Example ===')
# Create small dimension table
categories = spark.createDataFrame([
    ('Electronics', 'High-value items'),
    ('Books', 'Educational products'),
    ('Clothing', 'Fashion items')
], ['category', 'description'])

# Broadcast join
result = df.join(broadcast(categories), 'category', 'left')
print(f'Joined records: {result.count()}')

spark.stop()
EOF"

echo "Running performance tuning demo..."
sudo docker exec spark-master /spark/bin/spark-submit /tmp/performance_tuning.py

echo ""
echo "✅ Spark on YARN Exercises Complete!"
echo ""
echo "What you learned:"
echo "- DataFrame operations and transformations"
echo "- Spark SQL for complex analytics"
echo "- Streaming data processing patterns"
echo "- Performance optimization techniques"
echo "- Caching, partitioning, and broadcast joins"