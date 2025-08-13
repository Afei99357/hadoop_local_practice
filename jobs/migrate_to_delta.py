#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from delta import *
import sys

def migrate_hive_to_delta():
    """Migrate Hive tables to Delta Lake format for Databricks compatibility"""
    
    builder = SparkSession.builder \
        .appName("HiveToDeltaMigration") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.warehouse.dir", "hdfs://namenode:9000/user/hive/warehouse") \
        .enableHiveSupport()
    
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    
    spark.sparkContext.setLogLevel("INFO")
    
    print("\n" + "="*60)
    print("HIVE TO DELTA LAKE MIGRATION")
    print("="*60)
    
    print("\n1. Reading from Hive table...")
    hive_df = spark.sql("SELECT * FROM ecommerce.transactions")
    
    record_count = hive_df.count()
    print(f"   Records in Hive table: {record_count}")
    
    print("\n2. Creating Delta table from Hive data...")
    delta_path = "hdfs://namenode:9000/user/delta/transactions"
    
    hive_df.write \
        .mode("overwrite") \
        .format("delta") \
        .option("overwriteSchema", "true") \
        .save(delta_path)
    
    print(f"   Delta table created at: {delta_path}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta_transactions
        USING DELTA
        LOCATION '{delta_path}'
    """)
    
    print("\n3. Creating partitioned Delta table...")
    partitioned_delta_path = "hdfs://namenode:9000/user/delta/transactions_partitioned"
    
    hive_df.write \
        .mode("overwrite") \
        .format("delta") \
        .partitionBy("year", "month") \
        .option("overwriteSchema", "true") \
        .save(partitioned_delta_path)
    
    print(f"   Partitioned Delta table created at: {partitioned_delta_path}")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS delta_transactions_partitioned
        USING DELTA
        LOCATION '{partitioned_delta_path}'
    """)
    
    print("\n4. Optimizing Delta table...")
    spark.sql("OPTIMIZE delta_transactions")
    
    print("\n5. Creating Delta table with Z-ORDER for optimized queries...")
    spark.sql("""
        OPTIMIZE delta_transactions 
        ZORDER BY (customer_id, transaction_date)
    """)
    
    print("\n6. Delta table features demonstration:")
    
    print("   a. Time Travel - Show table history:")
    history_df = spark.sql("DESCRIBE HISTORY delta_transactions")
    history_df.select("version", "timestamp", "operation", "operationMetrics").show(5, False)
    
    print("   b. Schema Evolution - Adding new column:")
    updated_df = spark.read.format("delta").load(delta_path) \
        .withColumn("processing_timestamp", current_timestamp()) \
        .withColumn("data_source", lit("HIVE_MIGRATION"))
    
    updated_df.write \
        .mode("overwrite") \
        .format("delta") \
        .option("mergeSchema", "true") \
        .save(delta_path)
    
    print("   c. ACID Transactions - Performing MERGE operation:")
    
    spark.sql("""
        CREATE OR REPLACE TEMPORARY VIEW updates AS
        SELECT 
            transaction_id,
            customer_id,
            total_amount * 1.1 as total_amount,
            'UPDATED' as update_flag
        FROM delta_transactions
        LIMIT 100
    """)
    
    spark.sql(f"""
        MERGE INTO delta_transactions target
        USING updates source
        ON target.transaction_id = source.transaction_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.total_amount = source.total_amount,
                target.data_source = source.update_flag
    """)
    
    print("\n7. Validation - Comparing Hive and Delta tables:")
    
    hive_count = spark.sql("SELECT COUNT(*) as count FROM ecommerce.transactions").collect()[0]['count']
    delta_count = spark.sql("SELECT COUNT(*) as count FROM delta_transactions").collect()[0]['count']
    
    print(f"   Hive table records: {hive_count}")
    print(f"   Delta table records: {delta_count}")
    print(f"   Match: {'✓' if hive_count == delta_count else '✗'}")
    
    print("\n8. Performance comparison queries:")
    
    print("   Running aggregation on Hive table...")
    hive_start = spark.sql("""
        SELECT 
            category,
            COUNT(*) as cnt,
            SUM(total_amount) as total
        FROM ecommerce.transactions
        GROUP BY category
    """)
    hive_start.collect()
    
    print("   Running same aggregation on Delta table...")
    delta_start = spark.sql("""
        SELECT 
            category,
            COUNT(*) as cnt,
            SUM(total_amount) as total
        FROM delta_transactions
        GROUP BY category
    """)
    delta_start.collect()
    
    print("\n9. Creating Databricks-compatible metadata...")
    
    metadata = {
        "source_system": "Hadoop/Hive",
        "migration_date": str(current_timestamp()),
        "record_count": record_count,
        "delta_version": "1.0",
        "partitions": ["year", "month"],
        "z_order_columns": ["customer_id", "transaction_date"],
        "databricks_compatible": True
    }
    
    metadata_df = spark.createDataFrame([metadata])
    metadata_df.write \
        .mode("overwrite") \
        .format("json") \
        .save("hdfs://namenode:9000/user/delta/metadata")
    
    print("\n10. Generating migration report...")
    
    report = f"""
    MIGRATION SUMMARY
    =================
    Source: Hive table (ecommerce.transactions)
    Target: Delta Lake tables
    
    Tables Created:
    1. delta_transactions - Full table in Delta format
    2. delta_transactions_partitioned - Partitioned by year/month
    
    Delta Features Applied:
    - OPTIMIZE with Z-ORDER indexing
    - Schema evolution enabled
    - Time travel capability
    - ACID transaction support
    
    Files Location:
    - Main: {delta_path}
    - Partitioned: {partitioned_delta_path}
    
    Next Steps for Databricks:
    1. Copy Delta files to Databricks storage (DBFS/S3/Azure)
    2. Register tables in Databricks metastore
    3. Update applications to use Delta tables
    4. Set up incremental refresh pipelines
    """
    
    print(report)
    
    with open("/data/migration_report.txt", "w") as f:
        f.write(report)
    
    print("\nMigration completed successfully!")
    
    spark.stop()

def create_sample_delta_notebook():
    """Create a sample Databricks notebook for working with migrated Delta tables"""
    
    notebook_content = """
# Databricks Notebook - Working with Migrated Delta Tables

## Cell 1: Setup
```python
from pyspark.sql.functions import *
from delta.tables import *

# Set up Delta Lake
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
```

## Cell 2: Register Delta Tables
```python
# Register the migrated Delta tables
spark.sql('''
    CREATE TABLE IF NOT EXISTS transactions_delta
    USING DELTA
    LOCATION 's3://your-bucket/delta/transactions'
''')

spark.sql('''
    CREATE TABLE IF NOT EXISTS transactions_partitioned_delta
    USING DELTA
    LOCATION 's3://your-bucket/delta/transactions_partitioned'
''')
```

## Cell 3: Query Delta Tables
```python
# Basic query
df = spark.sql('''
    SELECT 
        category,
        COUNT(*) as transaction_count,
        SUM(total_amount) as total_revenue
    FROM transactions_delta
    WHERE year = 2024
    GROUP BY category
    ORDER BY total_revenue DESC
''')

display(df)
```

## Cell 4: Time Travel Query
```python
# Query historical version of the table
df_history = spark.read \
    .format("delta") \
    .option("versionAsOf", 0) \
    .load("s3://your-bucket/delta/transactions")

display(df_history.limit(100))
```

## Cell 5: Incremental Processing
```python
# Set up streaming from Delta table
stream_df = spark.readStream \
    .format("delta") \
    .load("s3://your-bucket/delta/transactions")

# Process stream
processed = stream_df \
    .groupBy("category", "city") \
    .agg(sum("total_amount").alias("revenue"))

# Write to another Delta table
query = processed.writeStream \
    .format("delta") \
    .outputMode("complete") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start("s3://your-bucket/delta/aggregated")
```

## Cell 6: Delta Lake Maintenance
```python
# Optimize table
spark.sql("OPTIMIZE transactions_delta")

# Vacuum old files
spark.sql("VACUUM transactions_delta RETAIN 168 HOURS")

# Show table history
display(spark.sql("DESCRIBE HISTORY transactions_delta"))
```
"""
    
    with open("/data/databricks_notebook.py", "w") as f:
        f.write(notebook_content)
    
    print("Sample Databricks notebook created at: /data/databricks_notebook.py")

if __name__ == "__main__":
    try:
        migrate_hive_to_delta()
        create_sample_delta_notebook()
    except Exception as e:
        print(f"Error during migration: {str(e)}")
        print("\nNote: Delta Lake requires additional JARs to be available in Spark.")
        print("For production use, ensure delta-core and delta-storage JARs are in classpath.")
        sys.exit(1)