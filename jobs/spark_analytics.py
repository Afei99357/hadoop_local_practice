#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import sys

def main():
    """PySpark job for e-commerce analytics on YARN"""
    
    spark = SparkSession.builder \
        .appName("EcommerceAnalytics") \
        .master("yarn") \
        .config("spark.submit.deployMode", "client") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.num.executors", "2") \
        .enableHiveSupport() \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    schema = StructType([
        StructField("transaction_id", StringType(), True),
        StructField("customer_id", StringType(), True),
        StructField("product_id", StringType(), True),
        StructField("product_name", StringType(), True),
        StructField("category", StringType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("transaction_date", StringType(), True),
        StructField("transaction_time", StringType(), True),
        StructField("city", StringType(), True),
        StructField("payment_method", StringType(), True),
        StructField("discount", DoubleType(), True),
        StructField("year", IntegerType(), True),
        StructField("month", IntegerType(), True),
        StructField("day", IntegerType(), True)
    ])
    
    input_path = "hdfs://namenode:9000/user/data/transactions.csv"
    
    df = spark.read \
        .option("header", "true") \
        .schema(schema) \
        .csv(input_path)
    
    df.createOrReplaceTempView("transactions")
    
    print("\n" + "="*60)
    print("SPARK ANALYTICS ON YARN - E-COMMERCE TRANSACTIONS")
    print("="*60)
    
    print("\n1. Dataset Overview:")
    print(f"Total Transactions: {df.count()}")
    print(f"Unique Customers: {df.select('customer_id').distinct().count()}")
    print(f"Unique Products: {df.select('product_id').distinct().count()}")
    
    print("\n2. Top Revenue Generating Categories:")
    category_revenue = df.groupBy("category") \
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("transaction_id").alias("transaction_count"),
            avg("total_amount").alias("avg_transaction_value")
        ) \
        .orderBy(desc("total_revenue"))
    
    category_revenue.show(10)
    
    print("\n3. Customer Segmentation by Purchase Value:")
    customer_segments = df.groupBy("customer_id") \
        .agg(
            sum("total_amount").alias("customer_lifetime_value"),
            count("transaction_id").alias("purchase_count"),
            avg("total_amount").alias("avg_purchase_value")
        ) \
        .withColumn("segment", 
            when(col("customer_lifetime_value") > 1000, "High Value")
            .when(col("customer_lifetime_value") > 500, "Medium Value")
            .otherwise("Low Value")
        )
    
    segment_summary = customer_segments.groupBy("segment") \
        .agg(
            count("customer_id").alias("customer_count"),
            avg("customer_lifetime_value").alias("avg_lifetime_value")
        ) \
        .orderBy("segment")
    
    segment_summary.show()
    
    print("\n4. Monthly Sales Trend:")
    monthly_trend = df.groupBy("year", "month") \
        .agg(
            sum("total_amount").alias("monthly_revenue"),
            count("transaction_id").alias("transaction_count")
        ) \
        .orderBy("year", "month")
    
    monthly_trend.show(24)
    
    print("\n5. Product Performance Analysis:")
    product_performance = df.groupBy("product_name", "category") \
        .agg(
            sum("quantity").alias("units_sold"),
            sum("total_amount").alias("total_revenue"),
            avg("discount").alias("avg_discount"),
            count("transaction_id").alias("transaction_count")
        ) \
        .orderBy(desc("total_revenue")) \
        .limit(15)
    
    product_performance.show(truncate=False)
    
    print("\n6. City-wise Sales Distribution:")
    city_sales = df.groupBy("city") \
        .agg(
            sum("total_amount").alias("total_revenue"),
            countDistinct("customer_id").alias("unique_customers"),
            avg("total_amount").alias("avg_transaction_value")
        ) \
        .orderBy(desc("total_revenue"))
    
    city_sales.show()
    
    print("\n7. Payment Method Preferences:")
    payment_analysis = df.groupBy("payment_method") \
        .agg(
            count("transaction_id").alias("usage_count"),
            sum("total_amount").alias("total_revenue"),
            avg("total_amount").alias("avg_transaction")
        ) \
        .withColumn("usage_percentage", 
            col("usage_count") * 100.0 / df.count()
        ) \
        .orderBy(desc("usage_count"))
    
    payment_analysis.show()
    
    output_path = "hdfs://namenode:9000/user/output/spark_results"
    
    print(f"\n8. Saving results to HDFS: {output_path}")
    category_revenue.coalesce(1).write \
        .mode("overwrite") \
        .parquet(f"{output_path}/category_revenue")
    
    customer_segments.coalesce(1).write \
        .mode("overwrite") \
        .parquet(f"{output_path}/customer_segments")
    
    monthly_trend.coalesce(1).write \
        .mode("overwrite") \
        .parquet(f"{output_path}/monthly_trend")
    
    print("\nSpark job completed successfully!")
    
    spark.stop()

if __name__ == "__main__":
    main()