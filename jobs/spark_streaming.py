#!/usr/bin/env python3

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

def main():
    """Spark Structured Streaming job for real-time analytics"""
    
    spark = SparkSession.builder \
        .appName("RealTimeEcommerceAnalytics") \
        .master("yarn") \
        .config("spark.submit.deployMode", "client") \
        .config("spark.executor.memory", "2g") \
        .config("spark.executor.cores", "2") \
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
        StructField("discount", DoubleType(), True)
    ])
    
    input_path = "hdfs://namenode:9000/user/streaming/transactions"
    
    streaming_df = spark.readStream \
        .option("header", "true") \
        .schema(schema) \
        .csv(input_path)
    
    windowed_sales = streaming_df \
        .withColumn("timestamp", 
            to_timestamp(concat(col("transaction_date"), lit(" "), col("transaction_time")))
        ) \
        .withWatermark("timestamp", "10 minutes") \
        .groupBy(
            window(col("timestamp"), "5 minutes", "1 minute"),
            "category"
        ) \
        .agg(
            sum("total_amount").alias("total_revenue"),
            count("transaction_id").alias("transaction_count"),
            avg("total_amount").alias("avg_transaction")
        )
    
    running_totals = streaming_df \
        .groupBy("category") \
        .agg(
            sum("total_amount").alias("cumulative_revenue"),
            count("transaction_id").alias("total_transactions")
        )
    
    query1 = windowed_sales.writeStream \
        .outputMode("update") \
        .format("console") \
        .option("truncate", False) \
        .trigger(processingTime="30 seconds") \
        .start()
    
    checkpoint_path = "hdfs://namenode:9000/user/checkpoints/streaming"
    output_path = "hdfs://namenode:9000/user/output/streaming_results"
    
    query2 = running_totals.writeStream \
        .outputMode("complete") \
        .format("parquet") \
        .option("path", output_path) \
        .option("checkpointLocation", checkpoint_path) \
        .trigger(processingTime="1 minute") \
        .start()
    
    print("Streaming job started. Monitoring for new transactions...")
    print("Press Ctrl+C to stop")
    
    try:
        query1.awaitTermination()
        query2.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping streaming job...")
        query1.stop()
        query2.stop()
        spark.stop()

if __name__ == "__main__":
    main()