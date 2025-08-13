# Hadoop Practice Project - E-Commerce Analytics

A comprehensive hands-on project to learn and practice Hadoop ecosystem technologies including HDFS, YARN, MapReduce, Hive, and Spark.

## Project Overview

This project simulates a real-world e-commerce analytics pipeline using the Hadoop ecosystem. It covers:

- **HDFS**: Distributed file storage and data movement
- **MapReduce**: Basic analytics jobs for category sales analysis
- **YARN**: Resource management for distributed computing
- **Hive**: SQL-based data warehousing with optimization techniques
- **Spark**: Advanced analytics and streaming processing
- **Delta Lake**: Modern data lake format migration

## Architecture

```
.
├── docker-compose.yml          # Complete Hadoop cluster setup
├── data/                       # Sample datasets
├── jobs/                       # Spark jobs
│   ├── spark_analytics.py      # Batch analytics
│   ├── spark_streaming.py      # Real-time processing
│   └── migrate_to_delta.py     # Delta Lake migration
├── mapreduce/                  # MapReduce jobs
│   ├── mapper.py
│   └── reducer.py
└── scripts/                    # Utility scripts
    ├── generate_data.py        # Data generator
    ├── hive_setup.sql          # Hive DDL
    ├── optimization_demo.sql   # Performance demos
    └── hdfs_commands.sh        # HDFS practice commands
```

## Quick Start

### 🚀 One-Command Setup (Recommended)

```bash
# Complete fresh start with everything configured
chmod +x fresh_start.sh && ./fresh_start.sh
```

This will automatically:
- Install docker-compose to /usr/local/bin/
- Clean up any existing containers
- Start the complete Hadoop cluster
- Install Python3 in containers (with fallback to shell)
- Generate sample data and upload to HDFS
- Run MapReduce analytics (Python or shell-based)

### 🔧 Manual Commands (if needed)

After running `fresh_start.sh`, you can use these commands:

```bash
# Interactive shell access
sudo docker exec -it namenode bash
```

## ⚠️ Python Version Issue

If you encounter syntax errors with f-strings when running the MapReduce Python jobs:

```
SyntaxError: invalid syntax
```

This is because the default Debian Stretch containers use Python 3.5, which doesn't support f-strings (introduced in Python 3.6).

### Solution: Upgrade Python in the Container

Run these commands inside the namenode container to install Python 3.9:

```bash
# Enter the namenode container
sudo docker exec -it namenode bash

# Install Miniconda and Python 3.9
apt-get update && apt-get install -y wget bzip2
wget https://repo.anaconda.com/miniconda/Miniconda3-latest-Linux-x86_64.sh -O miniconda.sh
bash miniconda.sh -b -p /opt/conda
/opt/conda/bin/conda install -y python=3.9
ln -sf /opt/conda/bin/python /usr/local/bin/python3

# Verify the upgrade
python3 --version  # Should show Python 3.9.x
```

After this upgrade, the MapReduce Python jobs should work correctly with f-string syntax.


# Continue work

```bash
# Run MapReduce analytics (Python version if available)
sudo docker exec namenode bash -c "hdfs dfs -cat /user/data/transactions.csv | python3 /mapreduce/mapper.py | sort | python3 /mapreduce/reducer.py"
```

```bash
# Or shell-based alternative
sudo docker exec namenode bash -c 'hdfs dfs -cat /user/data/transactions.csv | tail -n +2 | cut -d"," -f5,8 | sort | awk -F"," "{sales[\$1] += \$2; count[\$1]++} END {for (cat in sales) print cat \": \$\" sales[cat] \" (\" count[cat] \" transactions)\"}"'
```

```bash
# Run Spark on YARN (requires yarn-site.xml setup first)
# First, create yarn-site.xml in spark-master container:
sudo docker exec -it spark-master bash -lc 'mkdir -p /etc/hadoop/conf && cat > /etc/hadoop/conf/yarn-site.xml << "EOF"
<?xml version="1.0"?>
<configuration>
  <property><name>yarn.resourcemanager.hostname</name><value>resourcemanager</value></property>
  <property><name>yarn.resourcemanager.address</name><value>resourcemanager:8032</value></property>
  <property><name>yarn.resourcemanager.scheduler.address</name><value>resourcemanager:8030</value></property>
  <property><name>yarn.resourcemanager.resource-tracker.address</name><value>resourcemanager:8031</value></property>
  <property><name>yarn.resourcemanager.webapp.address</name><value>resourcemanager:8088</value></property>
</configuration>
EOF'

# Then run Spark on YARN:
sudo docker exec \
  -e PYSPARK_PYTHON=/usr/bin/python3 \
  -e PYSPARK_DRIVER_PYTHON=/usr/bin/python3 \
  -e HADOOP_CONF_DIR=/etc/hadoop/conf \
  spark-master /spark/bin/spark-submit \
  --master yarn --deploy-mode client \
  --conf spark.hadoop.fs.defaultFS=hdfs://namenode:9000 \
  --conf spark.yarn.stagingDir=hdfs:///user/root/.sparkStaging \
  /jobs/spark_analytics.py

# Setup Hive tables
sudo docker exec hive-server beeline -u jdbc:hive2://localhost:10000 -f /scripts/hive_setup.sql

# Stop everything
sudo docker-compose down
claude
# Complete cleanup
sudo docker-compose down -v
```

## 📚 Interactive Learning Exercises

This project includes comprehensive hands-on exercises to master each Hadoop ecosystem component. All exercises are located in the `exercises/` directory.

### 🚀 Quick Start - Run All Exercises

```bash
cd exercises/
chmod +x run_all_exercises.sh
sudo ./run_all_exercises.sh
```

This interactive script guides you through all modules with pauses for review and learning summaries.

### 📖 Exercise Modules

#### 1️⃣ HDFS Operations (`hdfs_exercises.sh`)
**Duration:** ~10 minutes

Learn essential HDFS commands and operations:
- Upload/download files to/from HDFS
- Create and manage directory structures
- Set and verify replication factors
- Copy and move files within HDFS
- Use wildcards and check statistics
- Monitor HDFS health and capacity

```bash
sudo ./exercises/hdfs_exercises.sh
```

#### 2️⃣ MapReduce Programming (`mapreduce_exercises.sh`)
**Duration:** ~15 minutes

Master the mapper/reducer pattern with practical examples:
- Category sales aggregation
- Product quantity analysis
- Top spending customers identification
- Sales by hour of day analytics
- Custom MapReduce job creation

```bash
sudo ./exercises/mapreduce_exercises.sh
```

#### 3️⃣ Hive SQL & Optimization (`hive_exercises.sh`)
**Duration:** ~15 minutes

Optimize big data queries with Hive:
- Create and query Hive tables
- Implement partitioning for performance
- Use bucketing for optimized joins
- Compare file formats (TextFile vs ORC vs Parquet)
- Apply Cost-Based Optimizer (CBO)
- Enable vectorization for faster processing

```bash
sudo ./exercises/hive_exercises.sh
```

#### 4️⃣ Spark Analytics (`spark_exercises.sh`)
**Duration:** ~20 minutes

Advanced analytics with Apache Spark:
- DataFrame operations and transformations
- Spark SQL analytics
- Customer segmentation (VIP/Regular/Occasional)
- Real-time streaming simulation
- Performance optimization with caching
- Broadcast joins for large datasets

```bash
sudo ./exercises/spark_exercises.sh
```

### 💡 Learning Tips

**For Beginners:**
- Start with `run_all_exercises.sh` for guided learning
- Read output carefully - each command is explained
- Take notes on interesting commands
- Re-run individual exercises to reinforce concepts

**For Advanced Users:**
- Modify scripts to analyze different aspects
- Experiment with optimization parameters
- Create custom MapReduce jobs
- Load your own datasets for analysis

### 🎯 Learning Objectives

After completing all exercises, you will be able to:
- ✅ **HDFS:** Manage distributed file storage effectively
- ✅ **MapReduce:** Write and run data processing jobs
- ✅ **Hive:** Optimize SQL queries on big data
- ✅ **Spark:** Perform advanced analytics and tuning
- ✅ **Integration:** Understand how components work together

## Key Learning Areas

### 1. HDFS Operations
- File upload/download
- Directory management
- Data replication
- DistCP for large transfers

### 2. MapReduce Programming
- Mapper/Reducer pattern
- Category sales aggregation
- Word count implementation

### 3. Hive Optimization
- Partitioning by date
- Bucketing for joins
- File format comparison (ORC, Parquet)
- Query optimization with CBO

### 4. Spark on YARN
- DataFrame operations
- SQL analytics
- Streaming processing
- Performance tuning

### 5. Data Migration
- Hive to Delta Lake
- Schema evolution
- ACID transactions
- Time travel queries

## Web Interfaces

- **HDFS NameNode**: http://localhost:9870
- **YARN ResourceManager**: http://localhost:8088
- **Spark Master**: http://localhost:8080
- **MapReduce History**: http://localhost:8188

## Performance Optimizations Demonstrated

1. **Partitioning**: Organize data by year/month for faster queries
2. **Bucketing**: Optimize joins on customer_id
3. **File Formats**: Compare TextFile vs ORC vs Parquet
4. **Compression**: Snappy compression for better I/O
5. **Caching**: Materialized views for frequent queries
6. **Vectorization**: Columnar processing in Hive

## Sample Queries

```sql
-- Top selling products
SELECT product_name, SUM(total_amount) as revenue
FROM transactions
GROUP BY product_name
ORDER BY revenue DESC
LIMIT 10;
```

```sql
-- Monthly trends
SELECT year, month, SUM(total_amount) as monthly_revenue
FROM transactions_partitioned
WHERE year = 2024
GROUP BY year, month;
```

## Cleanup

```bash
# Stop all services
docker-compose down

# Remove all data volumes
docker-compose down -v
```

## Skills Practiced

- Distributed computing concepts
- Big data file formats and compression
- SQL-on-Hadoop with Hive
- Spark DataFrame and SQL APIs
- Performance tuning techniques
- Data lake architecture with Delta
- Container orchestration with Docker

## Next Steps

1. Scale up with more data nodes
2. Implement Kafka streaming pipeline
3. Add machine learning with MLlib
4. Integrate with cloud storage (S3/Azure)
5. Build a real-time dashboard
6. Implement data quality checks
