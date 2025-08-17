# NiFi Data Pipeline Practice

This module provides hands-on practice with Apache NiFi for data ingestion into HDFS, preparing you for migration to Databricks workflows.

## 🎯 Learning Objectives

1. **NiFi Fundamentals**: Build data pipelines using processors and connections
2. **HDFS Integration**: Ingest various data formats into Hadoop
3. **Data Transformation**: Parse, filter, and enrich data in-flight
4. **Error Handling**: Implement retry logic and dead letter queues
5. **Databricks Migration**: Learn patterns for converting NiFi flows to Databricks

## 🚀 Quick Start

### 1. Start NiFi Services

```bash
# From the hadoop_practice directory
# First ensure Hadoop is running
docker-compose up -d

# Then start NiFi
cd nifi
docker-compose up -d

# Verify NiFi is running
docker ps | grep nifi
```

### 2. Access NiFi UI

Open browser to: http://localhost:8090/nifi/
- Username: admin
- Password: nifiadminpassword123

**Note:** The trailing slash (/) in the URL is important!

### 3. Access NiFi Registry

Open browser to: http://localhost:18080/nifi-registry

## 📚 Pipeline Exercises

### Exercise 1: Basic File Ingestion to HDFS - Detailed Steps

**Goal**: Ingest CSV sensor data into HDFS

#### Step 1: Create GetFile Processor
1. On the NiFi canvas, drag the **Processor icon** from the top toolbar
2. In "Add Processor" dialog:
   - Search for: `GetFile`
   - Select `GetFile` and click "ADD"
3. Configure GetFile processor (double-click to open):
   - Go to **PROPERTIES** tab
   - Set these values:
     ```
     Input Directory: /opt/nifi/nifi-current/data
     File Filter: .*\.csv
     Keep Source File: true
     Polling Interval: 10 sec
     ```
   - Click "APPLY"

#### Step 2: Create PutHDFS Processor
1. Drag another **Processor** onto the canvas
2. Search for: `PutHDFS`, select and "ADD"
3. Configure PutHDFS processor:
   - Go to **PROPERTIES** tab
   - Set these values:
     ```
     Hadoop Configuration Files: /opt/nifi/nifi-current/conf/core-site.xml
     Directory: /user/nifi/sensor_data
     Conflict Resolution Strategy: replace
     ```
   - Click "APPLY"

#### Step 3: Configure HDFS Connection
The required `core-site.xml` file has been created automatically in the container with:
```xml
<?xml version="1.0"?>
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode:9000</value>
    </property>
</configuration>
```

#### Step 4: Connect Processors
1. Hover over the GetFile processor
2. Drag from the arrow icon to the PutHDFS processor
3. In "Create Connection" dialog:
   - Ensure "success" relationship is checked
   - Click "ADD"

#### Step 5: Configure Error Handling
1. Right-click on PutHDFS processor → "Configure"
2. Go to **RELATIONSHIPS** tab
3. Configure both relationships:
   - **failure**: Select **Terminate** (to discard failed files)
   - **success**: Select **Terminate** (to discard successfully processed files)
4. Click "APPLY"

**Note**: This prevents the processor from stopping when files complete processing. The `success` relationship outputs the file after it's written to HDFS - since we don't need to do anything else with it, we terminate it.

#### Step 6: Start the Flow
1. Press `Ctrl+A` to select all processors
2. Right-click and select "Start" (or click ▶️ button)
3. Watch for data flow numbers on the connection

#### Step 7: Monitor and Verify

**In NiFi UI:**
- Check for error bulletins (⚠️) on processors
- Numbers should appear on connections showing data flow
- Right-click processors → "View statistics" for processing details
- Right-click connections → "List queue" → "View Details" for data provenance

**Verify Results in HDFS:**
```bash
# Check if file was created
docker exec namenode hdfs dfs -ls /user/nifi/sensor_data/

# View file contents in HDFS
docker exec namenode hdfs dfs -cat /user/nifi/sensor_data/sample_sensor_data.csv | head -5

# Compare with source file
docker exec nifi cat /opt/nifi/nifi-current/data/sample_sensor_data.csv | head -5
```

**Access Containers for Detailed Inspection:**
```bash
# Enter NiFi container
docker exec -it nifi bash
ls -la /opt/nifi/nifi-current/data/
tail -f /opt/nifi/nifi-current/logs/nifi-app.log

# Enter Hadoop namenode container  
docker exec -it namenode bash
hdfs dfs -ls /user/nifi/sensor_data/
hdfs dfs -cat /user/nifi/sensor_data/sample_sensor_data.csv
```

**Expected Success Indicators:**
- ✅ File appears in HDFS: `/user/nifi/sensor_data/sample_sensor_data.csv`
- ✅ File size: 1,560 bytes
- ✅ CSV structure and data integrity preserved
- ✅ No error bulletins in NiFi UI

**Sample data location**: `/opt/nifi/nifi-current/data/sample_sensor_data.csv`

#### Step 8: Export Your Flow as Template

Once your pipeline is working successfully, you can export it for reuse:

**Create Template:**
1. Press `Ctrl+A` to select all processors
2. Right-click on selected processors → **"Create template"**
3. Give it a name: `CSV_to_HDFS_Pipeline`
4. Add description: `Basic file ingestion from local directory to HDFS`
5. Click **"Create"**

**Download Template:**
1. Click the **hamburger menu** (☰) in the top-right corner
2. Select **"Templates"**
3. Find your template and click the **download icon** (⬇️)
4. This downloads an XML file you can import to other NiFi instances

**Template Benefits:**
- Reusable across different NiFi environments
- Shareable with team members
- Version control friendly
- Captures all processor configurations and connections

**Alternative:** A sample template has been created at `nifi/flows/csv_to_hdfs_template.xml` for reference.

### Exercise 2: JSON Log Processing Pipeline

**Goal**: Parse JSON logs, filter by level, and store in HDFS

1. **ListenHTTP Processor** (receive streaming data)
   - Port: 8091
   - Base Path: /ingest

2. **EvaluateJsonPath Processor**
   - Extract: level, service, timestamp

3. **RouteOnAttribute Processor**
   - Route ERROR and CRITICAL logs separately

4. **PutHDFS Processor**
   - Different directories by log level

### Exercise 3: Real-time Stream Processing

**Goal**: Process streaming sensor data with transformations

```bash
# Generate streaming data
docker exec -it nifi python3 /opt/nifi/scripts/generate_streaming_data.py --type sensor
```

Pipeline Components:
- GenerateFlowFile → Simulate data source
- UpdateAttribute → Add metadata
- JoltTransformJSON → Transform structure
- ConvertRecord → CSV to Parquet
- PutHDFS → Store in HDFS

### Exercise 4: Data Quality & Error Handling

**Goal**: Implement validation and error handling

1. **ValidateRecord Processor**
   - Schema validation for incoming data
   - Route invalid records to error queue

2. **RetryFlowFile Processor**
   - Configure retry attempts
   - Exponential backoff

3. **PutEmail Processor**
   - Alert on critical failures

## 🔄 NiFi to Databricks Migration Patterns

### Pattern 1: File Ingestion
```python
# NiFi: GetFile → PutHDFS
# Databricks Equivalent:

# Auto Loader for continuous file ingestion
df = spark.readStream \
    .format("cloudFiles") \
    .option("cloudFiles.format", "csv") \
    .option("cloudFiles.schemaLocation", "/tmp/schema") \
    .load("/mnt/data/sensor_data")

df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .start("/delta/sensor_data")
```

### Pattern 2: HTTP/API Ingestion
```python
# NiFi: ListenHTTP → ProcessJSON → PutHDFS
# Databricks Equivalent:

# Using Databricks Jobs with REST API
import requests
import json
from pyspark.sql import SparkSession

def ingest_api_data():
    response = requests.get("https://api.example.com/data")
    data = response.json()
    
    df = spark.createDataFrame(data)
    df.write.mode("append").format("delta").save("/delta/api_data")

# Schedule as Databricks Job
```

### Pattern 3: Stream Processing
```python
# NiFi: ConsumeKafka → TransformJSON → PutHDFS
# Databricks Equivalent:

# Structured Streaming with Kafka
df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "broker:9092") \
    .option("subscribe", "sensor_topic") \
    .load()

# Parse and transform
parsed_df = df.select(
    col("key").cast("string"),
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# Write to Delta
parsed_df.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/tmp/checkpoint") \
    .trigger(processingTime='10 seconds') \
    .start("/delta/sensor_stream")
```

### Pattern 4: Data Validation & Quality
```python
# NiFi: ValidateRecord → RouteOnAttribute
# Databricks Equivalent:

from pyspark.sql.functions import col, when
from delta.tables import DeltaTable

# Define quality rules
def validate_data(df):
    return df.withColumn(
        "is_valid",
        when(
            (col("temperature").between(-50, 50)) &
            (col("humidity").between(0, 100)) &
            (col("sensor_id").isNotNull()),
            True
        ).otherwise(False)
    )

# Apply validation
validated_df = validate_data(input_df)

# Route based on validation
valid_df = validated_df.filter(col("is_valid") == True)
invalid_df = validated_df.filter(col("is_valid") == False)

# Write to different locations
valid_df.write.format("delta").mode("append").save("/delta/valid_data")
invalid_df.write.format("delta").mode("append").save("/delta/quarantine")
```

## 📊 Sample NiFi Flows

### Flow 1: Batch File Processing
```
GetFile → UpdateAttribute → ConvertRecord (CSV→Parquet) → PutHDFS
```

### Flow 2: Streaming Data Pipeline
```
ListenHTTP → SplitJson → EvaluateJsonPath → RouteOnAttribute → PutHDFS
                                                ↓
                                          PutDatabaseRecord
```

### Flow 3: CDC Pipeline
```
CaptureChangeMySQL → ConvertRecord → MergeContent → PutHDFS
                           ↓
                     PublishKafka
```

## 🛠️ Common NiFi Processors for Data Engineering

| Processor | Use Case | Databricks Equivalent |
|-----------|----------|----------------------|
| GetFile | Read local files | spark.read.format() |
| ListenHTTP | REST API endpoint | REST API + Jobs |
| ConsumeKafka | Stream ingestion | spark.readStream.format("kafka") |
| PutHDFS | Write to HDFS | df.write.format("delta") |
| ConvertRecord | Format conversion | DataFrame transformations |
| JoltTransformJSON | JSON transformation | DataFrame select/withColumn |
| ExecuteSQL | Database queries | spark.read.jdbc() |
| RouteOnAttribute | Conditional routing | DataFrame filter() |
| MergeContent | Batch small files | coalesce() / repartition() |

## 🔧 Troubleshooting

### NiFi Connection Issues
```bash
# Check if NiFi is running
docker logs nifi

# Verify network connectivity
docker exec nifi ping namenode

# Check HDFS availability
docker exec namenode hdfs dfs -ls /
```

### HDFS Permission Issues
```bash
# Create NiFi user directory in HDFS
docker exec namenode hdfs dfs -mkdir -p /user/nifi
docker exec namenode hdfs dfs -chown nifi:nifi /user/nifi
```

### Memory Issues
```yaml
# Increase NiFi memory in docker-compose.nifi.yml
environment:
  - NIFI_JVM_HEAP_MAX=4g
```

## 📈 Monitoring & Metrics

### NiFi Metrics to Monitor
- Queue sizes and backpressure
- Processor execution time
- Data provenance
- Bulletin board for errors

### Databricks Equivalent Monitoring
```python
# Stream monitoring in Databricks
query = df.writeStream \
    .format("delta") \
    .trigger(processingTime='10 seconds') \
    .start("/delta/output")

# Monitor stream progress
query.status
query.lastProgress
query.recentProgress
```

## 🎓 Next Steps

1. **Build Complex Flows**: Combine multiple data sources
2. **Implement SLAs**: Use NiFi's prioritization and scheduling
3. **Version Control**: Use NiFi Registry for flow versioning
4. **Production Patterns**: Implement clustering and high availability
5. **Migrate to Cloud**: Convert flows to Databricks Delta Live Tables

## 📚 Resources

- [NiFi Documentation](https://nifi.apache.org/docs.html)
- [NiFi Expression Language](https://nifi.apache.org/docs/nifi-docs/html/expression-language-guide.html)
- [Databricks Auto Loader](https://docs.databricks.com/ingestion/auto-loader/index.html)
- [Delta Live Tables](https://docs.databricks.com/delta-live-tables/index.html)