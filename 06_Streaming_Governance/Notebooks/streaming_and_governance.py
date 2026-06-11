from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType

# Initialize Spark Session for Streaming and Delta Lake
spark = SparkSession.builder \
    .appName("NTT_Data_Streaming_Governance") \
    .getOrCreate()

# 1. DEFINE SCHEMA FOR INCOMING EVENT STREAM (Kafka / Event Hub)
stream_schema = StructType() \
    .add("order_id", StringType(), False) \
    .add("customer_id", StringType(), True) \
    .add("product_id", StringType(), True) \
    .add("quantity", IntegerType(), True) \
    .add("price_per_unit", DoubleType(), True) \
    .add("event_timestamp", TimestampType(), True)

# 2. SIMULATING STREAMING INGESTION USING MEMORY / FILE ENGINE
# In a real environment, this would point to: format("kafka") or format("eventhubs")
streaming_raw_df = spark.readStream \
    .format("rate") \
    .option("rowsPerSecond", 50) \
    .load() \
    .withColumn("value", F.lit('{"order_id":"ORD999","customer_id":"CUST101","product_id":"PROD55","quantity":2,"price_per_unit":49.99,"event_timestamp":"2026-06-11T14:30:00Z"}')) \
    .withColumn("parsed_data", F.from_json(F.col("value"), stream_schema)) \
    .select("parsed_data.*")

# 3. IMPLEMENTING DATA QUALITY GATES & STREAM SPLITTING
# Rule Definitions: No null order IDs, valid customer tracking, and logical quantities
df_with_validation = streaming_raw_df.withColumn(
    "is_valid_record",
    (F.col("order_id").isNotNull()) & 
    (F.col("customer_id").isNotNull()) & 
    (F.col("quantity") > 0) &
    (F.col("event_timestamp").isNotNull())
)

# 4. MICRO-BATCH WRITER FOR DUAL-ROUTING (Valid vs Quarantine)
def process_micro_batch(batch_df, batch_id):
    """
    Processes each real-time micro-batch, logging lineage, metrics,
    and routing data safely to either Silver or the Dead-Letter/Quarantine layer.
    """
    batch_df.cache()
    
    # Route Valid Records to Silver Lakehouse Layer
    valid_records = batch_df.filter(F.col("is_valid_record") == True).drop("is_valid_record")
    valid_records.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("silver_streaming_orders")
        
    # Route Invalid Records to Dead Letter Table for Audit Analysis
    invalid_records = batch_df.filter(F.col("is_valid_record") == False) \
        .withColumn("quarantine_reason", F.lit("Streaming Validation Failure: Missing IDs or Quantity <= 0")) \
        .withColumn("failed_at", F.current_timestamp())
        
    if invalid_records.count() > 0:
        invalid_records.write \
            .format("delta") \
            .mode("append") \
            .saveAsTable("silver_streaming_dead_letter_queue")
            
    # 5. REAL-TIME METRICS & AGGREGATIONS FOR OPERATIONAL MONITORING
    # Automatically calculates streaming throughput and operational state
    metrics_summary = batch_df.groupBy() \
        .agg(
            F.count("*").alias("total_processed_records"),
            F.sum(F.when(F.col("is_valid_record") == True, 1).otherwise(0)).alias("valid_record_count"),
            F.sum(F.when(F.col("is_valid_record") == False, 1).otherwise(0)).alias("failed_record_count"),
            F.max(F.col("event_timestamp")).alias("latest_event_latency")
        ) \
        .withColumn("batch_id", F.lit(batch_id)) \
        .withColumn("monitoring_updated_at", F.current_timestamp())
        
    metrics_summary.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable("gold_streaming_monitoring_dashboard")
        
    batch_df.unpersist()

# 6. START THE STREAMING QUERY EXECUTION
query = df_with_validation.writeStream \
    .foreachBatch(process_micro_batch) \
    .option("checkpointLocation", "/mnt/telemetry/checkpoints/order_streaming_query") \
    .trigger(processingTime='10 seconds') \
    .start()

# query.awaitTermination()
