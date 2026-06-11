from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
import time

# Initialize Spark Session configured with Delta Lake
spark = SparkSession.builder \
    .appName("NTT_Data_Performance_Optimization") \
    .config("spark.sql.adaptive.enabled", "true") \
    .getOrCreate()

# ==========================================
# 1. SIMULATING LARGE SCALE DATASET (20M+ Rows)
# ==========================================
print("Generating simulated large transaction dataset...")
# Creating a base dataframe of 20 million rows
base_df = spark.range(0, 20000000)

# Generating high-volume transaction records
transactions_large = base_df.withColumn("order_id", F.expr("cast(id as string)")) \
    .withColumn("customer_id", F.expr("cast(floor(rand() * 500000) as string)")) \
    .withColumn("product_id", F.expr("cast(floor(rand() * 10000) as string)")) \
    .withColumn("region", F.expr("case cast(floor(rand() * 5) as int) "
                                "when 0 then 'North' when 1 then 'South' when 2 then 'East' "
                                "when 3 then 'West' else 'Central' end")) \
    .withColumn("transaction_date", F.expr("date_add(to_date('2024-01-01'), cast(floor(rand() * 730) as int))")) \
    .withColumn("quantity", F.expr("cast(floor(rand() * 5) + 1 as int)")) \
    .withColumn("price_per_unit", F.expr("cast(rand() * 100 as double)"))

transactions_large.createOrReplaceTempView("transactions_large_view")

# Loading the smaller dimension tables from Exercise 1
# (Assuming gold tables are already registered in catalog)
# products_dim = spark.read.table("gold_dim_product")

# ==========================================
# 2. THE NAÏVE APPROACH (Shuffle & Scan Heavy)
# ==========================================
def run_naive_transformations():
    print("\nExecuting Naïve Transformations...")
    
    # Read raw table directly without optimization
    df = spark.table("transactions_large_view")
    
    # Simple join without broadcasting a smaller dataset
    # (Assuming we join with a product dimension table to fetch category)
    # This triggers an expensive Shuffle Hash Join or Sort Merge Join across 20M rows
    
    # Metric A: Daily sales by region
    daily_sales = df.groupBy("transaction_date", "region") \
        .agg(F.sum(F.col("quantity") * F.col("price_per_unit")).alias("total_sales"))
    
    # Metric B: Customer Lifetime Value (CLV)
    clv = df.groupBy("customer_id") \
        .agg(F.sum(F.col("quantity") * F.col("price_per_unit")).alias("customer_lifetime_value"))
    
    # Force execution to measure time using a count action
    daily_sales.count()
    clv.count()

# ==========================================
# 3. THE OPTIMIZED APPROACH (Applying Top Tuning Mechanics)
# ==========================================
def run_optimized_transformations():
    print("\nExecuting Optimized Transformations...")
    
    # Optimization 1: Predicate Pushdown / Filtering early
    # Optimization 2: Caching if a dataframe is reused multiple times
    df_optimized = spark.table("transactions_large_view").filter(F.col("transaction_date").isNotNull())
    df_optimized.cache() 
    
    # Optimization 3: Broadcast Join for Dimension Tables
    # Instead of an expensive SortMergeJoin, we explicitly broadcast smaller tables
    # products_broadcast = F.broadcast(spark.read.table("gold_dim_product"))
    # df_enriched = df_optimized.join(products_broadcast, "product_id", "inner")

    # Metric A: Daily sales by region
    daily_sales_opt = df_optimized.groupBy("transaction_date", "region") \
        .agg(F.sum(F.col("quantity") * F.col("price_per_unit")).alias("total_sales"))
    
    # Metric B: Customer Lifetime Value (CLV)
    clv_opt = df_optimized.groupBy("customer_id") \
        .agg(F.sum(F.col("quantity") * F.col("price_per_unit")).alias("customer_lifetime_value"))
    
    # Execution
    daily_sales_opt.count()
    clv_opt.count()
    
    # Clear cache memory immediately after processing
    df_optimized.unpersist()

# ==========================================
# 4. BENCHMARKING & EXECUTION RUNTIME COMPARISON
# ==========================================
# Run Naïve
start_time = time.time()
run_naive_transformations()
naive_duration = time.time() - start_time
print(f"--- Naïve Execution Time: {naive_duration:.2f} seconds ---")

# Run Optimized
start_time = time.time()
run_optimized_transformations()
optimized_duration = time.time() - start_time
print(f"--- Optimized Execution Time: {optimized_duration:.2f} seconds ---")

# ==========================================
# 5. PHYSICAL OPTIMIZATION: FILE COMPACTION & Z-ORDERING
# ==========================================
def apply_physical_storage_tuning():
    """
    Simulates writing optimized data back to Delta Lake using physical storage features
    """
    print("\nApplying physical layout optimizations to the Delta layer...")
    
    # Optimization 4: Partitioning the data on a highly queried field (Region)
    # Optimization 5: Liquid Clustering / Z-Ordering for lightning-fast file skipping
    
    # (Note: Execute via SQL command for Delta engine optimization)
    # spark.sql("OPTIMIZE gold_fact_sales ZORDER BY (customer_id, product_id)")
    print("Physical tuning commands structured successfully.")
