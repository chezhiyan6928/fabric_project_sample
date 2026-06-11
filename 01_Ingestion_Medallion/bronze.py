# Helper function to write to Bronze Delta Tables
def write_to_bronze(df, table_name, file_name):
    bronze_df = df.withColumn("bi_ingestion_timestamp", F.current_timestamp()) \
                  .withColumn("bi_source_file_name", F.lit(file_name)) \
                  .withColumn("bi_pipeline_run_id", F.lit(PIPELINE_RUN_ID))
    
    # Write as delta table
    bronze_df.write.format("delta").mode("append").saveAsTable(f"bronze_{table_name}")
    print(f"Successfully ingested to bronze_{table_name}")

# --- INGESTION EXECUTION ---
# (Assuming raw files are sitting in your Lakehouse/DBFS landing zone)
# path_prefix = "abfss://your-container@your-storage.dfs.core.windows.net/landing/"

# 1. Customers
# customers_df = spark.read.csv(f"{path_prefix}customers.csv", header=True, inferSchema=True)
# write_to_bronze(customers_df, "customers", "customers.csv")

# 2. Products
# products_df = spark.read.csv(f"{path_prefix}products.csv", header=True, inferSchema=True)
# write_to_bronze(products_df, "products", "products.csv")

# 3. Orders
# orders_df = spark.read.csv(f"{path_prefix}orders.csv", header=True, inferSchema=True)
# write_to_bronze(orders_df, "orders", "orders.csv")

# 4. Order Items (JSON)
# order_items_df = spark.read.json(f"{path_prefix}order_items.json")
# write_to_bronze(order_items_df, "order_items", "order_items.json")

# 5. Returns
# returns_df = spark.read.csv(f"{path_prefix}returns.csv", header=True, inferSchema=True)
# write_to_bronze(returns_df, "returns", "returns.csv")
