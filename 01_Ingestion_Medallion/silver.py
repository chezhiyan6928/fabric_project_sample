# Create a quarantine handler to document and store bad records
def quarantine_bad_records(df, condition, rule_name, table_name):
    bad_records = df.filter(~condition).withColumn("quarantine_reason", F.lit(f"{table_name}: {rule_name}"))
    if bad_records.count() > 0:
        bad_records.write.format("delta").mode("append").saveAsTable("silver_quarantine_records")
    return df.filter(condition)

# --- CLEANING EXECUTION ---

# 1. Silver Customers: Deduplicate customer records
df_bronze_cust = spark.read.table("bronze_customers")
df_silver_cust = df_bronze_cust.dropDuplicates(["customer_id"]) \
    .filter(F.col("customer_id").isNotNull())
df_silver_cust.write.format("delta").mode("overwrite").saveAsTable("silver_customers")

# 2. Silver Products: Validate product IDs
df_bronze_prod = spark.read.table("bronze_products")
# Condition: Product ID must not be null or malformed
valid_prod_cond = F.col("product_id").isNotNull() & (F.length(F.col("product_id")) > 0)
df_silver_prod = quarantine_bad_records(df_bronze_prod, valid_prod_cond, "Invalid Product ID", "products")
df_silver_prod.write.format("delta").mode("overwrite").saveAsTable("silver_products")

# 3. Silver Orders: Standardize inconsistent date formats
df_bronze_orders = spark.read.table("bronze_orders")
# Coalesce handles parsing multiple common incoming date patterns (e.g., yyyy-MM-dd vs MM/dd/yyyy)
df_silver_orders = df_bronze_orders.withColumn(
    "order_date_clean", 
    F.coalesce(
        F.to_date(F.col("order_date"), "yyyy-MM-dd"),
        F.to_date(F.col("order_date"), "MM/dd/yyyy"),
        F.to_date(F.col("order_date"), "dd-MM-yyyy")
    )
)
# Quarantine records where dates completely failed to parse
valid_date_cond = F.col("order_date_clean").isNotNull()
df_silver_orders = quarantine_bad_records(df_silver_orders, valid_date_cond, "Malformed Order Date", "orders") \
    .drop("order_date").withColumnRenamed("order_date_clean", "order_date")

df_silver_orders.write.format("delta").mode("overwrite").saveAsTable("silver_orders")

# 4. Silver Order Items
df_bronze_items = spark.read.table("bronze_order_items")
df_silver_items = df_bronze_items.filter(F.col("order_item_id").isNotNull())
df_silver_items.write.format("delta").mode("overwrite").saveAsTable("silver_order_items")

# 5. Silver Returns
df_bronze_returns = spark.read.table("bronze_returns")
df_silver_returns = df_bronze_returns.filter(F.col("return_id").isNotNull())
df_silver_returns.write.format("delta").mode("overwrite").saveAsTable("silver_returns")
