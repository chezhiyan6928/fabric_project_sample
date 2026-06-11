# 1. dim_customer
df_cust = spark.read.table("silver_customers")
dim_customer = df_cust.select("customer_id", "customer_name", "customer_email", "customer_region")
dim_customer.write.format("delta").mode("overwrite").saveAsTable("gold_dim_customer")

# 2. dim_product
df_prod = spark.read.table("silver_products")
dim_product = df_prod.select("product_id", "product_name", "product_category", "product_price")
dim_product.write.format("delta").mode("overwrite").saveAsTable("gold_dim_product")

# 3. dim_date (Generated dynamically for high-quality semantic models)
start_date = "2024-01-01"
end_date = "2026-12-31"
dim_date = spark.sql(f"""
    SELECT 
        explode(sequence(to_date('{start_date}'), to_date('{end_date}'), interval 1 day)) as date
""") \
.withColumn("date_key", F.date_format(F.col("date"), "yyyyMMdd").cast("int")) \
.withColumn("year", F.year(F.col("date"))) \
.withColumn("month", F.month(F.col("date"))) \
.withColumn("day", F.dayofmonth(F.col("date"))) \
.withColumn("quarter", F.quarter(F.col("date"))) \
.withColumn("day_of_week", F.dayofweek(F.col("date")))
dim_date.write.format("delta").mode("overwrite").saveAsTable("gold_dim_date")

# 4. fact_sales (Combining orders and item details)
orders = spark.read.table("silver_orders")
items = spark.read.table("silver_order_items")

fact_sales = items.join(orders, on="order_id", how="inner") \
    .select(
        F.col("order_id"),
        F.col("customer_id"),
        F.col("product_id"),
        F.date_format(F.col("order_date"), "yyyyMMdd").cast("int").alias("order_date_key"),
        F.col("quantity"),
        F.col("price_per_unit"),
        (F.col("quantity") * F.col("price_per_unit")).alias("total_sales_amount")
    )
fact_sales.write.format("delta").mode("overwrite").saveAsTable("gold_fact_sales")

# 5. fact_returns
returns = spark.read.table("silver_returns")
fact_returns = returns.select(
    F.col("return_id"),
    F.col("order_id"),
    F.col("product_id"),
    F.date_format(F.col("return_date"), "yyyyMMdd").cast("int").alias("return_date_key"),
    F.col("return_quantity"),
    F.col("return_reason")
)
fact_returns.write.format("delta").mode("overwrite").saveAsTable("gold_fact_returns")
