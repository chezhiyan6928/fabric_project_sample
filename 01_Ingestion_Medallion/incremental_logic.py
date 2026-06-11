def merge_incremental_orders(stage_df):
    """
    Demonstrates how new / late-arriving records from Silver 
    are upserted cleanly into the Gold Fact table.
    """
    stage_df.createOrReplaceTempView("incremental_stage_sales")
    
    # Delta Lake Merge syntax
    spark.sql("""
        MERGE INTO gold_fact_sales AS target
        USING incremental_stage_sales AS source
        ON target.order_id = source.order_id AND target.product_id = source.product_id
        WHEN MATCHED THEN
            UPDATE SET 
                target.quantity = source.quantity,
                target.total_sales_amount = source.total_sales_amount
        WHEN NOT MATCHED THEN
            INSERT *
    """)
