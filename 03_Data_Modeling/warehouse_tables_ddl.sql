-- ====================================================================
-- ENTERPRISE WAREHOUSE MODEL: STAR SCHEMA IMPLEMENTATION
-- Target System: Azure Databricks Unity Catalog / Microsoft Fabric Warehouse
-- ====================================================================

-- 1. Dim_Customer: Implemented as Slowly Changing Dimension (SCD) Type 2
CREATE TABLE IF NOT EXISTS gold_dim_customer (
    customer_sk STRING NOT NULL COMMENT 'Surrogate Key generated using hash(customer_id, start_date)',
    customer_id STRING NOT NULL COMMENT 'Natural business key from source system',
    customer_name STRING,
    customer_email STRING,
    customer_region STRING,
    -- SCD Type 2 Audit Tracking Columns
    start_date DATE NOT NULL COMMENT 'The date this version of the customer record became active',
    end_date DATE COMMENT 'The date this version expired. NULL indicates the current active version',
    is_current BOOLEAN NOT NULL COMMENT 'Flag indicating the active operational row (True/False)',
    bi_updated_at TIMESTAMP
) USING DELTA
COMMENT 'Conformed Customer Dimension with SCD Type 2 historical tracking';

-- 2. Dim_Product: Implemented as SCD Type 1 (Direct Overwrites)
CREATE TABLE IF NOT EXISTS gold_dim_product (
    product_sk STRING NOT NULL COMMENT 'Surrogate Key generated using hash(product_id)',
    product_id STRING NOT NULL COMMENT 'Natural business key from source system',
    product_name STRING,
    product_category STRING,
    product_price DOUBLE,
    bi_updated_at TIMESTAMP
) USING DELTA
COMMENT 'Conformed Product Dimension tracking core product catalog attributes';

-- 3. Dim_Date: Static, Pre-calculated Time Dimension
CREATE TABLE IF NOT EXISTS gold_dim_date (
    date_key INT NOT NULL COMMENT 'Surrogate Key in YYYYMMDD format',
    calendar_date DATE NOT NULL,
    calendar_year INT NOT NULL,
    calendar_month INT NOT NULL,
    calendar_day INT NOT NULL,
    calendar_quarter INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name STRING NOT NULL,
    month_name STRING NOT NULL
) USING DELTA
COMMENT 'Time Intelligence Dimension for high-performance Power BI parsing';

-- 4. Fact_Sales: Core Transactional Fact Table
CREATE TABLE IF NOT EXISTS gold_fact_sales (
    order_id STRING NOT NULL COMMENT 'Degenerate dimension / operational business key',
    customer_sk STRING NOT NULL COMMENT 'Foreign key to gold_dim_customer',
    product_sk STRING NOT NULL COMMENT 'Foreign key to gold_dim_product',
    order_date_key INT NOT NULL COMMENT 'Foreign key to gold_dim_date (YYYYMMDD)',
    quantity INT NOT NULL,
    price_per_unit DOUBLE NOT NULL,
    gross_sales_amount DOUBLE NOT NULL,
    discount_amount DOUBLE DEFAULT 0.0,
    net_sales_amount DOUBLE NOT NULL
) USING DELTA
PARTITIONED BY (order_date_key)
COMMENT 'Transactional Fact table storing line-item revenue records';

-- 5. Fact_Returns: Supporting Analysis Fact Table
CREATE TABLE IF NOT EXISTS gold_fact_returns (
    return_id STRING NOT NULL,
    order_id STRING NOT NULL,
    product_sk STRING NOT NULL COMMENT 'Foreign key to gold_dim_product',
    return_date_key INT NOT NULL COMMENT 'Foreign key to gold_dim_date (YYYYMMDD)',
    return_quantity INT NOT NULL,
    return_reason STRING
) USING DELTA
COMMENT 'Supporting Fact table tracking product returns and reason codes';
