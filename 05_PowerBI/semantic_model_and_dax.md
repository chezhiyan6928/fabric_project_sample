# Power BI Enterprise Semantic Model Configuration

This document contains the exact DAX measures and model configuration details optimized for the VertiPaq storage engine.

## 📊 Core DAX Measures

### 1. Total Sales
```dax
Total Sales = SUM(gold_fact_sales[gross_sales_amount])

Net Sales = SUM(gold_fact_sales[net_sales_amount])

Gross Margin % = 
DIVIDE(
    [Net Sales] - SUM(gold_fact_sales[discount_amount]), 
    [Net Sales], 
    0
)

Return Rate = 
DIVIDE(
    SUM(gold_fact_returns[return_quantity]), 
    SUM(gold_fact_sales[quantity]), 
    0
)

Average Order Value = 
DIVIDE(
    [Net Sales], 
    DISTINCTCOUNT(gold_fact_sales[order_id]), 
    0
)

YoY Sales Growth = 
VAR CurrentSales = [Net Sales]
VAR PriorYearSales = 
    CALCULATE(
        [Net Sales], 
        SAMEPERIODLASTYEAR(gold_dim_date[calendar_date])
    )
RETURN
    DIVIDE(CurrentSales - PriorYearSales, PriorYearSales, 0)

Customer Count = DISTINCTCOUNT(gold_fact_sales[customer_sk])

Top Product Rank = 
IF(
    ISINSCOPE(gold_dim_product[product_name]),
    RANKX(
        ALLSELECTED(gold_dim_product),
        [Net Sales],
        ,
        DESC,
        Dense
    ),
    BLANK()
)

