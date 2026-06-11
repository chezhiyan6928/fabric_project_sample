
## 🏗️ Semantic Model Architecture Diagram & Relationships
To optimize query traversal paths within the VertiPaq memory engine, the model strictly adheres to a **Star Schema** layout:

* `gold_dim_customer` (1) ───> (*) `gold_fact_sales` (Cross filter: Single)
* `gold_dim_product`  (1) ───> (*) `gold_fact_sales` (Cross filter: Single)
* `gold_dim_date`     (1) ───> (*) `gold_fact_sales` (Cross filter: Single)
* `gold_dim_product`  (1) ───> (*) `gold_fact_returns` (Cross filter: Single)
* `gold_dim_date`     (1) ───> (*) `gold_fact_returns` (Cross filter: Single)

*Note: Bidirectional (Both) cross-filtering is strictly avoided on 1-to-many relationships to eliminate ambiguity and prevent performance degradation.*

---

## 🔐 Row-Level Security (RLS) Implementation
To enforce data access controls for **Regional Managers**, an active security role is defined:

1. **Role Name:** `Regional_Manager`
2. **Table Filter Expression on `gold_dim_customer`:**
```dax
[customer_region] = 
LOOKUPVALUE(
    user_mapping[Region], 
    user_mapping[UserEmail], 
    USERPRINCIPALNAME()
)
