# Part 1: Lakehouse Ingestion & Medallion Pipeline

## 🏗️ Architecture Overview
This layer implements a classic **Medallion Architecture** using PySpark to safely transition raw data into structured, clean, and optimized analytical datasets.

1. **Bronze Layer:** Ingests raw source datasets (`customers.csv`, `products.csv`, `orders.csv`, `order_items.json`, and `returns.csv`) exactly as received. It appends standard metadata audit tracking metrics without altering schemas.
2. **Silver Layer:** Enforces data cleaning, parsing, deduplication, schema validation, and date standardization.
3. **Gold Layer:** Models data into a high-performance Star Schema optimizing transactional business facts and dimensional lookups.

---

## 🛡️ Operational Isolation & Quarantine Strategy
To prevent malformed data from crashing downstream batch executions, this pipeline does not silently drop bad rows. Instead, it utilizes an explicit **Quarantine Gate Engine**:

* **Deduplication:** Customer records are deduplicated using composite keys (`customer_id`). Null keys are dynamically segregated.
* **Date Uniformity:** The order dates are normalized by testing multiple common string formats (`yyyy-MM-dd`, `MM/dd/yyyy`, `dd-MM-yyyy`) into a rigid timestamp datatype.
* **Quarantine Table:** Records failing core validations (such as a missing or structural anomaly in a `product_id` or an unparseable date string) are automatically isolated into a centralized `silver_quarantine_records` table with a detailed text string mapping out the exact rule violation reason for engineering diagnostics.

---

## 🔄 Incremental Load & Late-Arriving Dimensions
For high-volume facts (`gold_fact_sales`), the pipeline avoids heavy table-overwrite patterns. Instead, it uses a Delta Lake `MERGE INTO` (upsert) command based on the target composite primary business keys.

When a late-arriving order or an updated line item passes through Silver, the merge statement matches on `order_id` and `product_id`. If a match is found, it updates the record fields dynamically; if no match is found, it inserts it as a new transaction line item.
