# Part 3: Dimensional Data Modeling Exercise

## 🏗️ Star Schema Design Architecture

To ensure enterprise-grade analytical performance and simple, error-free consumption paths for Power BI BI models, data models are built entirely around a high-performance **Star Schema**:

---
<img width="491" height="285" alt="image" src="https://github.com/user-attachments/assets/7eabd1ad-bacd-4256-a118-5a4f828cc454" />
---

## ⚙️ Architectural Warehouse Attributes

### 1. Granular Fact Definitions
* **`gold_fact_sales`:** The grain is exactly **one line item per customer order**. This granularity accommodates orders containing multiple discrete products, ensuring downstream users can slice financial metrics at the lowest depth.
* **`gold_fact_returns`:** The grain is exactly **one line item per returned product instance**.

### 2. Slowly Changing Dimensions (SCD) Philosophy
* **`gold_dim_customer` (SCD Type 2):** Tracks historical customer shifts cleanly over time. Changes in geographical parameters do not overwrite old data; instead, old rows are marked inactive (`is_current = False`) with a definitive `end_date`, and a new operational record is appended. This preserves historical sales alignment to historical regions.
* **`gold_dim_product` (SCD Type 1):** Core product information overrides are processed via direct overwrites where historical tracking of catalog attribute shifts isn't explicitly required by business rules.

### 3. Key Generation Strategy
Distributed cloud nodes can bottleneck or duplicate values if traditional sequential identity numbers are generated. To maintain full parallel execution capability, surrogate keys are generated using deterministic cryptographic hashes:
$$\text{Surrogate Key} = \text{SHA2}(\text{CONCAT\_WS}('\text{||}', \text{Business\_ID}, \text{Start\_Date}), 256)$$
