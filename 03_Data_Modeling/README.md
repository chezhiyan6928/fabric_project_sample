# Part 3: Dimensional Data Modeling Exercise

## 🏗️ Star Schema Design Architecture

To ensure enterprise-grade analytical performance and simple, error-free consumption paths for Power BI BI models, data models are built entirely around a high-performance **Star Schema**:
┌──────────────────┐
│ gold_dim_product │
└────────┬─────────┘
│ 1
│
│ *
┌──────────────────┐    ┌─▼──────────────┐    ┌──────────────┐
│ gold_dim_customer├───>│ gold_fact_sales│<───┤ gold_dim_date│
└──────────────────┘1   └────────▲───────┘ 1  └──────┬───────┘
│                   │
│ * │ 1
┌────────┴────────┐          │
│gold_fact_returns│<─────────┘
└─────────────────┘ *
