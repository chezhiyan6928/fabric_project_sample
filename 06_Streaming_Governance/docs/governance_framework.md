# Data Governance, Cataloging, and Trust Framework

To ensure engineering rigor and build trust for downstream consumer reporting, the pipeline integrates a multi-layered governance layer:

### 1. End-to-End Data Lineage & Discoverability
* **Unity Catalog Integration:** Every table generated across our Medallion framework (Bronze, Silver, Gold, Streaming) is registered to a centralized data catalog (e.g., Unity Catalog or Fabric OneLake Catalog). 
* This provides automated runtime column-level lineage tracking. Compliance officers and data consumers can visibly trace a Power BI metric's origin back through the transformations to the exact raw source file or real-time event hub stream.

### 2. Operational Transparency & Monitoring
* The real-time streaming pipeline continuously writes metadata telemetry logs directly into `gold_streaming_monitoring_dashboard`.
* This dataset exposes vital operational health metrics—such as message volume throughput, operational lag/latency processing times, and absolute dead-letter queue exception ratios—empowering engineers to immediately pinpoint upstream ingestion drops.

### 3. Access Controls & Retentions
* **Role-Based Access Control (RBAC):** Raw landing and Bronze zones are strictly locked down to automated service principals. Analysts and business users are explicitly restricted to the Gold layer.
* **Time-Travel Auditing:** Delta Lake retention parameters are set to retain 30 days of data version history (`SET delta.logRetentionDuration = "interval 30 days"`). This enables immediate historical rollback capabilities to restore metrics if upstream breaking bugs corrupt data points.
