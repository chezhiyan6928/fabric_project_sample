# Part 4: Microsoft Fabric Data Factory / Orchestration Challenge

## 🛠️ Orchestration Pipeline Design

The master batch orchestration flow is deployed using Microsoft Fabric Data Factory. It enforces rigid step-dependencies, error handling, parameters, and environmental isolation logic.

<img width="596" height="333" alt="image" src="https://github.com/user-attachments/assets/9c467347-af95-4c2b-9f86-bd25a6314984" />

---

## 🔧 Operational & Governance Settings

### 1. Parameterized Multi-Environment Routing (Dev/Test/Prod)
The orchestrator avoids hardcoded target path parameters by implementing a centralized parameter pipeline setup:
* **Pipeline Parameter:** `p_env` (Defaults to `dev`).
* **Dynamic Expression Mapping:**
  ```json
  @if(equals(pipeline().parameters.p_env, 'prod'), 'abfss://data@prodstorage.dfs.core.windows.net/', 'abfss://data@devstorage.dfs.core.windows.net/')
