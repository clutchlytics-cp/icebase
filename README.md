# IceBase
**Idaho Mashers Hockey Analytics Platform**

A declarative, medallion-architecture analytics pipeline built in Databricks
around a mock professional hockey franchise. Covers the full data engineering
stack from raw ingestion to ML-powered fan segmentation and churn prediction.

## Tech Stack
- **Platform:** Databricks (Delta Live Tables, Workflows, MLflow, Unity Catalog)
- **Storage:** Delta Lake — Bronze / Silver / Gold medallion architecture
- **Languages:** PySpark, Databricks SQL, Python (sklearn, XGBoost, Faker)
- **ML:** K-Means fan segmentation + XGBoost churn prediction via MLflow

## Season Narrative
The Idaho Mashers' data tells a deliberate business story:
- 🟢 **Hot Start (Games 1–22):** Organic demand, premium pricing, low promos
- 🔴 **Midseason Slump (Games 23–52):** Discount over-reliance erodes margins
- 🟡 **Late Push (Games 53–72):** Recovery, but promo-conditioned fans resist
- 🟣 **Jersey Night (Game 77):** Mack Tateson #14 retired — sellout, 412 new fans

## Project Phases
- [x] **Phase 1:** Foundation & seed data generator
- [ ] Phase 2: Bronze → Silver DLT pipeline
- [ ] Phase 3: Gold layer & orchestration
- [ ] Phase 4: ML — segmentation & churn
- [ ] Phase 5: Dashboards & portfolio polish# icebase
