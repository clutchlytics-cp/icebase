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
- [x] Phase 1: Foundation & seed data generator
- [x] Phase 2: Bronze → Silver Lakeflow declarative pipeline
- [x] Phase 3: Gold layer, Lakeflow Jobs orchestration
- [x] Phase 4: ML — fan segmentation & churn prediction
- [x] Phase 5: AI/BI Dashboards — branded, business-ready
- [ ] Phase 6: SQL Alerts & monitoring capstone


## Silver Layer Schema
| Table | Type | Source | Key Additions |
|---|---|---|---|
| `dim_game` | Materialized View | bronze.raw_events | result_numeric, is_playoff_relevant, wins_to_date |
| `dim_customer` | Streaming Table | raw_customers + stream | INITCAP name clean, tenure_days, record_source |
| `fact_tickets` | Streaming Table | raw_tickets + stream | seat_tier_rank, days_before_game, is_advance_purchase |
| `bridge_promo` | Materialized View | bronze.raw_promotions | promo_impact_score, discount_tier |
| `quarantine_tickets` | Streaming Table | Volume landing zone | Routes bad records — null IDs, zero prices |

## Gold Layer Schema
| Table | Rows | Key Columns | Powers |
|---|---|---|---|
| `customer_360` | ~5,400 | total_spend, promo_sensitivity, churn_flag, revenue_net | ML models, Fan Health Dashboard |
| `game_revenue` | 82 | gross_revenue, net_revenue, fill_rate, revenue_index | Revenue Ops Dashboard |
| `retention_cohort` | ~5,400 | churn_flag, returned_30d, days_since_last, is_jersey_night_cohort | Churn model training data |

## Orchestration
Lakeflow Job: `icebase-orchestrator` — runs every 30 minutes
Task DAG: Silver Pipeline → [customer_360 ∥ game_revenue] → retention_cohort

## Machine Learning Layer
| Model | Type | Algorithm | Output Table | Registry |
|---|---|---|---|---|
| Fan Segmentation | Unsupervised | K-Means (k=5) | `ml_fan_segments` | `icebase.gold.kmeans_fan_segmentation` |
| Churn Prediction | Supervised | XGBoost | `ml_churn_scores` | `icebase.gold.xgboost_churn_predictor` |

**Fan Segments:** Season Core · High Value New · Casual Fan · Promo Hunter · Lapsed
**Churn Risk Tiers:** High (≥0.70) · Medium (0.40–0.70) · Low (<0.40)
**MLflow Tracking:** Full experiment logging — parameters, AUC, confusion matrix, feature importance
**Feature Lineage:** Tracked via Databricks Feature Engineering in Unity Catalog

## Dashboards
Three AI/BI dashboards built on Databricks SQL, branded with the Idaho Mashers identity:

| Dashboard | Audience | Key Visuals |
|---|---|---|
| **Fan Health** | CMO, Retention | Churn risk distribution, high-risk watchlist, Jersey Night cohort |
| **Revenue Operations** | VP Revenue, Finance | Season revenue arc, fill rate trend, promo impact by phase |
| **Segment Explorer** | Marketing | Segment profiles, channel quality, campaign action guide |

*All dashboards read from Gold and ML output tables, refreshed automatically by the Lakeflow Job.*

