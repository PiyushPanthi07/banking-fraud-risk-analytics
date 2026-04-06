# 🏦 Banking Fraud & Risk Analytics Platform

> **End-to-end behavioral fraud detection and risk intelligence system** — synthetic data engineering → cloud data warehousing → calibrated risk scoring → executive dashboards.  
> Built independently to replicate production-grade pipelines used at institutions like JPMC, Barclays, and Amex.

---

## 📌 Quick Navigation

| Section | Description |
|---|---|
| [Project Overview](#-project-overview) | What this is, why it matters |
| [Architecture](#-architecture) | Medallion pipeline, data flow |
| [Dataset](#-dataset) | Synthetic data design, 6 source tables |
| [Data Pipeline](#-data-pipeline) | Bronze → Silver → Gold layer details |
| [Risk Scoring Model](#-risk-scoring-model) | 5-component model, calibration logic |
| [Gold Layer Views](#-gold-layer-views) | 11 analytical views |
| [Dashboards](#-dashboards) | 5 Tableau dashboards, audience & KPIs |
| [AML & Compliance](#-aml--compliance-signals) | Orphaned transactions, AML flags |
| [Key Engineering Decisions](#-key-engineering-decisions) | FK resolution, calibration, governance |
| [Tech Stack](#-tech-stack) | Full tool and platform list |
| [Project Scale](#-project-scale) | Numbers at a glance |
| [Setup & Replication](#-setup--replication) | How to run this locally |

---

## 🎯 Project Overview

This project demonstrates an **enterprise-grade fraud and risk analytics pipeline** — the kind financial institutions use to monitor transaction risk in real time, surface AML signals, and report risk exposure to senior leadership.

**What makes it different from typical portfolio projects:**
- No predefined fraud labels — risk is entirely derived from **behavioral signals** (velocity, amount anomalies, geographic mismatches, device patterns)
- Built with the same **architectural patterns** used in production (Medallion, Star Schema, bridge-key resolution)
- Outputs are **role-specific** — each dashboard targets a different stakeholder (CRO, KYC Officer, Financial Crime Director)
- Every design decision is **documented and defensible** — including what was excluded and why

**Target domain:** Financial services fraud, AML compliance, transaction risk intelligence

---

## 🏗 Architecture

```
Raw CSVs (AWS S3)
      │
      ▼
┌─────────────┐
│   BRONZE    │  Raw ingestion, type casting, UK country normalization,
│  (bronze_v2)│  channel bias injection, zero nulls enforcement
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   SILVER    │  Enrichment layer: country_mismatch_flag, is_international,
│  (silver_v2)│  channel_risk_region, entry_mode_valid, FX rate joins
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    GOLD     │  Star Schema: fact_transaction_risk_scores + 4 dims
│  (gold_v2)  │  5-component risk scores, 11 analytical views
└──────┬──────┘
       │
       ▼
  Tableau (5 Dashboards)
```

**Platform:** Snowflake (`BANKING_DB` / `BANKING_WH`) · **Storage:** AWS S3 (`banking-fraud-risk-analytics`, `eu-north-1`)

---

## 📦 Dataset

Fully synthetic — 1 million transactions across **6 source tables**. No publicly available labeled dataset was used. All fraud signals are derived behaviorally.

| Table | Rows | Key Fields |
|---|---|---|
| `transactions` | 1,000,000 | txn_id, customer_id, amount, channel, txn_country, txn_date |
| `customers` | ~100,000 | customer_id, home_country, billing_country, income_band, age |
| `accounts` | ~120,000 | account_id, customer_id, account_type, opened_date |
| `merchants` | ~10,000 | merchant_id, merchant_category, merchant_country, chargeback_rate |
| `devices` | ~80,000 | device_id, customer_id, device_type, device_country |
| `fx_rates` | ~365 | currency, date, rate_to_usd |

**Synthetic design decisions:**
- Channel-region bias injected manually (ATM ~35% in NG/ZA/BR/MX; ONLINE ~39% in US/UK/DE; MOBILE ~37% in IN/KE) — uniform channel distribution is a synthetic artifact; bias makes geographic risk signals credible
- `entry_mode` synced to `channel` to prevent logical violations
- No fraud label column — risk is derived, not labeled

---

## 🔄 Data Pipeline

### Bronze Layer (`bronze_v2`)
Raw ingestion with structural fixes applied.

| Rule | Detail |
|---|---|
| UK country normalization | Standardized to `GB` across all tables |
| Channel bias injection | Region-specific channel probabilities applied |
| entry_mode sync | Updated in lockstep with channel to eliminate violations |
| Null enforcement | Zero nulls in key identifier and amount fields |
| Schema preservation | Original bronze retained in parallel — no destructive edits |

### Silver Layer (`silver_v2`)
Enrichment layer. Source: `bronze_v2`. Pipeline tag: `bronze_v2_to_silver_v2`.

**3 new fields added vs prior version:**

| Field | Logic |
|---|---|
| `country_mismatch_flag` | `txn_country ≠ billing_country` → 5.4% of transactions |
| `is_international` | `txn_country ≠ home_country` → 6.77% of transactions |
| `channel_risk_region` | 5-group regional classification for geographic risk scoring |

**Signals excluded (validated as noise):**
- `txn_country vs merchant_country` → 88% noise, not used
- `txn_country vs device_country` → 88% noise, not used
- `device_os` → excluded from analytics

All 8 validation queries passed before Gold layer was built.

### Gold Layer (`gold_v2`)
Star Schema with behavioral risk scores.

**Fact table:** `fact_transaction_risk_scores`  
**Dimensions:** `dim_customers` · `dim_merchants` · `dim_devices` · `dim_fx_rates`

**FK resolution pattern:** `account_id` pool mismatch resolved via bridge-key through `customer_id`. `vw_primary_accounts` enforces 1:1 cardinality (oldest account per customer). 21,913 secondary account rows confirmed legitimate and retained.

---

## 📊 Risk Scoring Model

Five components. Total score: **0–100 pts**.

| Component | Max Points | Key Logic |
|---|---|---|
| Velocity Risk | 25 | Transaction frequency vs customer baseline |
| Amount Anomaly | 20 | Ratio-based: 1.5× / 2× / 3× customer average |
| Merchant Risk | 20 | Category risk + chargeback rate thresholds |
| Geographic Risk | 20 | country_mismatch (+18pts), is_international (+12pts) |
| Device Risk | 15 | Multi-account device usage (+8pts) |

**Risk Bands:**

| Band | Score Range | Target Distribution |
|---|---|---|
| 🟢 Low | 0 – 25 | ~60% |
| 🟡 Medium | 26 – 50 | ~25% |
| 🔴 High | 51 – 75 | ~12% |
| ⛔ Critical | 76 – 100 | ~3% |

**Calibration note:** Initial build produced 90%+ Low scores with only 5 Critical across 1M transactions. Root cause: velocity near-zero (96.95% single daily transactions); amount anomaly using unstable std dev approach. Fixed by switching to ratio-based thresholds and recalibrating geographic and device component weights.

**Additional Gold layer fields:**
- `risk_percentile` — via `PERCENT_RANK()` window function
- `txn_amount_usd` — FX-normalized transaction value
- `recommended_action` — decisioned field: Monitor / Review / Escalate / Block

---

## 🗂 Gold Layer Views

11 analytical views built on top of `fact_transaction_risk_scores`.

| View | Purpose |
|---|---|
| `vw_risk_band_summary` | Distribution across Low / Medium / High / Critical |
| `vw_monthly_risk_trend` | MoM risk score movement |
| `vw_channel_country_risk` | Risk by channel × country group |
| `vw_country_mismatch_risk` | Mismatch flag analysis by geography |
| `vw_merchant_risk_profile` | Merchant category risk + chargeback rates |
| `vw_customer_risk_segments` | Customer-level risk aggregation |
| `vw_customer_tenure_risk` | Risk by account age / tenure band |
| `vw_aml_alert_summary` | AML signal aggregation by category |
| `vw_aml_underage_accounts` | Accounts with age < 18 at account open |
| `vw_aml_velocity_anomalies` | Customers with abnormal transaction frequency |
| `pipeline_runs` | Data freshness tracking for dashboard KPI |

---

## 📈 Dashboards

5 Tableau dashboards. Each targets a specific business stakeholder. Design rule: max 4 KPIs + 2–3 charts per page. Dark theme throughout.

| Page | Audience | Key KPIs | Core Charts |
|---|---|---|---|
| P1: Transaction Risk Overview | CRO | Total txns, Avg risk score, High+Critical %, Flagged volume (USD) | Risk band distribution, MoM risk trend |
| P2: Geographic Risk Intelligence | Regional Risk Manager | Country mismatch rate, Intl txn %, High-risk region count | Choropleth map, channel × region heatmap |
| P3: Customer Risk Profile | KYC Compliance Officer | High-risk customer %, Avg customer risk score, KYC gap count | Risk by income band, tenure × risk scatter |
| P4: Merchant Risk Intelligence | Merchant Risk / Card Scheme | High-risk merchant %, Avg chargeback rate, Flagged merchant count | Category risk bar, chargeback vs volume scatter |
| P5: AML & Compliance Intelligence | Financial Crime Director | AML alert count, Orphaned txn exposure (USD), Velocity anomaly count | AML category breakdown, recommended action donut |

**Global filters:** Date range · Country · Risk band · Transaction status  
**Drill-downs:** Country group → individual country · Merchant category → individual merchant · Year → Quarter → Month

---

## 🚨 AML & Compliance Signals

**80,439 transactions (8% of dataset)** reference `customer_id` values absent from the silver layer customer table. This is a documented data generation gap — not a pipeline error.

**Why it matters:**

| Pattern | Interpretation |
|---|---|
| Concentration in Gambling, Crypto, Offshore Services, Digital Goods | AML red flag categories per FATF guidelines |
| Transactions with no KYC-linked customer record | Synthetic identity fraud / money mule pattern |
| Retained in fact table with governance documentation | Correct enterprise pattern — exclusion would destroy the signal |

These transactions are surfaced in `vw_aml_alert_summary` and flagged with `recommended_action = 'Escalate'` in the Gold layer.

---

## 🔧 Key Engineering Decisions

| Decision | What | Why |
|---|---|---|
| Bridge-key pattern | customer_id as FK bridge when account_id pool mismatched | Standard enterprise resolution; account_id FK integrity was not guaranteed by synthetic generator |
| Ratio-based anomaly detection | 1.5×/2×/3× customer average instead of std dev | More stable and interpretable in sparse datasets; std dev approach produced near-zero variance |
| `vw_primary_accounts` | Oldest account per customer enforces 1:1 join cardinality | Prevents fan-out multiplication in Gold layer joins |
| Parallel bronze schemas | bronze_v2 separate from original bronze | Data governance — never modify production source; comparison always available |
| Channel bias injection | Region-specific channel probabilities in bronze_v2 | Uniform distribution is synthetic artifact; geographic risk signals require realistic channel skew |
| device_os exclusion | Not used in any analytical view | Profiling showed zero analytical signal; inclusion would add noise to device risk component |

---

## 🛠 Tech Stack

| Layer | Tool / Platform |
|---|---|
| Data Storage | AWS S3 (`eu-north-1`) |
| Data Warehouse | Snowflake (`BANKING_DB`, `BANKING_WH` Small, Auto-suspend 60s) |
| Pipeline Language | SQL (Advanced: CTEs, Window Functions, QUALIFY, PERCENT_RANK) |
| Data Generation | Python |
| Visualization | Tableau (connected to Snowflake) |
| Naming Convention | dbt-standard (bronze / silver / gold schemas) |
| Version Control | GitHub |

**Snowflake-specific patterns applied:**
- `QUALIFY` not supported inside `WHERE IN` subqueries → used `ROW_NUMBER` in inner subquery
- `MATCH_BY_COLUMN_NAME` requires `PARSE_HEADER = TRUE`
- CTE before `UPDATE` fails in Snowflake → resolved with nested subquery

---

## 📐 Project Scale

| Metric | Value |
|---|---|
| Total Transactions | 1,000,000 |
| Source Tables | 6 |
| Pipeline Layers | 3 (Bronze → Silver → Gold) |
| Risk Model Components | 5 |
| Gold Layer Views | 11 |
| Tableau Dashboards | 5 |
| AML-flagged Transactions | 80,439 (8%) |
| Country Mismatch Rate | 5.4% |
| International Transaction Rate | 6.77% |

---

## ⚙️ Setup & Replication

### Prerequisites
- Snowflake account (any tier)
- AWS S3 bucket with IAM read access configured in Snowflake
- Python 3.9+ (for data generation scripts)
- Tableau Desktop (for dashboard files)

### Steps

```bash
# 1. Clone the repo
git clone https://github.com/your-username/banking-fraud-risk-analytics.git

# 2. Generate synthetic dataset
python scripts/generate_data.py

# 3. Upload to S3
aws s3 cp data/ s3://your-bucket-name/ --recursive

# 4. Run Snowflake layers in order
#    → sql/bronze/bronze_v2_setup.sql
#    → sql/silver/silver_v4_calibrated.sql
#    → sql/gold/gold_v2_star_schema.sql
#    → sql/gold/gold_v2_views.sql

# 5. Connect Tableau to Snowflake and open workbook
#    → tableau/banking_fraud_dashboard.twbx
```

### Snowflake Environment
```sql
-- Required setup
CREATE DATABASE BANKING_DB;
CREATE WAREHOUSE BANKING_WH WITH WAREHOUSE_SIZE = 'SMALL' AUTO_SUSPEND = 60;
CREATE SCHEMA BANKING_DB.bronze_v2;
CREATE SCHEMA BANKING_DB.silver_v2;
CREATE SCHEMA BANKING_DB.gold_v2;
```

---

## 👤 Author

**Piyush Panthi**  
B.Tech — Artificial Intelligence Engineering | MITS Gwalior  
📧 piyushpanthiofficial@gmail.com · 🔗 [LinkedIn] · 💻 [GitHub]

> *Built as a portfolio project targeting Data Analyst roles at financial institutions (JPMC, Barclays, Amex, Goldman Sachs, Visa, Mastercard, Razorpay, Big 4).*

---

*Last updated: 2025 · Snowflake · Tableau · AWS · Python · SQL*
