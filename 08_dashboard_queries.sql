-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 08 · Fraud Review Report: dashboard queries
-- MAGIC
-- MAGIC Each cell below is one **dataset** for a Databricks **AI/BI Dashboard**, reading only from the gold layer.
-- MAGIC
-- MAGIC **Build the dashboard (about 10 minutes):**
-- MAGIC 1. Left sidebar → **Dashboards** → **Create dashboard**. Name it *Fraud Review Report*.
-- MAGIC 2. **Data** tab → **Create from SQL** for each query below (copy the cell, drop the `USE CATALOG` line
-- MAGIC    if your dashboard already points at the catalog). Give each dataset the name in its comment.
-- MAGIC 3. **Canvas** tab → add widgets:
-- MAGIC
-- MAGIC | Widget | Dataset | Visual |
-- MAGIC |---|---|---|
-- MAGIC | Headline counters | `kpi_totals` | 4 Counter widgets: transactions, blocked, in review, approved |
-- MAGIC | Fraud & review by type | `fraud_by_type` | Bar: x = suspected_type, y = alerts, color = decision |
-- MAGIC | Analyst review queue | `review_queue` | Table, sorted by risk_score |
-- MAGIC | Genuine section | `genuine_summary` | Counter (approved_txns) + bar by channel |
-- MAGIC | Daily trend | `daily_kpis` | Line: x = event_date, y = blocked, in_review |
-- MAGIC | Rules vs Rules + AI | `rules_vs_ai` | Table or grouped bar on detection_pct, review_queue |
-- MAGIC | Who is hit | `fraud_by_age` | Bar: x = age_band, y = fraud_per_1000_txns |
-- MAGIC
-- MAGIC 4. Add a **filter** on `decision` and a **date range** on `event_date`, then **Publish**.
-- MAGIC
-- MAGIC Power BI or Tableau users can connect to the same gold views through the SQL warehouse instead
-- MAGIC (*Partner Connect* or the warehouse's *Connection details*).

-- COMMAND ----------

-- MAGIC %run ./00_config

-- COMMAND ----------

-- dataset: kpi_totals
SELECT count(*)                                              AS transactions,
       sum(CASE WHEN final_bucket = 'FRAUD' THEN 1 ELSE 0 END)   AS blocked,
       sum(CASE WHEN final_bucket = 'REVIEW' THEN 1 ELSE 0 END)  AS in_review,
       sum(CASE WHEN final_bucket = 'GENUINE' THEN 1 ELSE 0 END) AS approved,
       round(sum(CASE WHEN final_bucket = 'FRAUD' THEN amount ELSE 0 END), 0) AS amount_blocked_usd
FROM gold.fact_transaction_decision

-- COMMAND ----------

-- dataset: fraud_by_type
SELECT suspected_type, decision, alerts, amount_usd, confirmed_fraud
FROM gold.v_fraud_by_type
ORDER BY alerts DESC

-- COMMAND ----------

-- dataset: review_queue
SELECT decision, transaction_id, event_ts, customer_name, merchant_name, amount, suspected_type,
       risk_score, coalesce(analyst_note, rule_reasons) AS why_flagged, decided_by
FROM gold.v_review_report
ORDER BY risk_score DESC, event_ts DESC

-- COMMAND ----------

-- dataset: genuine_summary
SELECT channel, approved_txns, amount_usd, fraud_missed
FROM gold.v_genuine_summary

-- COMMAND ----------

-- dataset: daily_kpis
SELECT * FROM gold.v_daily_kpis ORDER BY event_date

-- COMMAND ----------

-- dataset: rules_vs_ai
SELECT * FROM gold.v_rules_vs_ai

-- COMMAND ----------

-- dataset: fraud_by_age
SELECT * FROM gold.v_fraud_by_age_band ORDER BY age_band
