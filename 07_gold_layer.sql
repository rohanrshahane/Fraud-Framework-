-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 07 · Gold: star schema and report views
-- MAGIC
-- MAGIC Gold turns the vault into something a report can read directly:
-- MAGIC
-- MAGIC * **Dimensions:** `dim_customer`, `dim_card`, `dim_merchant` (current view of each satellite)
-- MAGIC * **Fact:** `fact_transaction_decision` – one row per transaction with its features, rule score,
-- MAGIC   AI score and the **final decision**
-- MAGIC * **Report views** that the dashboard reads:
-- MAGIC
-- MAGIC | View | Report section |
-- MAGIC |---|---|
-- MAGIC | `v_review_report` | the analyst queue: every FRAUD and REVIEW alert with reasons and note |
-- MAGIC | `v_fraud_by_type` | alerts by suspected fraud type |
-- MAGIC | `v_genuine_summary` | the genuine section: approved transactions |
-- MAGIC | `v_daily_kpis` | daily volume, alerts and dollars held |
-- MAGIC | `v_rules_vs_ai` | detection, false blocks and queue size, rules vs rules + AI |
-- MAGIC | `v_fraud_by_age_band` | who is being hit, by customer age band |
-- MAGIC | `v_detection_by_pattern` | fraud caught per pattern, rules vs rules + AI |
-- MAGIC | `v_ai_outcomes` | what changed when AI was added (caught only with AI, false alarms removed, …) |
-- MAGIC | `v_ai_wins` | fraud the rules missed and AI caught, with the analyst note |

-- COMMAND ----------

-- MAGIC %run ./00_config

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.dim_customer AS
SELECT h.customer_id, s.first_name, s.last_name, concat(s.first_name, ' ', s.last_name) AS customer_name,
       s.age_band, s.home_city
FROM raw_vault.hub_customer h
JOIN (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY customer_hk ORDER BY load_dts DESC) AS rn
                     FROM raw_vault.sat_customer) WHERE rn = 1) s USING (customer_hk);

CREATE OR REPLACE TABLE gold.dim_card AS
SELECT hc.card_id, hcu.customer_id, s.card_type, s.issued_date
FROM raw_vault.hub_card hc
LEFT JOIN raw_vault.link_card_customer l ON l.card_hk = hc.card_hk
LEFT JOIN raw_vault.hub_customer hcu ON hcu.customer_hk = l.customer_hk
LEFT JOIN (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY card_hk ORDER BY load_dts DESC) AS rn
                          FROM raw_vault.sat_card) WHERE rn = 1) s ON s.card_hk = hc.card_hk;

CREATE OR REPLACE TABLE gold.dim_merchant AS
SELECT h.merchant_id, s.merchant_name, s.merchant_category, s.risk_tier
FROM raw_vault.hub_merchant h
JOIN (SELECT * FROM (SELECT *, row_number() OVER (PARTITION BY merchant_hk ORDER BY load_dts DESC) AS rn
                     FROM raw_vault.sat_merchant) WHERE rn = 1) s USING (merchant_hk);

-- COMMAND ----------

CREATE OR REPLACE TABLE gold.fact_transaction_decision AS
SELECT ht.transaction_id,
       st.event_ts,
       to_date(st.event_ts)                                 AS event_date,
       hc.card_id,
       dc.customer_id,
       hm.merchant_id,
       st.amount, st.channel, st.txn_city, st.ip_country,
       f.amount_ratio, f.txn_count_1h, f.small_txn_count_1h, f.km_from_prev_swipe, f.is_new_device, f.new_merchants_24h,
       r.rule_score, r.rule_bucket, r.rule_reasons,
       coalesce(r.suspected_type, CASE WHEN a.hybrid_bucket <> 'GENUINE' THEN 'Unusual behaviour (AI)' END) AS suspected_type,
       a.ml_probability, a.anomaly_pct, a.hybrid_score, a.hybrid_bucket, a.ai_drivers, a.analyst_note,
       coalesce(a.hybrid_bucket, r.rule_bucket)             AS final_bucket,
       CASE WHEN a.hybrid_bucket IS NOT NULL THEN 'Rules + AI' ELSE 'Rules' END AS decided_by,
       CASE WHEN lab.transaction_hk IS NOT NULL THEN 1 ELSE 0 END AS confirmed_fraud,
       lab.fraud_pattern                                    AS confirmed_pattern,
       round(coalesce(a.hybrid_score, r.rule_score), 1)     AS risk_score,
       CASE WHEN a.hybrid_bucket IS NOT NULL THEN 1 ELSE 0 END AS in_ai_test_window,
       -- What changed when AI was added (only for transactions the AI scored)
       CASE
         WHEN a.hybrid_bucket IS NULL THEN NULL
         WHEN lab.transaction_hk IS NOT NULL AND r.rule_bucket <> 'GENUINE' AND a.hybrid_bucket <> 'GENUINE' THEN 'Fraud caught by both'
         WHEN lab.transaction_hk IS NOT NULL AND r.rule_bucket =  'GENUINE' AND a.hybrid_bucket <> 'GENUINE' THEN 'Fraud caught only with AI'
         WHEN lab.transaction_hk IS NOT NULL AND r.rule_bucket <> 'GENUINE' AND a.hybrid_bucket =  'GENUINE' THEN 'Fraud caught only by rules'
         WHEN lab.transaction_hk IS NOT NULL                                                                 THEN 'Fraud missed by both'
         WHEN r.rule_bucket <> 'GENUINE' AND a.hybrid_bucket =  'GENUINE'                                     THEN 'False alarm removed by AI'
         WHEN r.rule_bucket =  'GENUINE' AND a.hybrid_bucket <> 'GENUINE'                                     THEN 'New false alarm from AI'
         WHEN r.rule_bucket <> 'GENUINE' AND a.hybrid_bucket <> 'GENUINE'                                     THEN 'False alarm in both'
         ELSE 'Genuine, approved by both'
       END                                                  AS ai_outcome
FROM raw_vault.hub_transaction ht
JOIN raw_vault.sat_transaction st             ON st.transaction_hk = ht.transaction_hk
JOIN raw_vault.link_transaction lt            ON lt.transaction_hk = ht.transaction_hk
JOIN raw_vault.hub_card hc                    ON hc.card_hk = lt.card_hk
JOIN raw_vault.hub_merchant hm                ON hm.merchant_hk = lt.merchant_hk
LEFT JOIN gold.dim_card dc                    ON dc.card_id = hc.card_id
JOIN business_vault.bsat_transaction_features f ON f.transaction_hk = ht.transaction_hk
JOIN business_vault.bsat_transaction_rules r    ON r.transaction_hk = ht.transaction_hk
LEFT JOIN business_vault.bsat_transaction_ai_score a ON a.transaction_hk = ht.transaction_hk
LEFT JOIN raw_vault.sat_transaction_label lab ON lab.transaction_hk = ht.transaction_hk;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Report views

-- COMMAND ----------

CREATE OR REPLACE VIEW gold.v_review_report AS
SELECT f.final_bucket AS decision, f.transaction_id, f.event_ts, c.customer_name, c.age_band, f.card_id,
       m.merchant_name, m.merchant_category, f.amount, f.channel, f.txn_city,
       f.suspected_type, coalesce(f.hybrid_score, f.rule_score) AS risk_score, f.rule_reasons, f.ai_drivers,
       f.analyst_note, f.decided_by, f.confirmed_fraud
FROM gold.fact_transaction_decision f
LEFT JOIN gold.dim_customer c USING (customer_id)
LEFT JOIN gold.dim_merchant m USING (merchant_id)
WHERE f.final_bucket IN ('FRAUD', 'REVIEW');

CREATE OR REPLACE VIEW gold.v_fraud_by_type AS
SELECT coalesce(suspected_type, 'Other') AS suspected_type, final_bucket AS decision,
       count(*) AS alerts, round(sum(amount), 2) AS amount_usd, sum(confirmed_fraud) AS confirmed_fraud
FROM gold.fact_transaction_decision
WHERE final_bucket IN ('FRAUD', 'REVIEW')
GROUP BY 1, 2;

CREATE OR REPLACE VIEW gold.v_genuine_summary AS
SELECT channel, count(*) AS approved_txns, round(sum(amount), 2) AS amount_usd,
       sum(confirmed_fraud) AS fraud_missed
FROM gold.fact_transaction_decision
WHERE final_bucket = 'GENUINE'
GROUP BY channel;

CREATE OR REPLACE VIEW gold.v_daily_kpis AS
SELECT event_date,
       count(*)                                                      AS transactions,
       sum(CASE WHEN final_bucket = 'FRAUD' THEN 1 ELSE 0 END)       AS blocked,
       sum(CASE WHEN final_bucket = 'REVIEW' THEN 1 ELSE 0 END)      AS in_review,
       sum(CASE WHEN final_bucket = 'GENUINE' THEN 1 ELSE 0 END)     AS approved,
       round(sum(CASE WHEN final_bucket = 'FRAUD' THEN amount ELSE 0 END), 2) AS amount_blocked_usd,
       max(decided_by)                                               AS decided_by
FROM gold.fact_transaction_decision
GROUP BY event_date;

CREATE OR REPLACE VIEW gold.v_rules_vs_ai AS
SELECT approach,
       sum(confirmed_fraud)                                                              AS actual_fraud,
       sum(CASE WHEN confirmed_fraud = 1 AND bucket <> 'GENUINE' THEN 1 ELSE 0 END)      AS fraud_caught,
       sum(CASE WHEN confirmed_fraud = 1 AND bucket = 'GENUINE' THEN 1 ELSE 0 END)       AS fraud_missed,
       round(100 * sum(CASE WHEN confirmed_fraud = 1 AND bucket <> 'GENUINE' THEN 1 ELSE 0 END)
             / nullif(sum(confirmed_fraud), 0), 1)                                       AS detection_pct,
       sum(CASE WHEN confirmed_fraud = 0 AND bucket = 'FRAUD' THEN 1 ELSE 0 END)         AS genuine_blocked,
       sum(CASE WHEN bucket = 'REVIEW' THEN 1 ELSE 0 END)                                AS review_queue
FROM (SELECT 'Rules' AS approach, rule_bucket AS bucket, confirmed_fraud
      FROM gold.fact_transaction_decision WHERE hybrid_bucket IS NOT NULL
      UNION ALL
      SELECT 'Rules + AI', hybrid_bucket, confirmed_fraud
      FROM gold.fact_transaction_decision WHERE hybrid_bucket IS NOT NULL)
GROUP BY approach;

CREATE OR REPLACE VIEW gold.v_fraud_by_age_band AS
SELECT c.age_band,
       count(*)                                          AS transactions,
       sum(f.confirmed_fraud)                            AS confirmed_fraud,
       round(1000 * sum(f.confirmed_fraud) / count(*), 2) AS fraud_per_1000_txns,
       round(sum(CASE WHEN f.confirmed_fraud = 1 THEN f.amount ELSE 0 END), 2) AS fraud_amount_usd
FROM gold.fact_transaction_decision f
JOIN gold.dim_customer c USING (customer_id)
GROUP BY c.age_band;

CREATE OR REPLACE VIEW gold.v_detection_by_pattern AS
SELECT confirmed_pattern AS fraud_pattern,
       count(*)                                                        AS fraud_txns,
       sum(CASE WHEN rule_bucket   <> 'GENUINE' THEN 1 ELSE 0 END)     AS caught_by_rules,
       sum(CASE WHEN hybrid_bucket <> 'GENUINE' THEN 1 ELSE 0 END)     AS caught_with_ai,
       round(100 * avg(CASE WHEN rule_bucket   <> 'GENUINE' THEN 1 ELSE 0 END), 1) AS rules_pct,
       round(100 * avg(CASE WHEN hybrid_bucket <> 'GENUINE' THEN 1 ELSE 0 END), 1) AS ai_pct
FROM gold.fact_transaction_decision
WHERE in_ai_test_window = 1 AND confirmed_fraud = 1
GROUP BY confirmed_pattern;

CREATE OR REPLACE VIEW gold.v_ai_outcomes AS
SELECT ai_outcome, count(*) AS transactions, round(sum(amount), 2) AS amount_usd
FROM gold.fact_transaction_decision
WHERE in_ai_test_window = 1
GROUP BY ai_outcome;

CREATE OR REPLACE VIEW gold.v_ai_wins AS
SELECT f.transaction_id, f.event_ts, c.customer_name, m.merchant_name, m.merchant_category, f.amount,
       f.confirmed_pattern, f.rule_score, f.rule_reasons, f.hybrid_score, f.hybrid_bucket, f.ai_drivers, f.analyst_note
FROM gold.fact_transaction_decision f
LEFT JOIN gold.dim_customer c USING (customer_id)
LEFT JOIN gold.dim_merchant m USING (merchant_id)
WHERE f.ai_outcome = 'Fraud caught only with AI';

-- COMMAND ----------

SELECT * FROM gold.v_rules_vs_ai;

-- COMMAND ----------

SELECT * FROM gold.v_fraud_by_type ORDER BY alerts DESC;
