# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Rules engine: tag every transaction FRAUD / REVIEW / GENUINE
# MAGIC
# MAGIC The rules live in a **table**, not in code: `business_vault.ref_fraud_rules`. Each rule is a SQL condition
# MAGIC over the feature columns, worth some points. Points add up to a 0–100 risk score:
# MAGIC
# MAGIC | Score | Bucket | Action |
# MAGIC |---|---|---|
# MAGIC | 70–100 | **FRAUD** | block and contact the cardholder |
# MAGIC | 40–69 | **REVIEW** | hold for a fraud analyst |
# MAGIC | 0–39 | **GENUINE** | approve |
# MAGIC
# MAGIC Because rules are data, an analyst can add or switch off a rule with one `INSERT` or `UPDATE`
# MAGIC and rerun this notebook. You will do exactly that in the student exercise.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS business_vault.ref_fraud_rules (
# MAGIC   rule_code STRING, description STRING, points INT, condition_sql STRING, suspected_type STRING, active BOOLEAN
# MAGIC ) USING DELTA;
# MAGIC
# MAGIC -- Seed the starter rules (insert-only, so rules you add later are kept)
# MAGIC MERGE INTO business_vault.ref_fraud_rules t
# MAGIC USING (SELECT * FROM VALUES
# MAGIC   ('R01_CARD_TESTING',     '3+ charges under $10 within 1 hour',                  40, 'small_txn_count_1h >= 3',                          'Card testing',       true),
# MAGIC   ('R02_VELOCITY',         '5+ transactions within 1 hour',                       30, 'txn_count_1h >= 5',                                'Card testing',       true),
# MAGIC   ('R03_IMPOSSIBLE_TRAVEL','swiped 500+ km away, faster than 900 km/h',           70, 'km_from_prev_swipe > 500 AND travel_speed_kmh > 900','Impossible travel', true),
# MAGIC   ('R04_AMOUNT_SPIKE',     'amount 5x+ usual spend for the card',                  25, 'amount_ratio >= 5',                                'Unusual spend',      true),
# MAGIC   ('R05_EXTREME_AMOUNT',   'amount 10x+ usual spend for the card',                 15, 'amount_ratio >= 10',                               'Unusual spend',      true),
# MAGIC   ('R06_NEW_DEVICE_HIGH',  'first purchase on this device and 3x+ usual spend',   30, 'is_new_device = 1 AND amount_ratio >= 3',          'Account takeover',   true),
# MAGIC   ('R07_HIGH_RISK_MCC',    'crypto, money transfer, gift cards or jewelry',       20, 'is_high_risk_mcc = 1',                             'High-risk merchant', true),
# MAGIC   ('R08_HIGH_RISK_LARGE',  'high-risk merchant and over $300',                    25, 'is_high_risk_mcc = 1 AND amount > 300',            'High-risk merchant', true),
# MAGIC   ('R09_FOREIGN_IP',       'online purchase from a non-US IP address',            25, 'is_foreign_ip = 1',                                'Account takeover',   true),
# MAGIC   ('R10_ODD_HOUR',         'midnight to 5am, far from usual hours for the card',   15, 'hour < 5 AND hour_deviation >= 6',                 'Account takeover',   true),
# MAGIC   ('R11_CNP_NEW_MERCHANT', 'card not present at a merchant never used before',    10, 'is_card_not_present = 1 AND is_new_merchant = 1',  'Unusual spend',      true)
# MAGIC   AS v(rule_code, description, points, condition_sql, suspected_type, active)) s
# MAGIC ON t.rule_code = s.rule_code
# MAGIC WHEN NOT MATCHED THEN INSERT *;
# MAGIC
# MAGIC SELECT * FROM business_vault.ref_fraud_rules ORDER BY rule_code;

# COMMAND ----------

# MAGIC %md
# MAGIC ### Build the scoring query from the rules table
# MAGIC Python reads the active rules and writes one SQL statement. It is printed so you can see exactly what runs.

# COMMAND ----------

rules = (spark.table("business_vault.ref_fraud_rules").filter("active")
         .orderBy("points", ascending=False).collect())

points = " +\n    ".join(f"CASE WHEN {r.condition_sql} THEN {r.points} ELSE 0 END" for r in rules)
reasons = ",\n    ".join(f"CASE WHEN {r.condition_sql} THEN '{r.rule_code}' END" for r in rules)
top_type = "\n    ".join(f"WHEN {r.condition_sql} THEN '{r.suspected_type}'" for r in rules)

scoring_sql = f"""
SELECT transaction_hk,
  LEAST(100, {points}) AS rule_score,
  concat_ws(', ', array({reasons})) AS rule_reasons,
  CASE {top_type}
    ELSE NULL END AS suspected_type
FROM business_vault.bsat_transaction_features"""
print(scoring_sql)

scored = spark.sql(f"""
SELECT *, CASE WHEN rule_score >= {FRAUD_THRESHOLD} THEN 'FRAUD'
               WHEN rule_score >= {REVIEW_THRESHOLD} THEN 'REVIEW'
               ELSE 'GENUINE' END AS rule_bucket,
       current_timestamp() AS load_dts
FROM ({scoring_sql})""")
(scored.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
       .saveAsTable("business_vault.bsat_transaction_rules"))

# COMMAND ----------

# MAGIC %md
# MAGIC ### How did the rules do? (graded against the chargeback feed)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT r.rule_bucket,
# MAGIC        count(*)                                          AS transactions,
# MAGIC        count(l.transaction_hk)                           AS confirmed_fraud,
# MAGIC        round(100 * count(l.transaction_hk) / count(*), 1) AS pct_fraud
# MAGIC FROM business_vault.bsat_transaction_rules r
# MAGIC LEFT JOIN raw_vault.sat_transaction_label l USING (transaction_hk)
# MAGIC GROUP BY r.rule_bucket
# MAGIC ORDER BY CASE r.rule_bucket WHEN 'FRAUD' THEN 1 WHEN 'REVIEW' THEN 2 ELSE 3 END

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Detection by fraud pattern: which patterns do the rules catch, and which slip through?
# MAGIC SELECT l.fraud_pattern,
# MAGIC        count(*) AS fraud_txns,
# MAGIC        sum(CASE WHEN r.rule_bucket <> 'GENUINE' THEN 1 ELSE 0 END) AS caught,
# MAGIC        round(100 * avg(CASE WHEN r.rule_bucket <> 'GENUINE' THEN 1 ELSE 0 END), 0) AS pct_caught
# MAGIC FROM raw_vault.sat_transaction_label l
# MAGIC JOIN business_vault.bsat_transaction_rules r USING (transaction_hk)
# MAGIC GROUP BY l.fraud_pattern
# MAGIC ORDER BY pct_caught
