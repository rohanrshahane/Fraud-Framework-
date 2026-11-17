# Databricks notebook source
# MAGIC %md
# MAGIC # 10 · Exercise solutions (instructor copy)
# MAGIC
# MAGIC The bank's framework is live. Two more days of transactions are about to arrive, analysts say a
# MAGIC "quiet shopping spree" pattern keeps slipping through, and the audit team wants devices modelled in the vault.
# MAGIC
# MAGIC | # | Task | Skill | Time |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Stream in two new days of events | Streaming, Auto Loader | 10 min |
# MAGIC | 2 | Add a rule that catches the shopping spree | SQL, rules engine | 15 min |
# MAGIC | 3 | Model devices as a hub and a link | Data Vault | 15 min |
# MAGIC | 4 | Build a merchant-risk report view | Gold layer, SQL | 10 min |
# MAGIC | ★ | Bonus: tune the AI hybrid | PySpark, ML | open |
# MAGIC
# MAGIC Completed version of notebook 09. Keep it out of the students' folder.
# MAGIC Prerequisite: the instructor has already run `01_generate_events` (days 1–30) and `00_run_pipeline`.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

def check(name, ok, detail=""):
    print(("✅ PASS  " if ok else "❌ NOT YET  ") + name + (f"  ({detail})" if detail else ""))

def subtle_stats():
    return spark.sql("""
      SELECT sum(CASE WHEN l.fraud_pattern = 'subtle_fraud' THEN 1 ELSE 0 END)                               AS subtle_total,
             sum(CASE WHEN l.fraud_pattern = 'subtle_fraud' AND r.rule_bucket <> 'GENUINE' THEN 1 ELSE 0 END) AS subtle_caught,
             sum(CASE WHEN l.transaction_hk IS NULL AND r.rule_bucket <> 'GENUINE' THEN 1 ELSE 0 END)         AS false_alarms
      FROM business_vault.bsat_transaction_rules r
      LEFT JOIN raw_vault.sat_transaction_label l USING (transaction_hk)""").first()

before_bronze = spark.table("bronze.transactions").count()
print(f"Starting point: {before_bronze:,} events in bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 1 · Stream in two new days (Nov 1 is day 32)
# MAGIC Drop the files for **days 31 and 32**, then rerun the pipeline. Watch the bronze notebook's output:
# MAGIC how many events does the stream pick up?
# MAGIC
# MAGIC Days 31 and 32.

# COMMAND ----------

# MAGIC %run ./01_generate_events $emit_from_day="31" $emit_to_day="32"

# COMMAND ----------

# MAGIC %run ./00_run_pipeline

# COMMAND ----------

# ✅ Check — Task 1
new_days = spark.sql("""SELECT to_date(event_ts) AS d, count(*) AS n FROM bronze.transactions
                        WHERE event_ts >= '2026-10-31' GROUP BY 1 ORDER BY 1""").collect()
after_bronze = spark.table("bronze.transactions").count()
check("Oct 31 and Nov 1 landed in bronze", len(new_days) == 2, ", ".join(f"{r.d}: {r.n:,}" for r in new_days))
check("Only the new files were ingested (no duplicates)",
      spark.table("bronze.transactions").select("transaction_id").distinct().count() == after_bronze,
      f"{after_bronze - before_bronze:,} new events")
baseline = subtle_stats()
print(f"\nBaseline for Task 2: rules catch {baseline.subtle_caught} of {baseline.subtle_total} subtle frauds, "
      f"with {baseline.false_alarms:,} genuine customers flagged")

# COMMAND ----------

# MAGIC %md
# MAGIC **Discuss:** the stream ran over the whole folder again, so why didn't it reload October?
# MAGIC *(Hint: look in the `_checkpoints` folder of the volume.)*
# MAGIC
# MAGIC ## Task 2 · Add a rule for the shopping spree
# MAGIC Subtle fraud looks like this: **two or more new shops within 24 hours**, each purchase **2x or more** the card's usual spend.
# MAGIC Every rule is a row in `business_vault.ref_fraud_rules`, and its condition can use any column of
# MAGIC `business_vault.bsat_transaction_features` (`new_merchants_24h`, `amount_ratio`, …).
# MAGIC
# MAGIC Choose the points (how strong is the signal on its own?) and write the condition.

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO business_vault.ref_fraud_rules t
# MAGIC USING (SELECT 'R12_SHOPPING_SPREE'                    AS rule_code,
# MAGIC               '2+ new shops in 24h, 2x+ usual spend'  AS description,
# MAGIC               30                                      AS points,
# MAGIC               'new_merchants_24h >= 2 AND amount_ratio >= 2' AS condition_sql,
# MAGIC               'Subtle fraud'                          AS suspected_type,
# MAGIC               true                                    AS active) s
# MAGIC ON t.rule_code = s.rule_code
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %run ./05_rules_engine

# COMMAND ----------

# MAGIC %run ./07_gold_layer

# COMMAND ----------

# ✅ Check — Task 2
rule = spark.sql("SELECT * FROM business_vault.ref_fraud_rules WHERE rule_code = 'R12_SHOPPING_SPREE'").first()
now = subtle_stats()
check("Rule R12_SHOPPING_SPREE exists and is active", rule is not None and rule.active)
check("Rules now catch more subtle fraud", now.subtle_caught > baseline.subtle_caught,
      f"{baseline.subtle_caught} -> {now.subtle_caught} of {now.subtle_total}")
print(f"Cost: genuine customers flagged {baseline.false_alarms:,} -> {now.false_alarms:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC **Discuss:** how many extra genuine customers did your rule flag for every extra fraud it caught?
# MAGIC Try 20, 30 and 40 points. Which would you ship, and why?
# MAGIC
# MAGIC ## Task 3 · Model devices in the Data Vault
# MAGIC A device (phone, laptop) is a business object in its own right: one device can touch many cards, which is
# MAGIC exactly what a fraud ring looks like. Model it:
# MAGIC * `raw_vault.hub_device` – one row per `device_id`
# MAGIC * `raw_vault.link_transaction_device` – which device made which transaction
# MAGIC
# MAGIC The tables are created for you. Write the two loads, copying the pattern from `hub_merchant` and
# MAGIC `link_transaction` in notebook 03. Only online transactions have a device (`device_id IS NOT NULL`).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS raw_vault.hub_device (device_hk STRING, device_id STRING, load_dts TIMESTAMP, record_source STRING) USING DELTA;
# MAGIC CREATE TABLE IF NOT EXISTS raw_vault.link_transaction_device (transaction_device_hk STRING, transaction_hk STRING, device_hk STRING, load_dts TIMESTAMP, record_source STRING) USING DELTA;

# COMMAND ----------

# MAGIC %sql
# MAGIC -- 3a: hub_device
# MAGIC MERGE INTO raw_vault.hub_device t
# MAGIC USING (SELECT DISTINCT sha2(upper(trim(device_id)), 256) AS device_hk, device_id
# MAGIC        FROM bronze.transactions WHERE device_id IS NOT NULL) s
# MAGIC ON t.device_hk = s.device_hk
# MAGIC WHEN NOT MATCHED THEN INSERT (device_hk, device_id, load_dts, record_source)
# MAGIC   VALUES (s.device_hk, s.device_id, current_timestamp(), 'bronze.transactions');
# MAGIC
# MAGIC -- 3b: link_transaction_device
# MAGIC MERGE INTO raw_vault.link_transaction_device t
# MAGIC USING (SELECT DISTINCT
# MAGIC          sha2(concat_ws('||', upper(trim(transaction_id)), upper(trim(device_id))), 256) AS transaction_device_hk,
# MAGIC          sha2(upper(trim(transaction_id)), 256) AS transaction_hk,
# MAGIC          sha2(upper(trim(device_id)), 256) AS device_hk
# MAGIC        FROM bronze.transactions WHERE device_id IS NOT NULL) s
# MAGIC ON t.transaction_device_hk = s.transaction_device_hk
# MAGIC WHEN NOT MATCHED THEN INSERT (transaction_device_hk, transaction_hk, device_hk, load_dts, record_source)
# MAGIC   VALUES (s.transaction_device_hk, s.transaction_hk, s.device_hk, current_timestamp(), 'bronze.transactions')

# COMMAND ----------

# ✅ Check — Task 3
devices = spark.sql("SELECT count(DISTINCT device_id) AS n FROM bronze.transactions WHERE device_id IS NOT NULL").first().n
online = spark.sql("SELECT count(DISTINCT transaction_id) AS n FROM bronze.transactions WHERE device_id IS NOT NULL").first().n
hub = spark.table("raw_vault.hub_device")
link = spark.table("raw_vault.link_transaction_device")
check("hub_device has one row per device", hub.count() == devices == hub.select("device_hk").distinct().count(),
      f"{hub.count():,} rows, {devices:,} devices")
check("link_transaction_device has one row per online transaction", link.count() == online, f"{link.count():,} rows")
check("Every link points at a hub_device row", link.join(hub, "device_hk", "left_anti").count() == 0)
print("Now rerun the load cell: the counts must not change. That is what idempotent means.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Task 4 · Merchant-risk report view
# MAGIC Merchant onboarding wants a gold view: for every merchant, how many transactions, how many alerts
# MAGIC (`final_bucket` FRAUD or REVIEW), how much confirmed fraud, and the **confirmed fraud rate per 1,000 transactions**.
# MAGIC Use `gold.fact_transaction_decision` joined to `gold.dim_merchant`. Then add it to the dashboard as a table.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW gold.v_merchant_risk AS
# MAGIC SELECT m.merchant_name,
# MAGIC        m.merchant_category,
# MAGIC        count(*)                                                                AS transactions,
# MAGIC        sum(CASE WHEN f.final_bucket IN ('FRAUD', 'REVIEW') THEN 1 ELSE 0 END)  AS alerts,
# MAGIC        sum(f.confirmed_fraud)                                                  AS confirmed_fraud,
# MAGIC        round(1000 * sum(f.confirmed_fraud) / count(*), 2)                      AS fraud_per_1000_txns
# MAGIC FROM gold.fact_transaction_decision f
# MAGIC JOIN gold.dim_merchant m USING (merchant_id)
# MAGIC GROUP BY m.merchant_name, m.merchant_category

# COMMAND ----------

# ✅ Check — Task 4
v = spark.table("gold.v_merchant_risk")
need = {"merchant_name", "transactions", "alerts", "confirmed_fraud", "fraud_per_1000_txns"}
check("View has the required columns", need <= set(v.columns), ", ".join(sorted(need - set(v.columns))) or "")
check("One row per merchant", v.count() == spark.table("gold.dim_merchant").count(), f"{v.count()} rows")
check("Totals match the fact table",
      v.agg({"transactions": "sum"}).first()[0] == spark.table("gold.fact_transaction_decision").count())
display(v.orderBy("fraud_per_1000_txns", ascending=False).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## ★ Bonus · Tune the AI hybrid
# MAGIC Open notebook **06_ai_scoring** and change the hybrid weights (`0.80` ML, `0.15` rules) or the thresholds in
# MAGIC **00_config**. Rerun `06_ai_scoring` and `07_gold_layer`, then compare:

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM gold.v_rules_vs_ai

# COMMAND ----------

# MAGIC %md
# MAGIC ## Wrap-up questions
# MAGIC 1. Your new rule caught more fraud but flagged more genuine customers. Who pays for each kind of mistake?
# MAGIC 2. Why does the vault keep every version of a merchant instead of updating the row?
# MAGIC 3. The AI model was trained on October. What would you monitor to know when it needs retraining?
