# Databricks notebook source
# MAGIC %md
# MAGIC # 06 · AI infusion: ML scoring, anomaly detection and explanations
# MAGIC
# MAGIC Three AI layers plug into the same framework and reuse the same features:
# MAGIC
# MAGIC 1. **Supervised ML** – a gradient-boosted model learns fraud from labelled history (the chargeback feed)
# MAGIC    and weighs many weak signals together.
# MAGIC 2. **Anomaly detection** – an Isolation Forest learns what *normal* looks like and flags the unfamiliar,
# MAGIC    with no labels needed.
# MAGIC 3. **Explanations** – every alert gets its top drivers and a plain-English analyst note.
# MAGIC
# MAGIC **Hybrid score** = 80% ML + 15% rules + an anomaly nudge; impossible travel stays a hard block.
# MAGIC The model trains on events **before** `AI_CUTOFF_DATE` and scores everything after it, which is how it
# MAGIC would score tomorrow's transactions. Runs are tracked in **MLflow**.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import numpy as np
import pandas as pd
from sklearn.ensemble import HistGradientBoostingClassifier, IsolationForest
from sklearn.metrics import average_precision_score

ML_FEATURES = ["amount", "amount_ratio", "hour", "hour_deviation", "txn_count_10m", "txn_count_1h", "txn_count_24h",
               "small_txn_count_1h", "spend_24h_ratio", "km_from_prev_swipe", "travel_speed_kmh", "is_new_device",
               "is_new_merchant", "new_merchants_24h", "is_high_risk_mcc", "is_card_not_present", "is_foreign_ip",
               "history_depth"]
FRIENDLY = {"amount": "dollar amount", "amount_ratio": "amount vs usual spend", "hour": "time of day",
            "hour_deviation": "unusual hour", "txn_count_10m": "burst in last 10 min", "txn_count_1h": "txns in last hour",
            "txn_count_24h": "txns in last 24h", "small_txn_count_1h": "tiny test charges",
            "spend_24h_ratio": "24h spend vs usual", "km_from_prev_swipe": "distance from last swipe",
            "travel_speed_kmh": "implied travel speed", "is_new_device": "new device", "is_new_merchant": "new merchant",
            "new_merchants_24h": "new shops in last 24h", "is_high_risk_mcc": "high-risk merchant",
            "is_card_not_present": "card not present", "is_foreign_ip": "foreign IP", "history_depth": "card history"}

pdf = spark.sql("""
SELECT f.*, r.rule_score, r.rule_reasons, r.rule_bucket,
       CASE WHEN l.transaction_hk IS NOT NULL THEN 1 ELSE 0 END AS is_fraud
FROM business_vault.bsat_transaction_features f
JOIN business_vault.bsat_transaction_rules r USING (transaction_hk)
LEFT JOIN raw_vault.sat_transaction_label l USING (transaction_hk)
""").toPandas()
# Spark returns rows in no fixed order; sort so training (and every number downstream) is repeatable run to run
pdf = pdf.sort_values(["event_ts", "transaction_hk"]).reset_index(drop=True)

cutoff = pd.Timestamp(AI_CUTOFF_DATE)
train, test = pdf[pdf.event_ts < cutoff].copy(), pdf[pdf.event_ts >= cutoff].copy()
print(f"Train: {len(train):,} txns ({train.is_fraud.sum()} fraud) before {AI_CUTOFF_DATE} | "
      f"Score: {len(test):,} txns on/after it")
assert len(test) > 0, "No transactions after the cutoff yet - emit more days in notebook 01."

# COMMAND ----------

# 1. Supervised model
w = np.sqrt(np.where(train.is_fraud == 1, (train.is_fraud == 0).sum() / max(train.is_fraud.sum(), 1), 1.0))
model = HistGradientBoostingClassifier(max_iter=300, learning_rate=0.05, max_leaf_nodes=31,
                                       l2_regularization=1.0, random_state=7)
model.fit(train[ML_FEATURES], train.is_fraud, sample_weight=w)
test["ml_prob"] = model.predict_proba(test[ML_FEATURES])[:, 1]

# 2. Anomaly detector trained on genuine history only
iso = IsolationForest(n_estimators=200, random_state=7).fit(train.loc[train.is_fraud == 0, ML_FEATURES])
ref = np.sort(-iso.score_samples(train[ML_FEATURES]))
test["anomaly_pct"] = np.searchsorted(ref, -iso.score_samples(test[ML_FEATURES])) / len(ref)

# 3. Hybrid decision
hybrid = 0.80 * test.ml_prob * 100 + 0.15 * test.rule_score + 5 * (test.anomaly_pct > 0.995)
hard_stop = test.rule_reasons.fillna("").str.contains("R03_IMPOSSIBLE_TRAVEL")
test["hybrid_score"] = np.where(hard_stop, np.maximum(hybrid, FRAUD_THRESHOLD), hybrid).clip(0, 100).round(1)
test["hybrid_bucket"] = np.where(test.hybrid_score >= FRAUD_THRESHOLD, "FRAUD",
                                 np.where(test.hybrid_score >= REVIEW_THRESHOLD, "REVIEW", "GENUINE"))

ap_ml = average_precision_score(test.is_fraud, test.ml_prob) if test.is_fraud.any() else float("nan")
ap_rules = average_precision_score(test.is_fraud, test.rule_score) if test.is_fraud.any() else float("nan")
print(f"Average precision on unseen days: ML {ap_ml:.3f} vs rules {ap_rules:.3f}")

# COMMAND ----------

# 4. Explanations: for each alert, which signals push the score up most?
baseline = train.loc[train.is_fraud == 0, ML_FEATURES].median()
alerts = test.hybrid_bucket != "GENUINE"
X = test.loc[alerts, ML_FEATURES]
base_p = model.predict_proba(X)[:, 1]
contrib = pd.DataFrame({c: base_p - model.predict_proba(X.assign(**{c: baseline[c]}))[:, 1] for c in ML_FEATURES},
                       index=X.index)
test["ai_drivers"] = None
test.loc[alerts, "ai_drivers"] = contrib.apply(
    lambda r: ", ".join(FRIENDLY[c] for c in r.sort_values(ascending=False).index[:3] if r[c] > 0.02), axis=1)


def note(r):
    if r.hybrid_bucket == "GENUINE":
        return None
    bits = []
    if r.amount_ratio >= 2:
        bits.append(f"${r.amount:,.2f} is {r.amount_ratio:.1f}x usual spend")
    if r.small_txn_count_1h >= 3:
        bits.append(f"{int(r.small_txn_count_1h)} tiny charges in the past hour")
    if r.km_from_prev_swipe > 500 and r.travel_speed_kmh > 900:
        bits.append(f"swiped {r.km_from_prev_swipe:,.0f} km away shortly before")
    if r.is_new_device:
        bits.append("first purchase from this device")
    if r.is_foreign_ip:
        bits.append("online session from abroad")
    if r.is_high_risk_mcc:
        bits.append("high-risk merchant type")
    if r.new_merchants_24h >= 2:
        bits.append(f"{int(r.new_merchants_24h)} new shops in 24h")
    action = "Block and contact cardholder." if r.hybrid_bucket == "FRAUD" else "Hold for analyst review."
    text = "; ".join(bits)
    return (text[:1].upper() + text[1:] + ". " if text else "") + action


test["analyst_note"] = test.apply(note, axis=1)

# COMMAND ----------

# 5. Track the run in MLflow (skipped quietly where MLflow is not available)
try:
    import mlflow
    with mlflow.start_run(run_name="fraud_hybrid_model"):
        mlflow.log_params({"model": "HistGradientBoosting", "cutoff": AI_CUTOFF_DATE, "features": len(ML_FEATURES)})
        mlflow.log_metrics({"avg_precision_ml": float(ap_ml), "avg_precision_rules": float(ap_rules)})
        mlflow.sklearn.log_model(model, "model", input_example=train[ML_FEATURES].head(5))
    print("Logged to MLflow")
except Exception as e:
    print(f"MLflow logging skipped: {type(e).__name__}: {e}")

# COMMAND ----------

out = test[["transaction_hk", "ml_prob", "anomaly_pct", "hybrid_score", "hybrid_bucket", "ai_drivers", "analyst_note"]]
schema = ("transaction_hk string, ml_prob double, anomaly_pct double, hybrid_score double, hybrid_bucket string, "
          "ai_drivers string, analyst_note string")
(spark.createDataFrame(out, schema=schema).withColumnRenamed("ml_prob", "ml_probability")
 .selectExpr("*", "current_timestamp() AS load_dts")
 .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
 .saveAsTable("business_vault.bsat_transaction_ai_score"))
print(f"business_vault.bsat_transaction_ai_score: {len(out):,} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Rules vs Rules + AI on the same unseen days
# MAGIC SELECT approach,
# MAGIC        sum(is_fraud)                                                    AS actual_fraud,
# MAGIC        sum(CASE WHEN is_fraud = 1 AND bucket <> 'GENUINE' THEN 1 END)   AS fraud_caught,
# MAGIC        round(100 * sum(CASE WHEN is_fraud = 1 AND bucket <> 'GENUINE' THEN 1 ELSE 0 END) / sum(is_fraud), 1) AS detection_pct,
# MAGIC        sum(CASE WHEN is_fraud = 0 AND bucket = 'FRAUD' THEN 1 ELSE 0 END)  AS genuine_blocked,
# MAGIC        sum(CASE WHEN bucket = 'REVIEW' THEN 1 ELSE 0 END)                 AS review_queue
# MAGIC FROM (
# MAGIC   SELECT 'Rules' AS approach, r.rule_bucket AS bucket, CASE WHEN l.transaction_hk IS NULL THEN 0 ELSE 1 END AS is_fraud
# MAGIC   FROM business_vault.bsat_transaction_ai_score a
# MAGIC   JOIN business_vault.bsat_transaction_rules r USING (transaction_hk)
# MAGIC   LEFT JOIN raw_vault.sat_transaction_label l USING (transaction_hk)
# MAGIC   UNION ALL
# MAGIC   SELECT 'Rules + AI', a.hybrid_bucket, CASE WHEN l.transaction_hk IS NULL THEN 0 ELSE 1 END
# MAGIC   FROM business_vault.bsat_transaction_ai_score a
# MAGIC   LEFT JOIN raw_vault.sat_transaction_label l USING (transaction_hk))
# MAGIC GROUP BY approach
