# Databricks notebook source
# MAGIC %md
# MAGIC # 04 · Business Vault: behavioural features (PySpark)
# MAGIC
# MAGIC A raw transaction says little on its own. Context makes it meaningful: *is $276 normal for this card?
# MAGIC Is this a device we have never seen? Was the card swiped 1,200 km away an hour ago?*
# MAGIC
# MAGIC This notebook reads the raw vault, computes per-card features with **window functions**, and writes a
# MAGIC computed satellite `business_vault.bsat_transaction_features` keyed by `transaction_hk`.
# MAGIC
# MAGIC **Golden rule:** every feature uses only the card's *past* transactions, exactly as a live system would see them.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window as W

# 1. Assemble one row per transaction from hubs, links and current satellites
txn = spark.sql("""
WITH cur_merchant AS (
  SELECT * FROM (
    SELECT *, row_number() OVER (PARTITION BY merchant_hk ORDER BY load_dts DESC) AS rn FROM raw_vault.sat_merchant)
  WHERE rn = 1)
SELECT ht.transaction_hk, ht.transaction_id, hc.card_id, hm.merchant_id,
       st.event_ts, st.amount, st.channel, st.card_present, st.device_id, st.ip_country,
       st.txn_city, st.txn_lat, st.txn_lon, m.merchant_category, m.risk_tier
FROM raw_vault.hub_transaction ht
JOIN raw_vault.sat_transaction st ON st.transaction_hk = ht.transaction_hk
JOIN raw_vault.link_transaction lt ON lt.transaction_hk = ht.transaction_hk
JOIN raw_vault.hub_card hc ON hc.card_hk = lt.card_hk
JOIN raw_vault.hub_merchant hm ON hm.merchant_hk = lt.merchant_hk
JOIN cur_merchant m ON m.merchant_hk = lt.merchant_hk
""").withColumn("ts", F.col("event_ts").cast("long"))

# COMMAND ----------

# 2. Window definitions: per card, ordered by time
by_card = W.partitionBy("card_id").orderBy("ts", "transaction_id")
past = by_card.rowsBetween(W.unboundedPreceding, -1)            # everything before this txn
def last(seconds):                                               # time window ending at this txn
    return W.partitionBy("card_id").orderBy("ts").rangeBetween(-seconds, 0)

GLOBAL_MEDIAN = txn.approxQuantile("amount", [0.5], 0.01)[0]

f = (txn
     # spending baseline
     .withColumn("history_depth", F.row_number().over(by_card) - 1)
     .withColumn("prior_median_amt", F.when(F.count("amount").over(past) >= 3,
                                            F.percentile_approx("amount", 0.5).over(past)))
     .withColumn("prior_median_amt", F.coalesce("prior_median_amt", F.lit(GLOBAL_MEDIAN)))
     .withColumn("amount_ratio", F.round(F.col("amount") / F.col("prior_median_amt"), 2))
     # time-of-day habit (circular distance from the card's usual hour)
     .withColumn("hour", F.hour("event_ts"))
     .withColumn("usual_hour", F.coalesce(F.when(F.count("hour").over(past) >= 3, F.avg("hour").over(past)), F.lit(14.0)))
     .withColumn("hour_deviation", F.round(F.least(F.abs(F.col("hour") - F.col("usual_hour")),
                                                   24 - F.abs(F.col("hour") - F.col("usual_hour"))), 1))
     # velocity
     .withColumn("txn_count_10m", F.count("*").over(last(600)))
     .withColumn("txn_count_1h", F.count("*").over(last(3600)))
     .withColumn("txn_count_24h", F.count("*").over(last(86400)))
     .withColumn("small_txn_count_1h", F.sum((F.col("amount") < 10).cast("int")).over(last(3600)))
     .withColumn("spend_24h_ratio", F.round(F.sum("amount").over(last(86400)) / F.col("prior_median_amt"), 2))
     # familiarity: device and merchant
     .withColumn("prior_online", F.coalesce(F.sum((~F.col("card_present")).cast("int")).over(past), F.lit(0)))
     .withColumn("device_seq", F.row_number().over(W.partitionBy("card_id", "device_id").orderBy("ts", "transaction_id")))
     .withColumn("is_new_device", (F.col("device_id").isNotNull() & (F.col("device_seq") == 1)
                                   & (F.col("prior_online") > 0)).cast("int"))
     .withColumn("merchant_seq", F.row_number().over(W.partitionBy("card_id", "merchant_id").orderBy("ts", "transaction_id")))
     .withColumn("is_new_merchant", ((F.col("merchant_seq") == 1) & (F.col("history_depth") >= 5)).cast("int"))
     .withColumn("new_merchants_24h", F.sum("is_new_merchant").over(last(86400)))
     # context
     .withColumn("is_high_risk_mcc", (F.col("risk_tier") == "high").cast("int"))
     .withColumn("is_card_not_present", (~F.col("card_present")).cast("int"))
     .withColumn("is_foreign_ip", (F.col("ip_country").isNotNull() & (F.col("ip_country") != "US")).cast("int")))

# 3. Geography: distance and implied speed between consecutive in-store swipes
cp_w = W.partitionBy("card_id").orderBy("ts", "transaction_id")
cp = (f.filter("card_present")
      .withColumn("p_lat", F.lag("txn_lat").over(cp_w))
      .withColumn("p_lon", F.lag("txn_lon").over(cp_w))
      .withColumn("p_ts", F.lag("ts").over(cp_w)))
dlat, dlon = F.radians(F.col("txn_lat") - F.col("p_lat")), F.radians(F.col("txn_lon") - F.col("p_lon"))
hav = F.sin(dlat / 2) ** 2 + F.cos(F.radians("p_lat")) * F.cos(F.radians("txn_lat")) * F.sin(dlon / 2) ** 2
cp = (cp.withColumn("km_from_prev_swipe", F.round(2 * 6371 * F.asin(F.sqrt(hav)), 0))
        .withColumn("travel_speed_kmh", F.round(F.col("km_from_prev_swipe")
                                                / F.greatest((F.col("ts") - F.col("p_ts")) / 3600, F.lit(1 / 60)), 0))
        .select("transaction_hk", "km_from_prev_swipe", "travel_speed_kmh"))
f = f.join(cp, "transaction_hk", "left").fillna(0, ["km_from_prev_swipe", "travel_speed_kmh"])

FEATURES = ["amount", "amount_ratio", "prior_median_amt", "hour", "hour_deviation", "txn_count_10m",
            "txn_count_1h", "txn_count_24h", "small_txn_count_1h", "spend_24h_ratio", "km_from_prev_swipe",
            "travel_speed_kmh", "is_new_device", "is_new_merchant", "new_merchants_24h", "is_high_risk_mcc",
            "is_card_not_present", "is_foreign_ip", "history_depth"]

(f.select("transaction_hk", "event_ts", *FEATURES)
  .withColumn("load_dts", F.current_timestamp())
  .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
  .saveAsTable("business_vault.bsat_transaction_features"))
print(f"business_vault.bsat_transaction_features: {spark.table('business_vault.bsat_transaction_features').count():,} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- A peek at the features for the most unusual transactions
# MAGIC SELECT ht.transaction_id, f.event_ts, f.amount, f.amount_ratio, f.txn_count_1h, f.small_txn_count_1h,
# MAGIC        f.km_from_prev_swipe, f.travel_speed_kmh, f.is_new_device, f.is_foreign_ip, f.new_merchants_24h
# MAGIC FROM business_vault.bsat_transaction_features f
# MAGIC JOIN raw_vault.hub_transaction ht USING (transaction_hk)
# MAGIC ORDER BY f.amount_ratio DESC
# MAGIC LIMIT 20
