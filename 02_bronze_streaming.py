# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze: stream events in with Auto Loader
# MAGIC
# MAGIC Auto Loader (`cloudFiles`) watches the landing volume and ingests **only new files** each run,
# MAGIC tracking progress in a checkpoint. Bronze keeps the events exactly as received, plus two audit columns:
# MAGIC `_ingest_ts` (when we loaded it) and `_source_file` (which file it came from).
# MAGIC
# MAGIC **Trigger:** `availableNow` processes everything that has landed, then stops. It is the trigger
# MAGIC serverless compute and Free Edition support. On a classic cluster you can switch the widget to
# MAGIC `continuous` to keep the stream running and watch new files arrive while notebook 01 drops them.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

from pyspark.sql import functions as F

TRIGGER = get_param("trigger_mode", "availableNow", "Trigger: availableNow | continuous")
RUN_SECONDS = int(get_param("continuous_seconds", 120, "Continuous mode: run for N seconds"))

TXN_SCHEMA = """transaction_id STRING, event_type STRING, event_ts TIMESTAMP, card_id STRING, merchant_id STRING,
amount DOUBLE, currency STRING, channel STRING, card_present BOOLEAN, device_id STRING, ip_country STRING,
txn_city STRING, txn_lat DOUBLE, txn_lon DOUBLE"""
LABEL_SCHEMA = "transaction_id STRING, label STRING, fraud_pattern STRING, reported_ts TIMESTAMP"


def stream_to_bronze(source_path, schema, table, name):
    """Incrementally ingest new JSON files from source_path into a bronze Delta table."""
    if ON_DATABRICKS:
        reader = (spark.readStream.format("cloudFiles")
                  .option("cloudFiles.format", "json")
                  .option("cloudFiles.schemaLocation", f"{CHECKPOINTS}/{name}_schema"))
    else:  # local testing without Auto Loader
        reader = spark.readStream.format("json")
    df = (reader.schema(schema).load(source_path)
          .withColumn("_ingest_ts", F.current_timestamp())
          .withColumn("_source_file", F.col("_metadata.file_path")))
    writer = (df.writeStream.format("delta")
              .option("checkpointLocation", f"{CHECKPOINTS}/{name}")
              .option("mergeSchema", "true"))
    if TRIGGER == "continuous":
        query = writer.trigger(processingTime="10 seconds").toTable(table)
        query.awaitTermination(RUN_SECONDS)
        query.stop()
    else:
        query = writer.trigger(availableNow=True).toTable(table)
        query.awaitTermination()
    return query


before = spark.table("bronze.transactions").count() if spark.catalog.tableExists("bronze.transactions") else 0
stream_to_bronze(TXN_PATH, TXN_SCHEMA, "bronze.transactions", "transactions")
stream_to_bronze(LABEL_PATH, LABEL_SCHEMA, "bronze.fraud_labels", "fraud_labels")
after = spark.table("bronze.transactions").count()
print(f"bronze.transactions: {before:,} -> {after:,} rows  (+{after - before:,} new events this run)")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reference data (batch)
# MAGIC Master data changes slowly, so it is reloaded in full. The raw vault keeps the history.

# COMMAND ----------

for name in ["customers", "cards", "merchants"]:
    (spark.read.json(f"{REF_PATH}/{name}")
     .withColumn("_ingest_ts", F.current_timestamp())
     .write.format("delta").mode("overwrite").option("overwriteSchema", "true")
     .saveAsTable(f"bronze.{name}"))
    print(f"bronze.{name}: {spark.table(f'bronze.{name}').count():,} rows")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- How the events arrived: one source file per simulated day
# MAGIC SELECT to_date(event_ts) AS event_date, count(*) AS events, max(_ingest_ts) AS ingested_at
# MAGIC FROM bronze.transactions
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1
