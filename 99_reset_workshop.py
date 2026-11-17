# Databricks notebook source
# MAGIC %md
# MAGIC # 99 · Reset the workshop
# MAGIC Drops every table, view and landed file so you can rehearse from a clean start.
# MAGIC Set the `confirm` widget to `yes` first. This cannot be undone.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import shutil

if get_param("confirm", "no", "Type yes to reset") != "yes":
    print("Nothing deleted. Set the 'confirm' widget to yes and rerun.")
else:
    for s in ["gold", "business_vault", "raw_vault", "bronze"]:
        spark.sql(f"DROP SCHEMA IF EXISTS {s} CASCADE")
    for sub in ["transactions", "labels", "reference", "_checkpoints"]:
        path = f"{VOLUME}/{sub}"
        if ON_DATABRICKS:
            dbutils.fs.rm(path, True)
        else:
            shutil.rmtree(path, ignore_errors=True)
    print("Reset complete. Start again with 01_generate_events.")
