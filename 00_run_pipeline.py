# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Run the pipeline (bronze → vault → features → rules → AI → gold)
# MAGIC
# MAGIC Processes whatever event files have landed so far. Run **01_generate_events** first to drop the files,
# MAGIC then run this notebook top to bottom (or *Run all*). It is safe to rerun: the stream only picks up new
# MAGIC files and every vault load is insert-only.
# MAGIC
# MAGIC Takes a few minutes on serverless compute.

# COMMAND ----------

# MAGIC %run ./02_bronze_streaming

# COMMAND ----------

# MAGIC %run ./03_raw_vault

# COMMAND ----------

# MAGIC %run ./04_business_vault_features

# COMMAND ----------

# MAGIC %run ./05_rules_engine

# COMMAND ----------

# MAGIC %run ./06_ai_scoring

# COMMAND ----------

# MAGIC %run ./07_gold_layer
