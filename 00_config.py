# Databricks notebook source
# MAGIC %md
# MAGIC # 00 · Configuration
# MAGIC
# MAGIC Shared settings for every notebook in the **Fraud Detection Framework** workshop.
# MAGIC Other notebooks pull this in with `%run ./00_config`.
# MAGIC
# MAGIC | Layer | Schema | What lives there |
# MAGIC |---|---|---|
# MAGIC | Landing | `landing` | Volume `events` with raw JSON files (transactions, chargeback labels, reference data) |
# MAGIC | Bronze | `bronze` | Raw events as ingested by the stream, plus ingestion metadata |
# MAGIC | Silver | `raw_vault` | Data Vault 2.0 hubs, links and satellites |
# MAGIC | Silver | `business_vault` | Computed satellites: features, rule scores, AI scores |
# MAGIC | Gold | `gold` | Star schema and report views for the dashboard |

# COMMAND ----------

import os

def get_param(name, default, label=None):
    """Read a notebook widget on Databricks; fall back to env var / default elsewhere."""
    try:
        dbutils.widgets.text(name, str(default), label or name)
        return dbutils.widgets.get(name)
    except NameError:
        return os.environ.get(name.upper(), str(default))

try:
    dbutils  # noqa: F821
    ON_DATABRICKS = True
except NameError:
    ON_DATABRICKS = False

CATALOG = get_param("catalog", "fraud_workshop", "Catalog")
SCHEMAS = ["landing", "bronze", "raw_vault", "business_vault", "gold"]

if ON_DATABRICKS:
    try:
        spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    except Exception as e:  # no permission to create catalogs -> use the default 'workspace' catalog
        print(f"Could not create catalog '{CATALOG}' ({type(e).__name__}); using 'workspace' instead.")
        CATALOG = "workspace"
    spark.sql(f"USE CATALOG {CATALOG}")
else:
    CATALOG = "spark_catalog"

for s in SCHEMAS:
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {s}")

if ON_DATABRICKS:
    spark.sql("CREATE VOLUME IF NOT EXISTS landing.events")
    VOLUME = f"/Volumes/{CATALOG}/landing/events"
else:
    VOLUME = os.environ.get("LOCAL_VOLUME", "/tmp/fraud_volume")

TXN_PATH = f"{VOLUME}/transactions"      # one JSON file per day of card transactions
LABEL_PATH = f"{VOLUME}/labels"          # confirmed-fraud feed (chargebacks)
REF_PATH = f"{VOLUME}/reference"         # customers, cards, merchants
CHECKPOINTS = f"{VOLUME}/_checkpoints"   # streaming checkpoints and Auto Loader schemas

# Decision thresholds shared by the rules engine and the AI layer
FRAUD_THRESHOLD = 70    # score >= 70  -> FRAUD  (block)
REVIEW_THRESHOLD = 40   # score 40-69  -> REVIEW (analyst)
AI_CUTOFF_DATE = "2026-10-21"  # AI trains on events before this date and scores the rest

print(f"Catalog: {CATALOG} | Volume: {VOLUME} | Databricks: {ON_DATABRICKS}")
