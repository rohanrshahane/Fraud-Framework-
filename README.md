# Fraud Detection Framework on Databricks — FDU Workshop

An end-to-end lakehouse demo: card transactions stream in from merchants, land in a Data Vault, are scored by an
explainable rules engine and then by AI, and finish in a gold layer that feeds a **Fraud Review Report** dashboard.
Students finish with a hands-on exercise. All data is synthetic.

```
 landing volume ──Auto Loader──▶ bronze ──▶ raw_vault (hubs, links, sats) ──▶ business_vault ──▶ gold ──▶ AI/BI dashboard
 (JSON per day)    (streaming)              Data Vault 2.0, insert-only        features (PySpark)   star schema
 transactions                                                                   rules  (SQL table)   report views
 chargebacks                                                                    AI     (sklearn + MLflow)
 reference data
```

## Notebooks

| Notebook | Language | What it does |
|---|---|---|
| `00_config` | Python | Catalog `fraud_workshop`, schemas, the `landing.events` volume, thresholds |
| `01_generate_events` | Python | Drops one JSON file per simulated day: transactions, chargeback labels, reference data |
| `02_bronze_streaming` | PySpark | Auto Loader stream (`availableNow`) into `bronze.transactions` and `bronze.fraud_labels` |
| `03_raw_vault` | SQL | Hubs, links and satellites with hash keys and hashdiffs; idempotent MERGE loads |
| `04_business_vault_features` | PySpark | 19 per-card behavioural features with window functions, past data only |
| `05_rules_engine` | SQL + Python | Rules stored in `ref_fraud_rules`; score 0–100 → FRAUD / REVIEW / GENUINE with reasons |
| `06_ai_scoring` | Python | Gradient boosting + Isolation Forest + hybrid score + explanations; logged to MLflow |
| `07_gold_layer` | SQL | Dimensions, `fact_transaction_decision`, and the report views |
| `08_dashboard_queries` | SQL | One query per dashboard widget, plus build steps |
| `00_run_pipeline` | — | Runs 02 → 07 in order |
| `11_ai_benefits_showcase` | SQL | Five presenter-ready queries that tell the AI story on unseen days |
| `13_create_dashboard` | Python | Creates and publishes the five-page Fraud Review Report AI/BI dashboard |
| `09_student_exercise` | SQL + Python | Four tasks with automatic checks |
| `10_exercise_solutions` | SQL + Python | Instructor copy (not in the student zip) |
| `99_reset_workshop` | Python | Drops everything for a clean rehearsal |

## Setup (works on Databricks Free Edition)

1. Sign in to a Databricks workspace (Free Edition is enough; it is serverless-only, which this demo is built for).
2. **Workspace → your folder → ⋮ → Import**, choose `fraud_workshop_databricks.zip`. The notebooks appear in a folder.
   (Alternatively push this folder to GitHub and add it as a **Git folder**.)
3. Open **01_generate_events**, attach to **Serverless**, *Run all*. Defaults land days 1–30 (Oct 1–30, 2026).
4. Open **00_run_pipeline**, *Run all* (a few minutes).
5. Run **13_create_dashboard** to create and publish the **Fraud Review Report** dashboard (manual fallback: notebook 08).
6. Share `fdu_fraud_workshop_students.zip` with students; it has no solutions and includes `STUDENT_GUIDE.md`.

If the account cannot create catalogs, `00_config` falls back to the default `workspace` catalog automatically.

## Running it live

| Workshop moment | What to run | What to show |
|---|---|---|
| Events arriving | `01_generate_events` with `seconds_between_days` = 3 | Files appearing in the volume (Catalog → landing → events) |
| Streaming ingest | `02_bronze_streaming` | Only new files are picked up; per-day counts |
| Data Vault | `03_raw_vault` | Hubs/links/sats, hash keys, the row-count check |
| Rules tagging | `05_rules_engine` | The generated SQL, bucket counts, detection by pattern |
| AI infusion | `06_ai_scoring` | Rules vs Rules + AI on unseen days; MLflow run |
| Report | the dashboard | Review queue, fraud by type, genuine section |

On a classic (non-serverless) cluster set `trigger_mode` = `continuous` in notebook 02 and run notebook 01 with a
pause in parallel to watch the stream pick files up as they land.

## Reference results (seed 42, days 1–30)

Rules engine, whole month: FRAUD 479 (444 confirmed fraud), REVIEW 594 (88 fraud), GENUINE 52,772 (187 fraud missed).

On the unseen days Oct 21–30 (18,089 transactions, 242 fraud):

| | Rules | Rules + AI |
|---|---|---|
| Fraud detected | 80.2% | 89.7% |
| Fraud missed | 48 | 25 |
| Genuine blocked | 10 | 7 |
| Review queue | 159 | 25 |
| Subtle fraud caught | 2 of 34 | 12 of 34 |

Figures come from a local Spark run of the same notebooks, and repeat exactly run to run. On Databricks they should
match closely; small differences can come from Spark's approximate median.

## Student exercise (notebook 09, ~50 minutes)

1. **Streaming:** land days 31–32 and rerun the pipeline; see that only new files are ingested.
2. **Rules:** add `R12_SHOPPING_SPREE` to the rules table. Reference answer (30 points,
   `new_merchants_24h >= 2 AND amount_ratio >= 2`) lifts subtle fraud caught from 9 to 78 of 146,
   but genuine customers flagged rise from 556 to 1,038: a good debate.
3. **Data Vault:** model `hub_device` and `link_transaction_device`.
4. **Gold:** build `gold.v_merchant_risk` and add it to the dashboard.
5. **Bonus:** tune the AI hybrid weights and compare `gold.v_rules_vs_ai`.

## Testing without Databricks

`tests/run_local.py` runs the notebooks on open-source PySpark (`pip install pyspark==3.5.* scikit-learn pandas numpy`).
It emulates the few Delta-only statements on parquet, so it is for checking logic, not for the workshop itself:

```bash
python tests/run_local.py 01_generate_events 00_run_pipeline 10_exercise_solutions
```
