# Fraud Detection Framework on Databricks — Student Guide

IEEE AI 6.0 series · FDU · Nov 18, 2026. All data in this workshop is synthetic: no real customers or cards.

## Setup (about 15 minutes, mostly waiting)

1. **Sign in to Databricks Free Edition.** Search "Databricks Free Edition" and sign up with your email or Google
   account. Your workspace opens automatically. Everything runs on serverless compute; there is nothing to install.
2. **Unzip** `fdu_fraud_workshop_students.zip` on your laptop.
3. **Import it.** In Databricks: **Workspace → Home → ⋮ (top right) → Import**, choose the zip, click **Import**.
   You should see a folder `fdu_fraud_workshop_students` with notebooks `00_config` … `13_create_dashboard`.
   Can't find it? Use the search bar at the top for `00_config`, or refresh the page.
4. **Land the events.** Open `01_generate_events`, connect to **Serverless** at the top, click **Run all**.
   You should see 30 lines like `2026-10-01: 1,694 transactions landed`.
5. **Run the pipeline.** Open `00_run_pipeline`, click **Run all**. It takes about 10 minutes.
   At the end you'll see a rules bucket table and a "Rules vs Rules + AI" table.
6. **Optional:** run `13_create_dashboard` and open the **Published** link it prints to see the Fraud Review Report.

## What each notebook does

| Notebook | What it does |
|---|---|
| `01_generate_events` | Drops one JSON file per day of card transactions into a volume |
| `02_bronze_streaming` | Auto Loader streams only the new files into the bronze layer |
| `03_raw_vault` | Data Vault: hubs, links and satellites in SQL |
| `04_business_vault_features` | 19 behavioural features per card in PySpark, past data only |
| `05_rules_engine` | Rules stored in a table → score 0–100 → FRAUD / REVIEW / GENUINE with reasons |
| `06_ai_scoring` | Machine learning + anomaly detection + hybrid score + explanations |
| `07_gold_layer` | Star schema and report views |
| `11_ai_benefits_showcase` | Five queries: what AI changes, on days the model never saw |
| `13_create_dashboard` | Builds the five-page Fraud Review Report dashboard |
| `09_student_exercise` | Your exercise |

## The exercise (`09_student_exercise`)

Fill in every `???`, run the cell, then run the **✅ Check** cell under it. A green PASS means you've got it.

1. **Streaming:** land two new days (31 and 32) and rerun the pipeline. Why didn't October load again?
2. **Rules:** add a rule that catches the "quiet shopping spree": 2+ new shops in 24 hours, each 2x+ usual spend.
   What did it cost in genuine customers flagged?
3. **Data Vault:** model devices as `hub_device` and `link_transaction_device`.
4. **Gold:** build `gold.v_merchant_risk`: transactions, alerts and confirmed fraud per merchant.
5. **Bonus:** change the AI hybrid weights in `06_ai_scoring` and compare `gold.v_rules_vs_ai`.

## If something goes wrong

| Problem | Fix |
|---|---|
| Notebooks imported as plain files | Import each `.py` / `.sql` file one by one into one folder |
| "Could not create catalog" message | Normal on some accounts: the notebooks use the `workspace` catalog automatically |
| `INFINITE_STREAMING_TRIGGER_NOT_SUPPORTED` | Set the `trigger_mode` widget in `02_bronze_streaming` back to `availableNow` |
| "No transactions after the cutoff" in notebook 06 | Rerun `01_generate_events` with days 1 to 30, then `00_run_pipeline` |
| Table or view not found | Run `00_run_pipeline` first; every later notebook needs its tables |
