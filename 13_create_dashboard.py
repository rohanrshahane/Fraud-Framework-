# Databricks notebook source
# MAGIC %md
# MAGIC # 13 · Create the Fraud Review Report dashboard (one click)
# MAGIC
# MAGIC Builds a five-page **AI/BI dashboard** on the gold layer and publishes it, so the report runs in the browser
# MAGIC on any computer (no Power BI needed).
# MAGIC
# MAGIC | Page | What it shows |
# MAGIC |---|---|
# MAGIC | Overview | volume, blocked / in review / approved, amount blocked, daily trend |
# MAGIC | Review report | alerts by fraud type, the analyst queue, the genuine section |
# MAGIC | AI benefits | rules vs rules + AI on unseen days, lift by fraud pattern, what changed |
# MAGIC | Cases AI caught | fraud the rules approved and the AI stopped, with the analyst note |
# MAGIC | Who is affected | fraud per 1,000 transactions by age band |
# MAGIC
# MAGIC Run it after `00_run_pipeline`. Rerunning updates the same dashboard instead of making a copy.
# MAGIC The last cell prints the link. If the API call is not allowed in your workspace, the notebook also saves
# MAGIC `fraud_review_report.lvdash.json` to the volume for **Dashboards → Create → Import dashboard from file**.

# COMMAND ----------

# MAGIC %run ./00_config

# COMMAND ----------

import json

DASHBOARD_NAME = "Fraud Review Report"
G = f"{CATALOG}.gold"

# ---------------------------------------------------------------- datasets (one SQL query each)
DATASETS = {
    "kpi": f"""
SELECT count(*)                                                                  AS transactions,
       sum(CASE WHEN final_bucket = 'FRAUD'   THEN 1 ELSE 0 END)                 AS blocked,
       sum(CASE WHEN final_bucket = 'REVIEW'  THEN 1 ELSE 0 END)                 AS in_review,
       sum(CASE WHEN final_bucket = 'GENUINE' THEN 1 ELSE 0 END)                 AS approved,
       round(sum(CASE WHEN final_bucket = 'FRAUD'   THEN amount ELSE 0 END), 0)  AS amount_blocked_usd,
       round(sum(CASE WHEN final_bucket = 'GENUINE' THEN amount ELSE 0 END), 0)  AS amount_approved_usd,
       sum(CASE WHEN final_bucket = 'GENUINE' THEN confirmed_fraud ELSE 0 END)   AS fraud_missed
FROM {G}.fact_transaction_decision""",
    "decision_mix": f"""
SELECT final_bucket AS decision, count(*) AS transactions
FROM {G}.fact_transaction_decision GROUP BY final_bucket""",
    "daily": f"""
SELECT event_date, final_bucket AS decision, count(*) AS transactions
FROM {G}.fact_transaction_decision WHERE final_bucket <> 'GENUINE'
GROUP BY event_date, final_bucket ORDER BY event_date""",
    "fraud_by_type": f"""
SELECT suspected_type, decision, alerts FROM {G}.v_fraud_by_type ORDER BY alerts DESC""",
    "review_queue": f"""
SELECT decision, transaction_id, event_ts, customer_name, merchant_name, amount, suspected_type, risk_score,
       coalesce(analyst_note, rule_reasons) AS why_flagged
FROM {G}.v_review_report ORDER BY risk_score DESC, event_ts DESC LIMIT 500""",
    "genuine_by_channel": f"""
SELECT channel, approved_txns FROM {G}.v_genuine_summary""",
    "ai_kpi": f"""
SELECT r.detection_pct AS rules_detection_pct, a.detection_pct AS ai_detection_pct,
       r.fraud_missed  AS rules_fraud_missed,  a.fraud_missed  AS ai_fraud_missed,
       r.review_queue  AS rules_review_queue,  a.review_queue  AS ai_review_queue,
       r.genuine_blocked AS rules_genuine_blocked, a.genuine_blocked AS ai_genuine_blocked,
       round(100 * (1 - a.review_queue / r.review_queue), 0) AS queue_reduction_pct
FROM (SELECT * FROM {G}.v_rules_vs_ai WHERE approach = 'Rules') r
CROSS JOIN (SELECT * FROM {G}.v_rules_vs_ai WHERE approach = 'Rules + AI') a""",
    "pattern_compare": f"""
SELECT fraud_pattern, 'Rules' AS approach, rules_pct AS pct_caught FROM {G}.v_detection_by_pattern
UNION ALL
SELECT fraud_pattern, 'Rules + AI' AS approach, ai_pct AS pct_caught FROM {G}.v_detection_by_pattern""",
    "ai_outcomes": f"""
SELECT ai_outcome, transactions FROM {G}.v_ai_outcomes
WHERE ai_outcome <> 'Genuine, approved by both' ORDER BY transactions DESC""",
    "ai_wins": f"""
SELECT transaction_id, event_ts, merchant_name, amount, confirmed_pattern, rule_score,
       coalesce(rule_reasons, '(no rule fired)') AS rules_fired, hybrid_score AS ai_score, analyst_note
FROM {G}.v_ai_wins ORDER BY hybrid_score DESC""",
    "age": f"""
SELECT age_band, fraud_per_1000_txns, confirmed_fraud FROM {G}.v_fraud_by_age_band ORDER BY age_band""",
}

# COMMAND ----------

# ---------------------------------------------------------------- widget builders (6-column grid)
_n = [0]
def _id(prefix):
    _n[0] += 1
    return f"{prefix}{_n[0]:02d}"

def _query(ds, cols):
    return [{"name": "main_query",
             "query": {"datasetName": ds, "disaggregated": True,
                       "fields": [{"name": c, "expression": f"`{c}`"} for c in cols]}}]

def text(md, x, y, w, h=1):
    return {"widget": {"name": _id("t"), "multilineTextboxSpec": {"lines": [md]}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

def counter(ds, col, title, x, y, w=1, h=2):
    return {"widget": {"name": _id("c"), "queries": _query(ds, [col]),
                       "spec": {"version": 2, "widgetType": "counter",
                                "encodings": {"value": {"fieldName": col, "displayName": title}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

def chart(kind, ds, xcol, ycol, title, x, y, w, h, color=None, xtype="categorical", group=False):
    enc = {"x": {"fieldName": xcol, "scale": {"type": xtype}, "displayName": xcol.replace("_", " ")},
           "y": {"fieldName": ycol, "scale": {"type": "quantitative"}, "displayName": ycol.replace("_", " ")}}
    cols = [xcol, ycol]
    if color:
        enc["color"] = {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color.replace("_", " ")}
        cols.append(color)
    spec = {"version": 3, "widgetType": kind, "encodings": enc, "frame": {"showTitle": True, "title": title}}
    if group:
        spec["mark"] = {"layout": "group"}
    return {"widget": {"name": _id("g"), "queries": _query(ds, cols), "spec": spec},
            "position": {"x": x, "y": y, "width": w, "height": h}}

def pie(ds, angle, color, title, x, y, w, h):
    return {"widget": {"name": _id("p"), "queries": _query(ds, [angle, color]),
                       "spec": {"version": 3, "widgetType": "pie",
                                "encodings": {"angle": {"fieldName": angle, "scale": {"type": "quantitative"}, "displayName": angle},
                                              "color": {"fieldName": color, "scale": {"type": "categorical"}, "displayName": color}},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

def table(ds, cols, title, x, y, w, h):
    return {"widget": {"name": _id("tb"), "queries": _query(ds, cols),
                       "spec": {"version": 2, "widgetType": "table",
                                "encodings": {"columns": [{"fieldName": c, "displayName": c.replace("_", " ")} for c in cols]},
                                "frame": {"showTitle": True, "title": title}}},
            "position": {"x": x, "y": y, "width": w, "height": h}}

# ---------------------------------------------------------------- pages
pages = [
    {"name": "overview", "displayName": "1 · Overview", "layout": [
        text("## Fraud Review Report · every card transaction, tagged FRAUD / REVIEW / GENUINE", 0, 0, 6),
        counter("kpi", "transactions", "Transactions", 0, 1),
        counter("kpi", "blocked", "Blocked (FRAUD)", 1, 1),
        counter("kpi", "in_review", "Held for review", 2, 1),
        counter("kpi", "approved", "Approved (GENUINE)", 3, 1),
        counter("kpi", "amount_blocked_usd", "Amount blocked ($)", 4, 1, w=2),
        pie("decision_mix", "transactions", "decision", "Decision mix", 0, 3, 2, 6),
        chart("line", "daily", "event_date", "transactions", "Alerts per day", 2, 3, 4, 6, color="decision", xtype="temporal"),
    ]},
    {"name": "review", "displayName": "2 · Review report", "layout": [
        text("## Fraud & review cases by type, the analyst queue, and the genuine section", 0, 0, 6),
        chart("bar", "fraud_by_type", "suspected_type", "alerts", "Alerts by suspected fraud type", 0, 1, 3, 6, color="decision"),
        text("### Genuine section: approved instantly", 3, 1, 3),
        counter("kpi", "approved", "Approved", 3, 2),
        counter("kpi", "amount_approved_usd", "Approved ($)", 4, 2),
        counter("kpi", "fraud_missed", "Fraud that slipped through", 5, 2),
        chart("bar", "genuine_by_channel", "channel", "approved_txns", "Approved by channel", 3, 4, 3, 3),
        table("review_queue", ["decision", "transaction_id", "event_ts", "customer_name", "merchant_name", "amount",
                               "suspected_type", "risk_score", "why_flagged"], "Analyst review queue (highest risk first)", 0, 7, 6, 8),
    ]},
    {"name": "ai", "displayName": "3 · AI benefits", "layout": [
        text("## Rules vs Rules + AI on days the model never saw: same transactions, same thresholds", 0, 0, 6),
        counter("ai_kpi", "rules_detection_pct", "Rules: fraud detected %", 0, 1),
        counter("ai_kpi", "ai_detection_pct", "Rules + AI: fraud detected %", 1, 1),
        counter("ai_kpi", "rules_fraud_missed", "Rules: fraud missed", 2, 1),
        counter("ai_kpi", "ai_fraud_missed", "Rules + AI: fraud missed", 3, 1),
        counter("ai_kpi", "rules_review_queue", "Rules: review queue", 4, 1),
        counter("ai_kpi", "ai_review_queue", "Rules + AI: review queue", 5, 1),
        counter("ai_kpi", "rules_genuine_blocked", "Rules: genuine blocked", 0, 3),
        counter("ai_kpi", "ai_genuine_blocked", "Rules + AI: genuine blocked", 1, 3),
        counter("ai_kpi", "queue_reduction_pct", "Review queue reduction %", 2, 3, w=2),
        text("AI earns its keep on fraud made of several weak signals; obvious fraud is caught either way.", 4, 3, 2, 2),
        chart("bar", "pattern_compare", "fraud_pattern", "pct_caught", "% of fraud caught, by pattern", 0, 5, 3, 6,
              color="approach", group=True),
        chart("bar", "ai_outcomes", "ai_outcome", "transactions", "What changed when AI was added", 3, 5, 3, 6),
    ]},
    {"name": "wins", "displayName": "4 · Cases AI caught", "layout": [
        text("## Fraud the rules approved and the AI stopped", 0, 0, 6),
        table("ai_wins", ["transaction_id", "event_ts", "merchant_name", "amount", "confirmed_pattern", "rule_score",
                          "rules_fired", "ai_score", "analyst_note"], "Caught only with AI", 0, 1, 6, 10),
    ]},
    {"name": "age", "displayName": "5 · Who is affected", "layout": [
        text("## Who is affected · synthetic data, see slide 1 for real FTC figures", 0, 0, 6),
        chart("bar", "age", "age_band", "fraud_per_1000_txns", "Confirmed fraud per 1,000 transactions", 0, 1, 3, 6),
        chart("bar", "age", "age_band", "confirmed_fraud", "Confirmed fraud cases", 3, 1, 3, 6),
    ]},
]

dashboard = {
    "datasets": [{"name": k, "displayName": k.replace("_", " "), "queryLines": [q.strip()]} for k, q in DATASETS.items()],
    "pages": pages,
}
serialized = json.dumps(dashboard)
print(f"{len(DATASETS)} datasets, {sum(len(p['layout']) for p in pages)} widgets on {len(pages)} pages")

# COMMAND ----------

# Check every dataset query runs before creating the dashboard
for name, sql in DATASETS.items():
    rows = spark.sql(sql).count()
    print(f"  {name:<20} {rows:>4} rows")

# Save an importable copy (manual route: Dashboards > Create dashboard ▾ > Import dashboard from file)
EXPORT = f"{VOLUME}/exports"
os.makedirs(EXPORT, exist_ok=True)
with open(f"{EXPORT}/fraud_review_report.lvdash.json", "w") as fh:
    json.dump(dashboard, fh, indent=2)
print(f"Saved {EXPORT}/fraud_review_report.lvdash.json")

# COMMAND ----------

# Create (or update) and publish the dashboard through the Databricks SDK
if not ON_DATABRICKS:
    print("Local run: skipping the dashboard API.")
else:
    from databricks.sdk import WorkspaceClient
    from databricks.sdk.service.dashboards import Dashboard

    w = WorkspaceClient()
    warehouses = list(w.warehouses.list())
    if not warehouses:
        raise RuntimeError("No SQL warehouse found. Open SQL Warehouses in the sidebar and start or create one.")
    warehouse = next((wh for wh in warehouses if str(wh.state).endswith("RUNNING")), warehouses[0])
    folder = f"/Users/{w.current_user.me().user_name}"

    def as_dashboard():
        return Dashboard(display_name=DASHBOARD_NAME, serialized_dashboard=serialized,
                         warehouse_id=warehouse.id, parent_path=folder)

    existing = next((d for d in w.lakeview.list()
                     if d.display_name == DASHBOARD_NAME and (d.path or "").startswith(folder)), None)
    try:
        if existing:
            try:
                d = w.lakeview.update(existing.dashboard_id, dashboard=as_dashboard())
            except TypeError:  # older SDK signature
                d = w.lakeview.update(existing.dashboard_id, display_name=DASHBOARD_NAME,
                                      serialized_dashboard=serialized, warehouse_id=warehouse.id)
        else:
            try:
                d = w.lakeview.create(dashboard=as_dashboard())
            except TypeError:  # older SDK signature
                d = w.lakeview.create(display_name=DASHBOARD_NAME, serialized_dashboard=serialized,
                                      warehouse_id=warehouse.id, parent_path=folder)
        w.lakeview.publish(d.dashboard_id, embed_credentials=True, warehouse_id=warehouse.id)
        host = w.config.host.rstrip("/")
        print(f"Dashboard ready on warehouse '{warehouse.name}':")
        print(f"  Edit:      {host}/dashboardsv3/{d.dashboard_id}")
        print(f"  Published: {host}/dashboardsv3/{d.dashboard_id}/published")
    except Exception as e:
        print(f"Could not create the dashboard automatically ({type(e).__name__}: {e}).")
        print("Manual route: download fraud_review_report.lvdash.json from Catalog > landing > events > exports,")
        print("then Dashboards > Create dashboard (dropdown) > Import dashboard from file.")
