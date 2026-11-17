-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 11 · AI benefits showcase
-- MAGIC
-- MAGIC Five short queries that tell the AI story on transactions the model **never saw** (Oct 21 onward).
-- MAGIC Run it top to bottom during the "AI infusion" part of the workshop. Each result is small enough to read
-- MAGIC from the back of the room; turn the ones marked 📊 into a chart with **+ → Visualization**.
-- MAGIC
-- MAGIC | Moment | Question it answers |
-- MAGIC |---|---|
-- MAGIC | 1 | Does AI catch more fraud? |
-- MAGIC | 2 | Does it bother fewer genuine customers and analysts? |
-- MAGIC | 3 | Which kinds of fraud does it help with? |
-- MAGIC | 4 | Show me one it caught that the rules missed |
-- MAGIC | 5 | Is the AI a black box? |

-- COMMAND ----------

-- MAGIC %run ./00_config

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Moment 1 · More fraud caught
-- MAGIC Same transactions, same thresholds. The only difference is how the score is made.

-- COMMAND ----------

SELECT approach,
       actual_fraud,
       fraud_caught,
       fraud_missed,
       concat(detection_pct, '%') AS detection_rate
FROM gold.v_rules_vs_ai
ORDER BY approach

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Moment 2 · Fewer people bothered 📊
-- MAGIC A smaller review queue means analysts spend their day on real cases, and fewer genuine customers get declined.

-- COMMAND ----------

SELECT approach,
       review_queue,
       genuine_blocked,
       round(100 * (review_queue - first_value(review_queue) OVER (ORDER BY approach)) /
             first_value(review_queue) OVER (ORDER BY approach), 0) AS queue_change_pct
FROM gold.v_rules_vs_ai
ORDER BY approach

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Moment 3 · Where the lift comes from 📊
-- MAGIC Rules already catch the obvious patterns. AI earns its keep on fraud made of several weak signals.
-- MAGIC *Chart: bar, x = fraud_pattern, y = rules_pct and ai_pct.*

-- COMMAND ----------

SELECT fraud_pattern, fraud_txns, caught_by_rules, caught_with_ai, rules_pct, ai_pct,
       round(ai_pct - rules_pct, 1) AS lift_pts
FROM gold.v_detection_by_pattern
ORDER BY lift_pts DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### What changed, transaction by transaction 📊
-- MAGIC *Chart: bar, x = ai_outcome, y = transactions (hide "Genuine, approved by both" to zoom in).*

-- COMMAND ----------

SELECT ai_outcome, transactions, amount_usd
FROM gold.v_ai_outcomes
WHERE ai_outcome <> 'Genuine, approved by both'
ORDER BY transactions DESC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Moment 4 · One the rules missed
-- MAGIC Pick a row and read it out: the rules scored it low and approved it; the AI saw the combination.

-- COMMAND ----------

SELECT transaction_id, event_ts, merchant_name, amount, confirmed_pattern,
       rule_score, coalesce(rule_reasons, '(no rule fired)') AS rules_fired,
       hybrid_score AS ai_score, hybrid_bucket AS ai_decision, analyst_note
FROM gold.v_ai_wins
ORDER BY CASE WHEN confirmed_pattern = 'subtle_fraud' THEN 0 ELSE 1 END, hybrid_score DESC
LIMIT 10

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Moment 5 · Not a black box
-- MAGIC Every alert carries its top drivers. These are the signals that most often drive AI alerts.

-- COMMAND ----------

SELECT trim(driver) AS signal, count(*) AS alerts_where_top_driver
FROM (SELECT explode(split(ai_drivers, ',')) AS driver
      FROM gold.fact_transaction_decision
      WHERE in_ai_test_window = 1 AND hybrid_bucket <> 'GENUINE' AND ai_drivers <> '')
GROUP BY trim(driver)
ORDER BY alerts_where_top_driver DESC
LIMIT 8
