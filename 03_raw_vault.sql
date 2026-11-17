-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Silver: Raw Data Vault
-- MAGIC
-- MAGIC The raw vault stores bronze data **as-is but integrated by business key**, insert-only and fully auditable.
-- MAGIC
-- MAGIC | Type | Table | Business key / relationship |
-- MAGIC |---|---|---|
-- MAGIC | Hub | `hub_customer`, `hub_card`, `hub_merchant`, `hub_transaction` | one row per business key, ever |
-- MAGIC | Link | `link_card_customer` | which customer owns which card |
-- MAGIC | Link | `link_transaction` | transaction ↔ card ↔ merchant |
-- MAGIC | Satellite | `sat_customer`, `sat_card`, `sat_merchant` | descriptive attributes, a new row only when they change (hashdiff) |
-- MAGIC | Satellite | `sat_transaction` | the authorization details (immutable events) |
-- MAGIC | Satellite | `sat_transaction_label` | confirmed fraud from the chargeback feed (a second record source) |
-- MAGIC
-- MAGIC Hash keys are `sha2(upper(trim(business_key)), 256)`. Every load is idempotent: rerun it and nothing duplicates.
-- MAGIC
-- MAGIC ```
-- MAGIC  hub_customer ── link_card_customer ── hub_card ── link_transaction ── hub_transaction
-- MAGIC       │                                    │              │                 │
-- MAGIC  sat_customer                          sat_card      hub_merchant     sat_transaction
-- MAGIC                                                         │             sat_transaction_label
-- MAGIC                                                    sat_merchant
-- MAGIC ```

-- COMMAND ----------

-- MAGIC %run ./00_config

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Create the vault tables (once)

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS raw_vault.hub_customer (customer_hk STRING, customer_id STRING, load_dts TIMESTAMP, record_source STRING) USING DELTA;
CREATE TABLE IF NOT EXISTS raw_vault.hub_card (card_hk STRING, card_id STRING, load_dts TIMESTAMP, record_source STRING) USING DELTA;
CREATE TABLE IF NOT EXISTS raw_vault.hub_merchant (merchant_hk STRING, merchant_id STRING, load_dts TIMESTAMP, record_source STRING) USING DELTA;
CREATE TABLE IF NOT EXISTS raw_vault.hub_transaction (transaction_hk STRING, transaction_id STRING, load_dts TIMESTAMP, record_source STRING) USING DELTA;

CREATE TABLE IF NOT EXISTS raw_vault.link_card_customer (card_customer_hk STRING, card_hk STRING, customer_hk STRING, load_dts TIMESTAMP, record_source STRING) USING DELTA;
CREATE TABLE IF NOT EXISTS raw_vault.link_transaction (transaction_link_hk STRING, transaction_hk STRING, card_hk STRING, merchant_hk STRING, load_dts TIMESTAMP, record_source STRING) USING DELTA;

CREATE TABLE IF NOT EXISTS raw_vault.sat_customer (customer_hk STRING, load_dts TIMESTAMP, hashdiff STRING, first_name STRING, last_name STRING, age_band STRING, home_city STRING, record_source STRING) USING DELTA;
CREATE TABLE IF NOT EXISTS raw_vault.sat_card (card_hk STRING, load_dts TIMESTAMP, hashdiff STRING, card_type STRING, issued_date DATE, record_source STRING) USING DELTA;
CREATE TABLE IF NOT EXISTS raw_vault.sat_merchant (merchant_hk STRING, load_dts TIMESTAMP, hashdiff STRING, merchant_name STRING, merchant_category STRING, risk_tier STRING, record_source STRING) USING DELTA;
CREATE TABLE IF NOT EXISTS raw_vault.sat_transaction (transaction_hk STRING, load_dts TIMESTAMP, event_ts TIMESTAMP, amount DOUBLE, currency STRING, channel STRING, card_present BOOLEAN, device_id STRING, ip_country STRING, txn_city STRING, txn_lat DOUBLE, txn_lon DOUBLE, record_source STRING) USING DELTA;
CREATE TABLE IF NOT EXISTS raw_vault.sat_transaction_label (transaction_hk STRING, load_dts TIMESTAMP, label STRING, fraud_pattern STRING, reported_ts TIMESTAMP, record_source STRING) USING DELTA;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Load hubs: insert business keys we have not seen before

-- COMMAND ----------

MERGE INTO raw_vault.hub_customer t
USING (SELECT DISTINCT sha2(upper(trim(customer_id)), 256) AS customer_hk, customer_id FROM bronze.customers) s
ON t.customer_hk = s.customer_hk
WHEN NOT MATCHED THEN INSERT (customer_hk, customer_id, load_dts, record_source)
  VALUES (s.customer_hk, s.customer_id, current_timestamp(), 'bronze.customers');

MERGE INTO raw_vault.hub_card t
USING (SELECT card_hk, card_id, source FROM (
         SELECT sha2(upper(trim(card_id)), 256) AS card_hk, card_id, source,
                row_number() OVER (PARTITION BY card_id ORDER BY source) AS rn
         FROM (SELECT card_id, 'bronze.cards' AS source FROM bronze.cards
               UNION ALL
               SELECT DISTINCT card_id, 'bronze.transactions' AS source FROM bronze.transactions))
       WHERE rn = 1) s
ON t.card_hk = s.card_hk
WHEN NOT MATCHED THEN INSERT (card_hk, card_id, load_dts, record_source)
  VALUES (s.card_hk, s.card_id, current_timestamp(), s.source);

MERGE INTO raw_vault.hub_merchant t
USING (SELECT DISTINCT sha2(upper(trim(merchant_id)), 256) AS merchant_hk, merchant_id FROM bronze.merchants) s
ON t.merchant_hk = s.merchant_hk
WHEN NOT MATCHED THEN INSERT (merchant_hk, merchant_id, load_dts, record_source)
  VALUES (s.merchant_hk, s.merchant_id, current_timestamp(), 'bronze.merchants');

MERGE INTO raw_vault.hub_transaction t
USING (SELECT DISTINCT sha2(upper(trim(transaction_id)), 256) AS transaction_hk, transaction_id FROM bronze.transactions) s
ON t.transaction_hk = s.transaction_hk
WHEN NOT MATCHED THEN INSERT (transaction_hk, transaction_id, load_dts, record_source)
  VALUES (s.transaction_hk, s.transaction_id, current_timestamp(), 'bronze.transactions');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Load links: relationships between business keys

-- COMMAND ----------

MERGE INTO raw_vault.link_card_customer t
USING (SELECT DISTINCT
         sha2(concat_ws('||', upper(trim(card_id)), upper(trim(customer_id))), 256) AS card_customer_hk,
         sha2(upper(trim(card_id)), 256) AS card_hk,
         sha2(upper(trim(customer_id)), 256) AS customer_hk
       FROM bronze.cards) s
ON t.card_customer_hk = s.card_customer_hk
WHEN NOT MATCHED THEN INSERT (card_customer_hk, card_hk, customer_hk, load_dts, record_source)
  VALUES (s.card_customer_hk, s.card_hk, s.customer_hk, current_timestamp(), 'bronze.cards');

MERGE INTO raw_vault.link_transaction t
USING (SELECT DISTINCT
         sha2(concat_ws('||', upper(trim(transaction_id)), upper(trim(card_id)), upper(trim(merchant_id))), 256) AS transaction_link_hk,
         sha2(upper(trim(transaction_id)), 256) AS transaction_hk,
         sha2(upper(trim(card_id)), 256) AS card_hk,
         sha2(upper(trim(merchant_id)), 256) AS merchant_hk
       FROM bronze.transactions) s
ON t.transaction_link_hk = s.transaction_link_hk
WHEN NOT MATCHED THEN INSERT (transaction_link_hk, transaction_hk, card_hk, merchant_hk, load_dts, record_source)
  VALUES (s.transaction_link_hk, s.transaction_hk, s.card_hk, s.merchant_hk, current_timestamp(), 'bronze.transactions');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Load satellites
-- MAGIC Descriptive satellites get a new row only when the **hashdiff** (a hash of all attributes) differs
-- MAGIC from the latest row for that key. Transactions are immutable events, so `sat_transaction` is insert-once.

-- COMMAND ----------

INSERT INTO raw_vault.sat_customer (customer_hk, load_dts, hashdiff, first_name, last_name, age_band, home_city, record_source)
SELECT s.customer_hk, current_timestamp(), s.hashdiff, s.first_name, s.last_name, s.age_band, s.home_city, 'bronze.customers'
FROM (SELECT sha2(upper(trim(customer_id)), 256) AS customer_hk,
             sha2(concat_ws('||', coalesce(first_name, ''), coalesce(last_name, ''), coalesce(age_band, ''), coalesce(home_city, '')), 256) AS hashdiff,
             first_name, last_name, age_band, home_city
      FROM bronze.customers) s
LEFT JOIN (SELECT customer_hk, hashdiff FROM (
             SELECT customer_hk, hashdiff, row_number() OVER (PARTITION BY customer_hk ORDER BY load_dts DESC) AS rn
             FROM raw_vault.sat_customer) WHERE rn = 1) cur
  ON s.customer_hk = cur.customer_hk
WHERE cur.customer_hk IS NULL OR cur.hashdiff <> s.hashdiff;

INSERT INTO raw_vault.sat_card (card_hk, load_dts, hashdiff, card_type, issued_date, record_source)
SELECT s.card_hk, current_timestamp(), s.hashdiff, s.card_type, s.issued_date, 'bronze.cards'
FROM (SELECT sha2(upper(trim(card_id)), 256) AS card_hk,
             sha2(concat_ws('||', coalesce(card_type, ''), coalesce(issued_date, '')), 256) AS hashdiff,
             card_type, to_date(issued_date) AS issued_date
      FROM bronze.cards) s
LEFT JOIN (SELECT card_hk, hashdiff FROM (
             SELECT card_hk, hashdiff, row_number() OVER (PARTITION BY card_hk ORDER BY load_dts DESC) AS rn
             FROM raw_vault.sat_card) WHERE rn = 1) cur
  ON s.card_hk = cur.card_hk
WHERE cur.card_hk IS NULL OR cur.hashdiff <> s.hashdiff;

INSERT INTO raw_vault.sat_merchant (merchant_hk, load_dts, hashdiff, merchant_name, merchant_category, risk_tier, record_source)
SELECT s.merchant_hk, current_timestamp(), s.hashdiff, s.merchant_name, s.merchant_category, s.risk_tier, 'bronze.merchants'
FROM (SELECT sha2(upper(trim(merchant_id)), 256) AS merchant_hk,
             sha2(concat_ws('||', coalesce(merchant_name, ''), coalesce(merchant_category, ''), coalesce(risk_tier, '')), 256) AS hashdiff,
             merchant_name, merchant_category, risk_tier
      FROM bronze.merchants) s
LEFT JOIN (SELECT merchant_hk, hashdiff FROM (
             SELECT merchant_hk, hashdiff, row_number() OVER (PARTITION BY merchant_hk ORDER BY load_dts DESC) AS rn
             FROM raw_vault.sat_merchant) WHERE rn = 1) cur
  ON s.merchant_hk = cur.merchant_hk
WHERE cur.merchant_hk IS NULL OR cur.hashdiff <> s.hashdiff;

MERGE INTO raw_vault.sat_transaction t
USING (SELECT * FROM (
         SELECT sha2(upper(trim(transaction_id)), 256) AS transaction_hk, event_ts, amount, currency, channel, card_present,
                device_id, ip_country, txn_city, txn_lat, txn_lon,
                row_number() OVER (PARTITION BY transaction_id ORDER BY _ingest_ts) AS rn
         FROM bronze.transactions) WHERE rn = 1) s
ON t.transaction_hk = s.transaction_hk
WHEN NOT MATCHED THEN INSERT (transaction_hk, load_dts, event_ts, amount, currency, channel, card_present, device_id,
                              ip_country, txn_city, txn_lat, txn_lon, record_source)
  VALUES (s.transaction_hk, current_timestamp(), s.event_ts, s.amount, s.currency, s.channel, s.card_present, s.device_id,
          s.ip_country, s.txn_city, s.txn_lat, s.txn_lon, 'bronze.transactions');

MERGE INTO raw_vault.sat_transaction_label t
USING (SELECT * FROM (
         SELECT sha2(upper(trim(transaction_id)), 256) AS transaction_hk, label, fraud_pattern, reported_ts,
                row_number() OVER (PARTITION BY transaction_id ORDER BY _ingest_ts) AS rn
         FROM bronze.fraud_labels) WHERE rn = 1) s
ON t.transaction_hk = s.transaction_hk
WHEN NOT MATCHED THEN INSERT (transaction_hk, load_dts, label, fraud_pattern, reported_ts, record_source)
  VALUES (s.transaction_hk, current_timestamp(), s.label, s.fraud_pattern, s.reported_ts, 'bronze.fraud_labels');

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Check the vault

-- COMMAND ----------

SELECT 'hub_customer' AS vault_table, count(*) AS rows FROM raw_vault.hub_customer UNION ALL
SELECT 'hub_card', count(*) FROM raw_vault.hub_card UNION ALL
SELECT 'hub_merchant', count(*) FROM raw_vault.hub_merchant UNION ALL
SELECT 'hub_transaction', count(*) FROM raw_vault.hub_transaction UNION ALL
SELECT 'link_card_customer', count(*) FROM raw_vault.link_card_customer UNION ALL
SELECT 'link_transaction', count(*) FROM raw_vault.link_transaction UNION ALL
SELECT 'sat_customer', count(*) FROM raw_vault.sat_customer UNION ALL
SELECT 'sat_card', count(*) FROM raw_vault.sat_card UNION ALL
SELECT 'sat_merchant', count(*) FROM raw_vault.sat_merchant UNION ALL
SELECT 'sat_transaction', count(*) FROM raw_vault.sat_transaction UNION ALL
SELECT 'sat_transaction_label', count(*) FROM raw_vault.sat_transaction_label
