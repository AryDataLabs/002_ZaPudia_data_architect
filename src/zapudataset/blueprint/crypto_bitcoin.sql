/*
=============================================
ADVANCED SQL QUERY FOR PRODUCT RECOMMENDATION ML MODEL
Dataset: bigquery-public-data.crypto_bitcoin
Features: CustomerID (Input Address), ProductID (Output Address/Transaction Type), Demographics, Behaviors, Counts, Pricing, UTC+9 Timestamps
Note: This dataset contains Bitcoin blockchain transaction data. We treat input addresses as "customers" and output addresses/transaction types as "products".
Tables: transactions (with nested inputs and outputs arrays)
Values are in satoshis (1 satoshi = 0.00000001 BTC)
=============================================
*/

WITH
/* Input Addresses as Customers */
input_addresses AS (
  SELECT
    input.address AS customer_id,
    /* Address characteristics */
    CASE
      WHEN LENGTH(input.address) > 34 THEN 'Multi-sig'
      WHEN input.address LIKE '1%' THEN 'Legacy'
      WHEN input.address LIKE '3%' THEN 'SegWit'
      ELSE 'Unknown'
    END AS address_type,
    /* Count metrics */
    CAST(COUNT(DISTINCT tx.hash) AS INT64) AS transaction_count,
    CAST(SUM(input.value) AS INT64) AS total_input_value_satoshis,
    CAST(AVG(input.value) AS FLOAT64) AS avg_input_value_satoshis,
    /* First and last activity */
    MIN(tx.block_timestamp) AS first_activity_timestamp,
    MAX(tx.block_timestamp) AS last_activity_timestamp,
    /* Activity period */
    CAST(DATE_DIFF(
      MAX(tx.block_timestamp),
      MIN(tx.block_timestamp),
      DAY
    ) AS INT64) AS days_active
  FROM `bigquery-public-data.crypto_bitcoin.transactions` tx,
  UNNEST(tx.inputs) AS input
  WHERE input.address IS NOT NULL
    AND input.value > 0
  GROUP BY 1, 2
),

/* Output Addresses as Products */
output_addresses AS (
  SELECT
    output.address AS product_id,
    /* Address characteristics */
    CASE
      WHEN LENGTH(output.address) > 34 THEN 'Multi-sig'
      WHEN output.address LIKE '1%' THEN 'Legacy'
      WHEN output.address LIKE '3%' THEN 'SegWit'
      ELSE 'Unknown'
    END AS address_type,
    /* Count metrics */
    CAST(COUNT(DISTINCT tx.hash) AS INT64) AS transaction_count,
    CAST(SUM(output.value) AS INT64) AS total_output_value_satoshis,
    CAST(AVG(output.value) AS FLOAT64) AS avg_output_value_satoshis,
    /* First and last activity */
    MIN(tx.block_timestamp) AS first_activity_timestamp,
    MAX(tx.block_timestamp) AS last_activity_timestamp,
    /* Activity period */
    CAST(DATE_DIFF(
      MAX(tx.block_timestamp),
      MIN(tx.block_timestamp),
      DAY
    ) AS INT64) AS days_active,
    /* Is this a known exchange or service? */
    CASE
      WHEN output.address IN (
        '1A1zP1eP5QGefi2DMPTfTL5SLmv7DivfNa',  /* MtGox */
        '1BitcoinEaterAddressDontSendf59kuE',     /* Bitcoin Eater */
        '1CounterpartyXXXXXXXXXXXXXXX43TUF'     /* Counterparty */
      ) THEN 'Exchange'
      ELSE 'Regular'
    END AS address_category
  FROM `bigquery-public-data.crypto_bitcoin.transactions` tx,
  UNNEST(tx.outputs) AS output
  WHERE output.address IS NOT NULL
    AND output.value > 0
  GROUP BY 1, 2, 3
),

/* Transaction Types as Product Categories */
transaction_categories AS (
  SELECT DISTINCT
    CASE
      WHEN tx.is_coinbase = TRUE THEN 'Coinbase'
      WHEN tx.input_count > 1 THEN 'Multi-Input'
      WHEN tx.output_count > 2 THEN 'Multi-Output'
      WHEN tx.input_count = 1 AND tx.output_count = 2 THEN 'Standard'
      ELSE 'Complex'
    END AS transaction_type,
    CAST(COUNT(*) AS INT64) AS transaction_count,
    CAST(AVG(tx.fee) AS FLOAT64) AS avg_fee_satoshis,
    CAST(AVG(tx.input_value) AS FLOAT64) AS avg_input_value_satoshis,
    CAST(AVG(tx.output_value) AS FLOAT64) AS avg_output_value_satoshis
  FROM `bigquery-public-data.crypto_bitcoin.transactions` tx
  GROUP BY 1
),

/* Customer-Product Interaction Matrix (Address-Address) */
address_interactions AS (
  SELECT
    input.address AS customer_id,
    output.address AS product_id,
    tx.hash AS transaction_hash,
    tx.block_number AS block_number,
    tx.block_timestamp AS interaction_timestamp,
    /* Transaction details */
    CAST(tx.input_count AS INT64) AS input_count,
    CAST(tx.output_count AS INT64) AS output_count,
    CAST(input.value AS INT64) AS input_value_satoshis,
    CAST(output.value AS INT64) AS output_value_satoshis,
    CAST(tx.fee AS INT64) AS transaction_fee_satoshis,
    tx.is_coinbase AS is_coinbase,
    /* Value in BTC (satoshis / 100,000,000) */
    CAST(input.value / 100000000.0 AS FLOAT64) AS input_value_btc,
    CAST(output.value / 100000000.0 AS FLOAT64) AS output_value_btc,
    CAST(tx.fee / 100000000.0 AS FLOAT64) AS transaction_fee_btc,
    /* Value ratio */
    CASE
      WHEN input.value > 0 THEN
        CAST(output.value * 100.0 / input.value AS FLOAT64)
      ELSE CAST(0 AS FLOAT64)
    END AS output_input_ratio
  FROM `bigquery-public-data.crypto_bitcoin.transactions` tx,
  UNNEST(tx.inputs) AS input,
  UNNEST(tx.outputs) AS output
  WHERE input.address IS NOT NULL
    AND output.address IS NOT NULL
    AND input.value > 0
    AND output.value > 0
),

/* Customer Behavioral Metrics */
customer_behavior AS (
  SELECT
    customer_id,
    /* Transaction metrics */
    CAST(COUNT(DISTINCT tx.hash) AS INT64) AS transaction_count,
    CAST(SUM(input.value) AS INT64) AS total_input_value_satoshis,
    CAST(AVG(input.value) AS FLOAT64) AS avg_input_value_satoshis,
    CAST(MAX(input.value) AS INT64) AS max_input_value_satoshis,
    /* Temporal metrics */
    MIN(tx.block_timestamp) AS first_transaction_timestamp,
    MAX(tx.block_timestamp) AS last_transaction_timestamp,
    CAST(DATE_DIFF(
      MAX(tx.block_timestamp),
      MIN(tx.block_timestamp),
      DAY
    ) AS INT64) AS days_active,
    /* Network metrics */
    CAST(COUNT(DISTINCT output.address) AS INT64) AS unique_outputs,
    CAST(AVG(tx.input_count) AS FLOAT64) AS avg_inputs_per_tx,
    CAST(AVG(tx.output_count) AS FLOAT64) AS avg_outputs_per_tx
  FROM `bigquery-public-data.crypto_bitcoin.transactions` tx,
  UNNEST(tx.inputs) AS input
  WHERE input.address IS NOT NULL
    AND input.value > 0
  GROUP BY 1
),

/* Product Behavioral Metrics */
product_behavior AS (
  SELECT
    product_id,
    /* Transaction metrics */
    CAST(COUNT(DISTINCT tx.hash) AS INT64) AS transaction_count,
    CAST(SUM(output.value) AS INT64) AS total_output_value_satoshis,
    CAST(AVG(output.value) AS FLOAT64) AS avg_output_value_satoshis,
    CAST(MAX(output.value) AS INT64) AS max_output_value_satoshis,
    /* Temporal metrics */
    MIN(tx.block_timestamp) AS first_transaction_timestamp,
    MAX(tx.block_timestamp) AS last_transaction_timestamp,
    CAST(DATE_DIFF(
      MAX(tx.block_timestamp),
      MIN(tx.block_timestamp),
      DAY
    ) AS INT64) AS days_active,
    /* Network metrics */
    CAST(COUNT(DISTINCT input.address) AS INT64) AS unique_inputs,
    CAST(AVG(tx.output_count) AS FLOAT64) AS avg_outputs_per_tx
  FROM `bigquery-public-data.crypto_bitcoin.transactions` tx,
  UNNEST(tx.outputs) AS output
  WHERE output.address IS NOT NULL
    AND output.value > 0
  GROUP BY 1
)

/*
=============================================
MAIN QUERY: Unified Address-Address Interaction Dataset for ML
=============================================
*/
SELECT
  /* Core Identifiers */
  ai.customer_id AS customer_id,
  ai.product_id AS product_id,
  ia.address_type AS customer_address_type,
  oa.address_type AS product_address_type,
  oa.address_category AS product_category,
  tc.transaction_type AS transaction_type,

  /* Customer Demographics */
  ia.transaction_count AS customer_transaction_count,
  ia.days_active AS customer_days_active,
  CAST(ia.total_input_value_satoshis / 100000000.0 AS FLOAT64) AS customer_total_input_btc,
  CAST(ia.avg_input_value_satoshis / 100000000.0 AS FLOAT64) AS customer_avg_input_btc,

  /* Product Features */
  oa.transaction_count AS product_transaction_count,
  oa.days_active AS product_days_active,
  CAST(oa.total_output_value_satoshis / 100000000.0 AS FLOAT64) AS product_total_output_btc,
  CAST(oa.avg_output_value_satoshis / 100000000.0 AS FLOAT64) AS product_avg_output_btc,

  /* Purchase Behavior Metrics */
  COALESCE(ai.input_count, CAST(0 AS INT64)) AS transaction_input_count,
  COALESCE(ai.output_count, CAST(0 AS INT64)) AS transaction_output_count,
  COALESCE(ai.input_value_satoshis, CAST(0 AS INT64)) AS input_value_satoshis,
  COALESCE(ai.output_value_satoshis, CAST(0 AS INT64)) AS output_value_satoshis,
  COALESCE(ai.transaction_fee_satoshis, CAST(0 AS INT64)) AS transaction_fee_satoshis,
  COALESCE(ai.input_value_btc, CAST(0 AS FLOAT64)) AS input_value_btc,
  COALESCE(ai.output_value_btc, CAST(0 AS FLOAT64)) AS output_value_btc,
  COALESCE(ai.transaction_fee_btc, CAST(0 AS FLOAT64)) AS transaction_fee_btc,
  COALESCE(ai.output_input_ratio, CAST(0 AS FLOAT64)) AS value_transfer_ratio,

  /* Pricing Features */
  ai.input_value_btc AS input_amount_btc,
  ai.output_value_btc AS output_amount_btc,
  ai.transaction_fee_btc AS fee_amount_btc,
  CAST(ai.output_value_btc - ai.input_value_btc AS FLOAT64) AS value_difference_btc,

  /* Behavioral Aggregates */
  cb.transaction_count AS customer_total_transactions,
  cb.unique_outputs AS customer_unique_outputs,
  cb.avg_inputs_per_tx AS customer_avg_inputs_per_tx,
  pb.transaction_count AS product_total_transactions,
  pb.unique_inputs AS product_unique_inputs,
  pb.avg_outputs_per_tx AS product_avg_outputs_per_tx,

  /* Time Features in UTC+9 (Asia/Jayapura timezone) */
  TIMESTAMP(
    DATETIME(ai.interaction_timestamp, 'UTC'),
    'Asia/Jayapura'
  ) AS interaction_datetime_utc9,

  EXTRACT(DATE FROM
    TIMESTAMP(DATETIME(ai.interaction_timestamp, 'UTC'), 'Asia/Jayapura')
  ) AS interaction_date_utc9,

  CAST(EXTRACT(HOUR FROM
    TIMESTAMP(DATETIME(ai.interaction_timestamp, 'UTC'), 'Asia/Jayapura')
  ) AS INT64) AS interaction_hour_utc9,

  CAST(EXTRACT(DAYOFWEEK FROM
    TIMESTAMP(DATETIME(ai.interaction_timestamp, 'UTC'), 'Asia/Jayapura')
  ) AS INT64) AS interaction_day_of_week_utc9,

  /* Blockchain-Specific Features */
  COALESCE(ai.block_number, CAST(0 AS INT64)) AS block_number,
  COALESCE(ai.is_coinbase, FALSE) AS is_coinbase_transaction,

  /* Recency Metrics */
  CAST(DATE_DIFF(
    CURRENT_DATE(),
    ai.interaction_timestamp,
    DAY
  ) AS INT64) AS days_since_transaction,

  /* Target Variables for ML */
  CASE
    WHEN ai.output_value_satoshis > ai.input_value_satoshis THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS is_profitable_transaction,

  CASE
    WHEN ai.transaction_fee_satoshis > 0 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS has_transaction_fee,

  CASE
    WHEN ai.is_coinbase = TRUE THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS is_coinbase,

  /* Engagement Score (custom metric) */
  CAST(
    (COALESCE(ai.input_value_btc, 0) * 0.4) +
    (COALESCE(ai.output_value_btc, 0) * 0.4) +
    (COALESCE(cb.transaction_count, 0) * 0.1) +
    (COALESCE(pb.transaction_count, 0) * 0.1) AS FLOAT64
  ) AS engagement_score,

  /* Transaction Value Tier */
  CASE
    WHEN ai.output_value_btc >= 1.0 THEN 'Large'
    WHEN ai.output_value_btc >= 0.1 THEN 'Medium'
    WHEN ai.output_value_btc >= 0.01 THEN 'Small'
    ELSE 'Micro'
  END AS transaction_value_tier,

  /* Network Centrality Score */
  CAST(
    (COALESCE(cb.unique_outputs, 0) * 0.3) +
    (COALESCE(pb.unique_inputs, 0) * 0.3) +
    (COALESCE(ai.output_input_ratio, 0) * 0.4) AS FLOAT64
  ) AS network_centrality_score

FROM address_interactions ai
LEFT JOIN input_addresses ia ON ai.customer_id = ia.customer_id
LEFT JOIN output_addresses oa ON ai.product_id = oa.product_id
LEFT JOIN transaction_categories tc ON
  CASE
    WHEN ai.is_coinbase = TRUE THEN 'Coinbase'
    WHEN ai.input_count > 1 THEN 'Multi-Input'
    WHEN ai.output_count > 2 THEN 'Multi-Output'
    WHEN ai.input_count = 1 AND ai.output_count = 2 THEN 'Standard'
    ELSE 'Complex'
  END = tc.transaction_type
LEFT JOIN customer_behavior cb ON ai.customer_id = cb.customer_id
LEFT JOIN product_behavior pb ON ai.product_id = pb.product_id

/* Filter out invalid records */
WHERE ai.customer_id IS NOT NULL
  AND ai.product_id IS NOT NULL
  AND ai.customer_id != ai.product_id  /* Exclude self-transactions */

/* Deduplicate records - keep the highest value transaction */
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY ai.customer_id, ai.product_id
  ORDER BY
    ai.output_value_satoshis DESC,
    ai.interaction_timestamp DESC
) = 1

/* Optimize for ML training */
ORDER BY
  ai.customer_id,
  ai.product_id,
  ai.output_value_satoshis DESC;