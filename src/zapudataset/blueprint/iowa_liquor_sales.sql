/*
=============================================
ADVANCED SQL QUERY FOR PRODUCT RECOMMENDATION ML MODEL
Dataset: bigquery-public-data.iowa_liquor_sales
Features: CustomerID (Store Proxy), ProductID, Demographics (Store), Behaviors, Counts, Pricing, UTC+9 Timestamps
=============================================
*/

WITH
/* Store Demographics (treating stores as "customers") */
store_demographics AS (
  SELECT
    store_number AS customer_id,
    store_name AS customer_name,
    address AS address,
    city AS city,
    zip_code AS zip_code,
    county AS county,
    county_number AS county_number,
    /* Extract geographic features */
    SPLIT(address, ',')[OFFSET(0)] AS street_address,
    /* Create demographic proxies from store characteristics */
    CASE
      WHEN store_name LIKE '%HY-VEE%' THEN 'Supermarket Chain'
      WHEN store_name LIKE '%FAREWAY%' THEN 'Regional Grocery'
      WHEN store_name LIKE '%WALMART%' THEN 'Big Box Retail'
      WHEN store_name LIKE '%QUICK%' OR store_name LIKE '%CONVENIENCE%' THEN 'Convenience Store'
      ELSE 'Other Retail'
    END AS store_type,
    /* Store size proxy */
    CASE
      WHEN store_name LIKE '%#%' THEN 'Chain Store'
      ELSE 'Independent Store'
    END AS store_size_category
  FROM `bigquery-public-data.iowa_liquor_sales.sales`
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

/* Product Information with Enhanced Features */
product_features AS (
  SELECT
    item_number AS product_id,
    item_description AS product_name,
    category AS category,
    category_name AS category_name,
    vendor_number AS vendor_number,
    vendor_name AS vendor_name,
    CAST(pack AS INT64) AS pack,
    CAST(bottle_volume_ml AS INT64) AS bottle_volume_ml,
    /* Price features */
    CAST(AVG(state_bottle_cost) AS FLOAT64) AS avg_cost,
    CAST(AVG(state_bottle_retail) AS FLOAT64) AS avg_retail_price,
    CAST(AVG(sale_dollars) AS FLOAT64) AS avg_sale_dollars,
    /* Volume features */
    CAST(AVG(volume_sold_liters) AS FLOAT64) AS avg_volume_liters,
    CAST(AVG(volume_sold_gallons) AS FLOAT64) AS avg_volume_gallons,
    /* Popularity metrics */
    CAST(COUNT(DISTINCT store_number) AS INT64) AS stores_selling,
    CAST(COUNT(DISTINCT date) AS INT64) AS days_available,
    CAST(SUM(bottles_sold) AS INT64) AS total_bottles_sold,
    CAST(SUM(sale_dollars) AS FLOAT64) AS total_revenue,
    /* Price per liter */
    CAST(ROUND(AVG(sale_dollars / NULLIF(volume_sold_liters, 0)), 2) AS FLOAT64) AS avg_price_per_liter,
    /* Category popularity */
    CAST(COUNT(*) OVER (PARTITION BY category_name) AS INT64) AS category_popularity
  FROM `bigquery-public-data.iowa_liquor_sales.sales`
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

/* Store-Product Interaction Matrix */
store_product_interactions AS (
  SELECT
    store_number AS customer_id,
    item_number AS product_id,
    category_name AS category_name,
    vendor_name AS vendor_name,
    /* Purchase behavior */
    CAST(COUNT(DISTINCT date) AS INT64) AS purchase_frequency,
    CAST(SUM(bottles_sold) AS INT64) AS total_bottles_purchased,
    CAST(SUM(sale_dollars) AS FLOAT64) AS total_spent,
    CAST(AVG(state_bottle_retail) AS FLOAT64) AS avg_price_per_bottle,
    CAST(AVG(volume_sold_liters) AS FLOAT64) AS avg_volume_per_transaction,
    /* Recency */
    MAX(date) AS last_purchase_date,
    MIN(date) AS first_purchase_date,
    CAST(DATE_DIFF(MAX(date), MIN(date), DAY) AS INT64) AS customer_relationship_days,
    /* Seasonality features */
    CAST(COUNT(DISTINCT EXTRACT(MONTH FROM date)) AS INT64) AS months_purchased,
    CAST(COUNT(DISTINCT EXTRACT(DAYOFWEEK FROM date)) AS INT64) AS days_of_week_purchased,
    /* Price sensitivity */
    CAST(STDDEV(state_bottle_retail) AS FLOAT64) AS price_variation,
    /* Volume patterns */
    CAST(AVG(pack) AS FLOAT64) AS avg_pack_size,
    CAST(STDDEV(bottles_sold) AS FLOAT64) AS purchase_quantity_variation
  FROM `bigquery-public-data.iowa_liquor_sales.sales`
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

/* Store Behavioral Metrics */
store_behavior AS (
  SELECT
    store_number AS customer_id,
    /* Overall purchasing behavior */
    CAST(COUNT(DISTINCT item_number) AS INT64) AS unique_products_purchased,
    CAST(COUNT(DISTINCT category_name) AS INT64) AS unique_categories_purchased,
    CAST(COUNT(DISTINCT vendor_name) AS INT64) AS unique_vendors_purchased,
    /* Spending patterns */
    CAST(SUM(sale_dollars) AS FLOAT64) AS total_spent_all_time,
    CAST(AVG(sale_dollars) AS FLOAT64) AS avg_transaction_value,
    CAST(SUM(bottles_sold) AS INT64) AS total_bottles_all_time,
    /* Category preferences */
    STRING_AGG(DISTINCT category_name, ', ' ORDER BY SUM(sale_dollars) DESC LIMIT 3) AS top_categories,
    /* Temporal patterns */
    CAST(COUNT(DISTINCT date) AS INT64) AS active_days,
    CAST(DATE_DIFF(MAX(date), MIN(date), DAY) AS INT64) AS days_active,
    /* Seasonality */
    CAST(COUNT(DISTINCT EXTRACT(MONTH FROM date)) AS INT64) AS active_months,
    /* Recency */
    CAST(DATE_DIFF(CURRENT_DATE(), MAX(date), DAY) AS INT64) AS days_since_last_purchase
  FROM `bigquery-public-data.iowa_liquor_sales.sales`
  GROUP BY 1
)

/*
=============================================
MAIN QUERY: Unified Store-Product Interaction Dataset
=============================================
*/
SELECT
  /* Core Identifiers */
  spi.customer_id AS customer_id,
  spi.product_id AS product_id,
  pf.product_name AS product_name,
  pf.category AS category,
  pf.category_name AS category_name,
  pf.vendor_name AS vendor_name,
  spi.vendor_name AS vendor_from_interaction,

  /* Store Demographics (Customer Proxy) */
  sd.customer_name AS store_name,
  sd.city AS store_city,
  sd.zip_code AS store_zip_code,
  sd.county AS store_county,
  sd.store_type AS store_type,
  sd.store_size_category AS store_size_category,

  /* Purchase Behavior Metrics */
  spi.purchase_frequency AS purchase_frequency,
  spi.total_bottles_purchased AS total_bottles_purchased,
  spi.total_spent AS total_spent,
  spi.avg_price_per_bottle AS avg_price_per_bottle,
  spi.avg_volume_per_transaction AS avg_volume_per_transaction,
  spi.avg_pack_size AS avg_pack_size,
  spi.purchase_quantity_variation AS purchase_quantity_variation,

  /* Pricing Features */
  pf.avg_cost AS product_avg_cost,
  pf.avg_retail_price AS product_avg_retail_price,
  pf.avg_price_per_liter AS avg_price_per_liter,
  spi.avg_price_per_bottle AS store_avg_price_per_bottle,
  /* Price difference (store vs product average) */
  CAST(ROUND(spi.avg_price_per_bottle - pf.avg_retail_price, 2) AS FLOAT64) AS price_difference_from_avg,

  /* Product Popularity */
  pf.stores_selling AS product_availability,
  pf.total_bottles_sold AS product_total_sales_volume,
  pf.total_revenue AS product_total_revenue,
  pf.category_popularity AS category_popularity,

  /* Time Features in UTC+9 (Asia/Jayapura timezone) */
  TIMESTAMP(
    DATETIME(spi.last_purchase_date, 'America/Chicago'),
    'Asia/Jayapura'
  ) AS last_purchase_utc9,

  TIMESTAMP(
    DATETIME(spi.first_purchase_date, 'America/Chicago'),
    'Asia/Jayapura'
  ) AS first_purchase_utc9,

  /* Extract time components in UTC+9 */
  EXTRACT(DATE FROM
    TIMESTAMP(DATETIME(spi.last_purchase_date, 'America/Chicago'), 'Asia/Jayapura')
  ) AS last_purchase_date_utc9,

  CAST(EXTRACT(HOUR FROM
    TIMESTAMP(DATETIME(spi.last_purchase_date, 'America/Chicago'), 'Asia/Jayapura')
  ) AS INT64) AS last_purchase_hour_utc9,

  CAST(EXTRACT(DAYOFWEEK FROM
    TIMESTAMP(DATETIME(spi.last_purchase_date, 'America/Chicago'), 'Asia/Jayapura')
  ) AS INT64) AS last_purchase_day_of_week_utc9,

  /* Store Behavioral Aggregates */
  sb.unique_products_purchased AS store_unique_products,
  sb.unique_categories_purchased AS store_unique_categories,
  sb.total_spent_all_time AS store_lifetime_value,
  sb.avg_transaction_value AS avg_transaction_value,
  sb.active_days AS active_days,
  sb.days_active AS store_days_active,
  sb.days_since_last_purchase AS store_recency,

  /* Target Variables for ML */
  CASE
    WHEN spi.total_bottles_purchased > 0 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS has_purchased,

  /* Recency, Frequency, Monetary for store-product */
  CAST(DATE_DIFF(
    CURRENT_DATE(),
    spi.last_purchase_date,
    DAY
  ) AS INT64) AS days_since_last_purchase,

  /* Store-product relationship strength */
  CAST(ROUND(
    CASE
      WHEN sb.total_spent_all_time > 0 THEN
        (spi.total_spent / sb.total_spent_all_time) * 100
      ELSE 0
    END,
    2
  ) AS FLOAT64) AS product_share_of_store_spending,

  /* Price elasticity proxy */
  CAST(CASE
    WHEN spi.price_variation > 0 AND spi.purchase_frequency > 1 THEN
      ROUND(spi.purchase_quantity_variation / spi.price_variation, 2)
    ELSE 0
  END AS FLOAT64) AS price_elasticity_proxy,

  /* Geographic Features */
  sd.city AS store_city,
  sd.county AS store_county,
  sd.zip_code AS store_zip_code

FROM store_product_interactions spi
JOIN product_features pf ON spi.product_id = pf.product_id
JOIN store_demographics sd ON spi.customer_id = sd.customer_id
JOIN store_behavior sb ON spi.customer_id = sb.customer_id

/* Filter out invalid records */
WHERE spi.customer_id IS NOT NULL
  AND spi.product_id IS NOT NULL
  AND spi.total_bottles_purchased > 0

/* Deduplicate records */
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY spi.customer_id, spi.product_id
  ORDER BY spi.total_spent DESC, spi.purchase_frequency DESC
) = 1

/* Optimize for ML training */
ORDER BY
  spi.customer_id,
  spi.product_id,
  spi.total_spent DESC;
