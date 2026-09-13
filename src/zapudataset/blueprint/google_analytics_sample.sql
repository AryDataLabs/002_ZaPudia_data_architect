/*
=============================================
ADVANCED SQL QUERY FOR PRODUCT RECOMMENDATION ML MODEL
Dataset: bigquery-public-data.google_analytics_sample
Features: CustomerID, ProductID, Demographics, Behaviors, Counts, Pricing, UTC+9 Timestamps
Note: This is Universal Analytics (UA) sample dataset from Google Merchandise Store
Tables: ga_sessions_YYYYMMDD (wildcard pattern for all dates)
Revenue values are stored as integers multiplied by 10^6 (divide by 1,000,000)
=============================================
*/

WITH
/* Customer Demographics and Session Information */
customer_demographics AS (
  SELECT
    fullVisitorId AS customer_id,
    /* Geographic information */
    geoNetwork.continent AS continent,
    geoNetwork.country AS country,
    geoNetwork.region AS region,
    geoNetwork.metro AS metro,
    geoNetwork.city AS city,
    geoNetwork.networkDomain AS network_domain,
    /* Device information */
    device.deviceCategory AS device_category,
    device.operatingSystem AS operating_system,
    device.mobileDeviceInfo.mobileDeviceBranding AS mobile_brand,
    device.mobileDeviceInfo.mobileDeviceModel AS mobile_model,
    device.mobileDeviceInfo.mobileInputSelector AS mobile_input,
    device.mobileDeviceInfo.mobileDeviceMarketingName AS mobile_marketing_name,
    /* Traffic source */
    trafficSource.source AS traffic_source,
    trafficSource.medium AS traffic_medium,
    trafficSource.campaign AS traffic_campaign,
    trafficSource.keyword AS traffic_keyword,
    trafficSource.referralPath AS referral_path,
    /* First visit timestamp */
    MIN(TIMESTAMP_SECONDS(visitStartTime)) AS first_visit_timestamp
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE fullVisitorId IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21
),

/* Product Information with Pricing */
product_features AS (
  SELECT
    hit.product.productSKU AS product_id,
    hit.product.v2ProductName AS product_name,
    hit.product.v2ProductCategory AS product_category,
    hit.product.productBrand AS product_brand,
    hit.product.productVariant AS product_variant,
    /* Pricing from hits */
    CAST(AVG(hit.product.productPrice) AS FLOAT64) AS avg_product_price,
    CAST(AVG(hit.product.productQuantity) AS FLOAT64) AS avg_quantity,
    CAST(AVG(hit.product.productRevenue) AS FLOAT64) AS avg_product_revenue,
    /* Product popularity */
    CAST(COUNT(DISTINCT fullVisitorId) AS INT64) AS unique_customers,
    CAST(COUNT(DISTINCT CONCAT(fullVisitorId, '_', visitId)) AS INT64) AS unique_sessions,
    CAST(SUM(hit.product.productQuantity) AS INT64) AS total_quantity_viewed,
    /* Revenue calculation (divide by 1,000,000 for UA format) */
    CAST(SUM(hit.product.productRevenue) / 1000000 AS FLOAT64) AS total_revenue
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hit
  WHERE hit.product.productSKU IS NOT NULL
    AND hit.type = 'PAGE'
  GROUP BY 1, 2, 3, 4, 5, 6
),

/* Customer-Product Interaction Matrix */
customer_product_interactions AS (
  SELECT
    fullVisitorId AS customer_id,
    hit.product.productSKU AS product_id,
    /* Interaction types */
    CAST(COUNT(DISTINCT CASE WHEN hit.type = 'PAGE' AND hit.page.pagePath LIKE '%product%' THEN hit.hitNumber END) AS INT64) AS product_page_views,
    CAST(COUNT(DISTINCT CASE WHEN hit.eCommerceAction.action_type = '2' THEN hit.hitNumber END) AS INT64) AS product_clicks,
    CAST(COUNT(DISTINCT CASE WHEN hit.eCommerceAction.action_type = '3' THEN hit.hitNumber END) AS INT64) AS add_to_cart_count,
    CAST(COUNT(DISTINCT CASE WHEN hit.eCommerceAction.action_type = '4' THEN hit.hitNumber END) AS INT64) AS remove_from_cart_count,
    CAST(COUNT(DISTINCT CASE WHEN hit.eCommerceAction.action_type = '5' THEN hit.hitNumber END) AS INT64) AS checkout_start_count,
    CAST(COUNT(DISTINCT CASE WHEN hit.eCommerceAction.action_type = '6' THEN hit.hitNumber END) AS INT64) AS purchase_count,
    /* Pricing from interactions */
    CAST(AVG(hit.product.productPrice) AS FLOAT64) AS avg_view_price,
    CAST(SUM(CASE WHEN hit.eCommerceAction.action_type = '6' THEN hit.product.productPrice * hit.product.productQuantity ELSE 0 END) AS FLOAT64) AS total_purchased_value,
    CAST(MAX(hit.product.productQuantity) AS INT64) AS max_quantity_purchased,
    /* Timestamps */
    MIN(TIMESTAMP_SECONDS(visitStartTime)) AS first_interaction_timestamp,
    MAX(TIMESTAMP_SECONDS(visitStartTime)) AS last_interaction_timestamp,
    /* Session count */
    CAST(COUNT(DISTINCT visitId) AS INT64) AS session_count
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hit
  WHERE fullVisitorId IS NOT NULL
    AND hit.product.productSKU IS NOT NULL
  GROUP BY 1, 2
),

/* Transaction Data */
transaction_data AS (
  SELECT
    fullVisitorId AS customer_id,
    hit.transaction.transactionId AS transaction_id,
    /* Transaction details */
    CAST(hit.transaction.transactionRevenue / 1000000 AS FLOAT64) AS transaction_value,
    hit.transaction.transactionCurrency AS currency,
    CAST(hit.transaction.transactionTax / 1000000 AS FLOAT64) AS tax_amount,
    CAST(hit.transaction.transactionShipping / 1000000 AS FLOAT64) AS shipping_amount,
    /* Item details */
    CAST(SUM(hit.transaction.transactionRevenue / 1000000) AS FLOAT64) AS total_transaction_value,
    CAST(SUM(hit.product.productQuantity) AS INT64) AS total_items_purchased,
    /* Timestamps */
    TIMESTAMP_SECONDS(visitStartTime) AS transaction_timestamp,
    date AS transaction_date
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`,
  UNNEST(hits) AS hit
  WHERE hit.transaction.transactionId IS NOT NULL
    AND fullVisitorId IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

/* Customer Behavioral Metrics */
customer_behavior AS (
  SELECT
    fullVisitorId AS customer_id,
    /* Session metrics */
    CAST(COUNT(DISTINCT visitId) AS INT64) AS session_count,
    CAST(SUM(totals.visits) AS INT64) AS total_visits,
    CAST(SUM(totals.pageviews) AS INT64) AS total_pageviews,
    CAST(SUM(totals.bounces) AS INT64) AS total_bounces,
    /* Ecommerce metrics */
    CAST(SUM(totals.transactions) AS INT64) AS total_transactions,
    CAST(SUM(totals.transactionRevenue) / 1000000 AS FLOAT64) AS total_revenue,
    CAST(AVG(CASE WHEN totals.transactions > 0 THEN totals.transactionRevenue / 1000000 ELSE NULL END) AS FLOAT64) AS avg_transaction_value,
    /* Time metrics */
    MIN(TIMESTAMP_SECONDS(visitStartTime)) AS first_visit_timestamp,
    MAX(TIMESTAMP_SECONDS(visitStartTime)) AS last_visit_timestamp,
    CAST(DATE_DIFF(
      PARSE_DATE('%Y%m%d', MAX(_TABLE_SUFFIX)),
      DATE(MIN(TIMESTAMP_SECONDS(visitStartTime))),
      DAY
    ) AS INT64) AS customer_lifetime_days
  FROM `bigquery-public-data.google_analytics_sample.ga_sessions_*`
  WHERE fullVisitorId IS NOT NULL
  GROUP BY 1
)

/*
=============================================
MAIN QUERY: Unified Customer-Product Interaction Dataset for ML
=============================================
*/
SELECT
  /* Core Identifiers */
  COALESCE(cpi.customer_id, td.customer_id) AS customer_id,
  cpi.product_id AS product_id,
  pf.product_name AS product_name,
  pf.product_category AS product_category,
  pf.product_brand AS product_brand,
  pf.product_variant AS product_variant,

  /* Customer Demographics */
  cd.device_category AS device_category,
  cd.operating_system AS operating_system,
  cd.mobile_brand AS mobile_brand,
  cd.country AS country,
  cd.region AS region,
  cd.city AS city,
  cd.traffic_source AS traffic_source,
  cd.traffic_medium AS traffic_medium,
  cd.traffic_campaign AS traffic_campaign,

  /* Purchase Behavior Metrics */
  COALESCE(cpi.product_page_views, CAST(0 AS INT64)) AS product_page_views,
  COALESCE(cpi.product_clicks, CAST(0 AS INT64)) AS product_clicks,
  COALESCE(cpi.add_to_cart_count, CAST(0 AS INT64)) AS add_to_cart_count,
  COALESCE(cpi.purchase_count, CAST(0 AS INT64)) AS purchase_count,
  COALESCE(cpi.total_purchased_value, CAST(0 AS FLOAT64)) AS total_purchased_value,
  COALESCE(cpi.avg_view_price, CAST(0 AS FLOAT64)) AS avg_view_price,
  COALESCE(cpi.max_quantity_purchased, CAST(0 AS INT64)) AS max_quantity_purchased,

  /* Product Features */
  COALESCE(pf.avg_product_price, CAST(0 AS FLOAT64)) AS product_avg_price,
  COALESCE(pf.total_quantity_viewed, CAST(0 AS INT64)) AS product_total_viewed,
  COALESCE(pf.total_revenue, CAST(0 AS FLOAT64)) AS product_total_revenue,
  COALESCE(pf.unique_customers, CAST(0 AS INT64)) AS product_unique_customers,

  /* Pricing Features */
  cpi.avg_view_price AS avg_view_price,
  pf.avg_product_price AS product_list_price,
  CAST(COALESCE(cpi.avg_view_price, 0) - COALESCE(pf.avg_product_price, 0) AS FLOAT64) AS price_difference,

  /* Customer Lifetime Value */
  COALESCE(cb.total_revenue, CAST(0 AS FLOAT64)) AS customer_lifetime_value,
  COALESCE(cb.avg_transaction_value, CAST(0 AS FLOAT64)) AS avg_transaction_value,
  COALESCE(cb.session_count, CAST(0 AS INT64)) AS total_sessions,
  COALESCE(cb.total_visits, CAST(0 AS INT64)) AS total_visits,
  COALESCE(cb.total_pageviews, CAST(0 AS INT64)) AS total_pageviews,
  COALESCE(cb.total_transactions, CAST(0 AS INT64)) AS total_transactions,

  /* Time Features in UTC+9 (Asia/Jayapura timezone) */
  TIMESTAMP(
    DATETIME(
      COALESCE(
        cpi.last_interaction_timestamp,
        cd.first_visit_timestamp,
        cpi.first_interaction_timestamp
      ),
      'UTC'
    ),
    'Asia/Jayapura'
  ) AS last_interaction_datetime_utc9,

  EXTRACT(DATE FROM
    TIMESTAMP(
      DATETIME(COALESCE(
        cpi.last_interaction_timestamp,
        cd.first_visit_timestamp,
        cpi.first_interaction_timestamp
      ), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS last_interaction_date_utc9,

  CAST(EXTRACT(HOUR FROM
    TIMESTAMP(
      DATETIME(COALESCE(
        cpi.last_interaction_timestamp,
        cd.first_visit_timestamp,
        cpi.first_interaction_timestamp
      ), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS INT64) AS last_interaction_hour_utc9,

  CAST(EXTRACT(DAYOFWEEK FROM
    TIMESTAMP(
      DATETIME(COALESCE(
        cpi.last_interaction_timestamp,
        cd.first_visit_timestamp,
        cpi.first_interaction_timestamp
      ), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS INT64) AS last_interaction_day_of_week_utc9,

  /* Recency Metrics */
  COALESCE(cpi.session_count, CAST(0 AS INT64)) AS customer_product_sessions,
  COALESCE(cb.customer_lifetime_days, CAST(0 AS INT64)) AS customer_lifetime_days,

  /* Target Variables for ML */
  CASE
    WHEN cpi.purchase_count > 0 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS has_purchased,

  CASE
    WHEN cpi.add_to_cart_count > 0 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS has_added_to_cart,

  CASE
    WHEN cpi.product_page_views > 0 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS has_viewed_product,

  /* Engagement Score (custom metric) */
  CAST(
    (COALESCE(cpi.product_page_views, 0) * 0.1) +
    (COALESCE(cpi.add_to_cart_count, 0) * 0.3) +
    (COALESCE(cpi.purchase_count, 0) * 0.6) +
    (COALESCE(cpi.total_purchased_value, 0) / 10) AS FLOAT64
  ) AS engagement_score,

  /* Price Sensitivity Index */
  CASE
    WHEN pf.avg_product_price > 0 THEN
      CAST(
        1 - ABS(COALESCE(cpi.avg_view_price, 0) - pf.avg_product_price) / pf.avg_product_price,
        2
      ) AS FLOAT64
    ELSE CAST(0 AS FLOAT64)
  END AS price_sensitivity_index,

  /* Conversion Rate */
  CASE
    WHEN cpi.product_page_views > 0 THEN
      CAST(cpi.purchase_count * 100.0 / cpi.product_page_views AS FLOAT64)
    ELSE CAST(0 AS FLOAT64)
  END AS conversion_rate_percentage

FROM customer_product_interactions cpi
LEFT JOIN customer_demographics cd ON cpi.customer_id = cd.customer_id
LEFT JOIN product_features pf ON cpi.product_id = pf.product_id
LEFT JOIN customer_behavior cb ON cpi.customer_id = cb.customer_id
LEFT JOIN transaction_data td ON cpi.customer_id = td.customer_id

/* Filter out invalid records */
WHERE cpi.customer_id IS NOT NULL
  AND cpi.product_id IS NOT NULL

/* Deduplicate records - keep the most engaged interaction */
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY cpi.customer_id, cpi.product_id
  ORDER BY
    cpi.purchase_count DESC,
    cpi.total_purchased_value DESC,
    cpi.add_to_cart_count DESC,
    cpi.product_page_views DESC,
    cpi.last_interaction_timestamp DESC
) = 1

/* Optimize for ML training */
ORDER BY
  cpi.customer_id,
  cpi.product_id,
  cpi.total_purchased_value DESC;

