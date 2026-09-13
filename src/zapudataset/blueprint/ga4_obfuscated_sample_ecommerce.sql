/*
=============================================
ADVANCED SQL QUERY FOR PRODUCT RECOMMENDATION ML MODEL
Dataset: bigquery-public-data.ga4_obfuscated_sample_ecommerce
Features: CustomerID, ProductID, Demographics, Behaviors, Counts, Pricing, UTC+9 Timestamps
Note: This dataset contains obfuscated GA4 event export data from Google Merchandise Store
Tables: events_YYYYMMDD (wildcard pattern for all dates)
=============================================
*/

WITH
/* Customer Demographics and Device Information */
customer_demographics AS (
  SELECT
    user_pseudo_id AS customer_id,
    user_id AS user_id,
    /* Geographic information */
    geo.continent AS continent,
    geo.country AS country,
    geo.region AS region,
    geo.metro AS metro,
    geo.city AS city,
    /* Device information */
    device.category AS device_category,
    device.operating_system AS operating_system,
    device.web_info.browser AS browser,
    device.language AS language,
    /* User lifecycle */
    TIMESTAMP_MICROS(user_first_touch_timestamp) AS first_touch_timestamp,
    /* Traffic source */
    traffic_source.source AS traffic_source,
    traffic_source.medium AS traffic_medium,
    traffic_source.campaign AS traffic_campaign,
    /* User properties */
    CAST(
      (SELECT value.string_value
       FROM UNNEST(user_properties)
       WHERE key = 'user_type'
       LIMIT 1)
     AS STRING) AS user_type
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  WHERE user_pseudo_id IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17
),

/* Product Information with Pricing */
product_features AS (
  SELECT
    item.item_id AS product_id,
    item.item_name AS product_name,
    item.item_brand AS brand,
    item.item_category AS category,
    item.item_category2 AS category2,
    item.item_category3 AS category3,
    item.item_category4 AS category4,
    item.item_category5 AS category5,
    CAST(AVG(item.price) AS FLOAT64) AS avg_price,
    CAST(AVG(item.quantity) AS FLOAT64) AS avg_quantity,
    /* Product popularity metrics */
    CAST(COUNT(DISTINCT user_pseudo_id) AS INT64) AS unique_users_viewed,
    CAST(COUNT(DISTINCT event_date) AS INT64) AS days_available,
    CAST(SUM(item.quantity) AS INT64) AS total_quantity_sold,
    CAST(SUM(item.price * item.quantity) AS FLOAT64) AS total_revenue
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
  WHERE item.item_id IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

/* Customer-Product Interaction Matrix */
customer_product_interactions AS (
  SELECT
    event.user_pseudo_id AS customer_id,
    item.item_id AS product_id,
    /* Event types */
    CAST(COUNT(DISTINCT CASE WHEN event.event_name = 'view_item' THEN event.event_timestamp END) AS INT64) AS view_count,
    CAST(COUNT(DISTINCT CASE WHEN event.event_name = 'add_to_cart' THEN event.event_timestamp END) AS INT64) AS cart_add_count,
    CAST(COUNT(DISTINCT CASE WHEN event.event_name = 'remove_from_cart' THEN event.event_timestamp END) AS INT64) AS cart_remove_count,
    CAST(COUNT(DISTINCT CASE WHEN event.event_name = 'add_to_wishlist' THEN event.event_timestamp END) AS INT64) AS wishlist_count,
    CAST(COUNT(DISTINCT CASE WHEN event.event_name = 'begin_checkout' THEN event.event_timestamp END) AS INT64) AS checkout_start_count,
    CAST(COUNT(DISTINCT CASE WHEN event.event_name = 'purchase' THEN event.event_timestamp END) AS INT64) AS purchase_count,
    /* Pricing from events */
    CAST(AVG(CASE WHEN event.event_name = 'purchase' THEN item.price ELSE NULL END) AS FLOAT64) AS avg_purchase_price,
    CAST(SUM(CASE WHEN event.event_name = 'purchase' THEN item.price * item.quantity ELSE 0 END) AS FLOAT64) AS total_spent,
    CAST(MAX(CASE WHEN event.event_name = 'purchase' THEN item.quantity ELSE NULL END) AS INT64) AS max_quantity_purchased,
    /* Timestamps */
    MIN(event.event_timestamp) AS first_interaction_timestamp,
    MAX(event.event_timestamp) AS last_interaction_timestamp,
    /* Recency */
    CAST(DATE_DIFF(
      PARSE_DATE('%Y%m%d', _TABLE_SUFFIX),
      DATE(MIN(event.event_timestamp)),
      DAY
    ) AS INT64) AS days_since_first_interaction
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(items) AS item
  WHERE event.user_pseudo_id IS NOT NULL
    AND item.item_id IS NOT NULL
  GROUP BY 1, 2
),

/* Ecommerce Transaction Data */
ecommerce_transactions AS (
  SELECT
    event.user_pseudo_id AS customer_id,
    event.event_name AS event_type,
    /* Transaction details */
    ecommerce.transaction_id AS transaction_id,
    CAST(ecommerce.value AS FLOAT64) AS transaction_value,
    ecommerce.currency AS currency,
    CAST(ecommerce.tax AS FLOAT64) AS tax_amount,
    CAST(ecommerce.shipping AS FLOAT64) AS shipping_amount,
    /* Item details from transactions */
    CAST(SUM(ecommerce.items.price * ecommerce.items.quantity) AS FLOAT64) AS items_total_value,
    CAST(SUM(ecommerce.items.quantity) AS INT64) AS total_items_purchased,
    /* Timestamps */
    event.event_timestamp AS transaction_timestamp,
    event.event_date AS transaction_date
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`,
  UNNEST(ecommerce.items) AS ecommerce_items
  WHERE event.event_name = 'purchase'
    AND event.user_pseudo_id IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
),

/* Customer Behavioral Metrics */
customer_behavior AS (
  SELECT
    user_pseudo_id AS customer_id,
    /* Event counts */
    CAST(COUNT(DISTINCT CASE WHEN event_name = 'session_start' THEN event_timestamp END) AS INT64) AS session_count,
    CAST(COUNT(DISTINCT CASE WHEN event_name = 'page_view' THEN event_timestamp END) AS INT64) AS page_view_count,
    CAST(COUNT(DISTINCT CASE WHEN event_name = 'view_item' THEN event_timestamp END) AS INT64) AS item_view_count,
    CAST(COUNT(DISTINCT CASE WHEN event_name = 'add_to_cart' THEN event_timestamp END) AS INT64) AS cart_add_count,
    CAST(COUNT(DISTINCT CASE WHEN event_name = 'purchase' THEN event_timestamp END) AS INT64) AS purchase_count,
    /* Revenue metrics */
    CAST(SUM(CASE WHEN event_name = 'purchase' THEN ecommerce.value ELSE 0 END) AS FLOAT64) AS total_revenue,
    CAST(AVG(CASE WHEN event_name = 'purchase' THEN ecommerce.value ELSE NULL END) AS FLOAT64) AS avg_transaction_value,
    /* Time metrics */
    MIN(event_timestamp) AS first_event_timestamp,
    MAX(event_timestamp) AS last_event_timestamp,
    CAST(DATE_DIFF(
      PARSE_DATE('%Y%m%d', MAX(_TABLE_SUFFIX)),
      DATE(MIN(event_timestamp)),
      DAY
    ) AS INT64) AS customer_lifetime_days
  FROM `bigquery-public-data.ga4_obfuscated_sample_ecommerce.events_*`
  LEFT JOIN UNNEST(ecommerce) AS ecommerce
  WHERE user_pseudo_id IS NOT NULL
  GROUP BY 1
)

/*
=============================================
MAIN QUERY: Unified Customer-Product Interaction Dataset for ML
=============================================
*/
SELECT
  /* Core Identifiers */
  COALESCE(cpi.customer_id, et.customer_id) AS customer_id,
  cpi.product_id AS product_id,
  pf.product_name AS product_name,
  pf.brand AS brand,
  pf.category AS product_category,

  /* Customer Demographics */
  cd.user_id AS user_id,
  cd.continent AS continent,
  cd.country AS country,
  cd.region AS region,
  cd.city AS city,
  cd.device_category AS device_category,
  cd.operating_system AS operating_system,
  cd.browser AS browser,
  cd.language AS language,
  cd.traffic_source AS traffic_source,
  cd.traffic_medium AS traffic_medium,
  cd.traffic_campaign AS traffic_campaign,
  cd.user_type AS user_type,

  /* Purchase Behavior Metrics */
  COALESCE(cpi.view_count, CAST(0 AS INT64)) AS product_views,
  COALESCE(cpi.cart_add_count, CAST(0 AS INT64)) AS product_cart_adds,
  COALESCE(cpi.wishlist_count, CAST(0 AS INT64)) AS product_wishlist_adds,
  COALESCE(cpi.purchase_count, CAST(0 AS INT64)) AS product_purchases,
  COALESCE(cpi.total_spent, CAST(0 AS FLOAT64)) AS total_spent_on_product,
  COALESCE(cpi.avg_purchase_price, CAST(0 AS FLOAT64)) AS avg_purchase_price,
  COALESCE(cpi.max_quantity_purchased, CAST(0 AS INT64)) AS max_quantity_purchased,

  /* Product Features */
  COALESCE(pf.avg_price, CAST(0 AS FLOAT64)) AS product_avg_price,
  COALESCE(pf.total_quantity_sold, CAST(0 AS INT64)) AS product_total_sold,
  COALESCE(pf.total_revenue, CAST(0 AS FLOAT64)) AS product_total_revenue,
  COALESCE(pf.unique_users_viewed, CAST(0 AS INT64)) AS product_unique_viewers,

  /* Pricing Features */
  cpi.avg_purchase_price AS avg_purchase_price,
  pf.avg_price AS product_list_price,
  CAST(COALESCE(cpi.avg_purchase_price, 0) - COALESCE(pf.avg_price, 0) AS FLOAT64) AS price_difference,

  /* Customer Lifetime Value */
  COALESCE(cb.total_revenue, CAST(0 AS FLOAT64)) AS customer_lifetime_value,
  COALESCE(cb.avg_transaction_value, CAST(0 AS FLOAT64)) AS avg_transaction_value,
  COALESCE(cb.session_count, CAST(0 AS INT64)) AS total_sessions,
  COALESCE(cb.page_view_count, CAST(0 AS INT64)) AS total_page_views,
  COALESCE(cb.item_view_count, CAST(0 AS INT64)) AS total_item_views,
  COALESCE(cb.cart_add_count, CAST(0 AS INT64)) AS total_cart_adds,
  COALESCE(cb.purchase_count, CAST(0 AS INT64)) AS total_purchases,

  /* Time Features in UTC+9 (Asia/Jayapura timezone) */
  TIMESTAMP(
    DATETIME(
      COALESCE(
        cpi.last_interaction_timestamp,
        cd.first_touch_timestamp,
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
        cd.first_touch_timestamp,
        cpi.first_interaction_timestamp
      ), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS last_interaction_date_utc9,

  CAST(EXTRACT(HOUR FROM
    TIMESTAMP(
      DATETIME(COALESCE(
        cpi.last_interaction_timestamp,
        cd.first_touch_timestamp,
        cpi.first_interaction_timestamp
      ), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS INT64) AS last_interaction_hour_utc9,

  CAST(EXTRACT(DAYOFWEEK FROM
    TIMESTAMP(
      DATETIME(COALESCE(
        cpi.last_interaction_timestamp,
        cd.first_touch_timestamp,
        cpi.first_interaction_timestamp
      ), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS INT64) AS last_interaction_day_of_week_utc9,

  /* Recency Metrics */
  COALESCE(cpi.days_since_first_interaction, CAST(0 AS INT64)) AS days_since_first_interaction,
  COALESCE(cb.customer_lifetime_days, CAST(0 AS INT64)) AS customer_lifetime_days,

  /* Target Variables for ML */
  CASE
    WHEN cpi.purchase_count > 0 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS has_purchased,

  CASE
    WHEN cpi.cart_add_count > 0 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS has_added_to_cart,

  CASE
    WHEN cpi.view_count > 0 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS has_viewed,

  /* Engagement Score (custom metric) */
  CAST(
    (COALESCE(cpi.view_count, 0) * 0.1) +
    (COALESCE(cpi.cart_add_count, 0) * 0.3) +
    (COALESCE(cpi.purchase_count, 0) * 0.6) +
    (COALESCE(cpi.total_spent, 0) / 10) AS FLOAT64
  ) AS engagement_score,

  /* Price Sensitivity Index */
  CASE
    WHEN pf.avg_price > 0 THEN
      CAST(
        1 - ABS(COALESCE(cpi.avg_purchase_price, 0) - pf.avg_price) / pf.avg_price,
        2
      ) AS FLOAT64
    ELSE CAST(0 AS FLOAT64)
  END AS price_sensitivity_index

FROM customer_product_interactions cpi
LEFT JOIN customer_demographics cd ON cpi.customer_id = cd.customer_id
LEFT JOIN product_features pf ON cpi.product_id = pf.product_id
LEFT JOIN customer_behavior cb ON cpi.customer_id = cb.customer_id
LEFT JOIN ecommerce_transactions et ON cpi.customer_id = et.customer_id

/* Filter out invalid records */
WHERE cpi.customer_id IS NOT NULL
  AND cpi.product_id IS NOT NULL

/* Deduplicate records - keep the most recent interaction */
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY cpi.customer_id, cpi.product_id
  ORDER BY
    cpi.purchase_count DESC,
    cpi.total_spent DESC,
    cpi.last_interaction_timestamp DESC
) = 1

/* Optimize for ML training */
ORDER BY
  cpi.customer_id,
  cpi.product_id,
  cpi.total_spent DESC;
