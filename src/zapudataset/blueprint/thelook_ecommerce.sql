/*
=============================================
ADVANCED SQL QUERY FOR PRODUCT RECOMMENDATION ML MODEL
Dataset: bigquery-public-data.thelook_ecommerce
Features: CustomerID, ProductID, Demographics, Behaviors, Counts, Pricing, UTC+9 Timestamps
=============================================
*/

WITH
/* Customer Demographics with Behavioral Metrics */
customer_behavior AS (
  SELECT
    u.id AS customer_id,
    u.gender AS gender,
    CAST(u.age AS INT64) AS age,
    u.country AS country,
    u.city AS city,
    u.state AS state,
    u.traffic_source AS traffic_source,
    /* Behavioral metrics from events */
    CAST(COUNT(DISTINCT CASE WHEN e.event_type = 'purchase' THEN e.id END) AS INT64) AS purchase_count,
    CAST(COUNT(DISTINCT CASE WHEN e.event_type = 'cart' THEN e.id END) AS INT64) AS cart_add_count,
    CAST(COUNT(DISTINCT CASE WHEN e.event_type = 'product' THEN e.id END) AS INT64) AS product_view_count,
    CAST(COUNT(DISTINCT CASE WHEN e.event_type = 'department' THEN e.id END) AS INT64) AS department_view_count,
    CAST(COUNT(DISTINCT e.id) AS INT64) AS total_events,
    /* First and last activity timestamps */
    MIN(e.created_at) AS first_activity_at,
    MAX(e.created_at) AS last_activity_at,
    /* Days since first activity */
    CAST(DATE_DIFF(CURRENT_DATE(), MIN(e.created_at), DAY) AS INT64) AS days_as_customer
  FROM `bigquery-public-data.thelook_ecommerce.users` u
  LEFT JOIN `bigquery-public-data.thelook_ecommerce.events` e
    ON u.id = e.user_id
  WHERE u.id IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8
),

/* Product Purchase History with Pricing */
product_purchases AS (
  SELECT
    oi.user_id AS customer_id,
    oi.product_id AS product_id,
    p.name AS product_name,
    p.category AS category,
    p.brand AS brand,
    CAST(p.retail_price AS FLOAT64) AS retail_price,
    CAST(oi.sale_price AS FLOAT64) AS sale_price,
    oi.status AS order_item_status,
    o.id AS order_id,
    o.created_at AS order_created_at,
    o.status AS order_status,
    CAST(o.num_of_item AS INT64) AS quantity_purchased,
    /* Calculate discount */
    CAST(ROUND(((p.retail_price - oi.sale_price) / NULLIF(p.retail_price, 0)) * 100, 2) AS FLOAT64) AS discount_percentage,
    /* Product popularity metrics */
    CAST(COUNT(DISTINCT oi.id) OVER (PARTITION BY oi.product_id) AS INT64) AS product_purchase_count,
    CAST(AVG(oi.sale_price) OVER (PARTITION BY oi.product_id) AS FLOAT64) AS avg_product_price,
    /* Customer-product interaction frequency */
    CAST(COUNT(DISTINCT oi.id) OVER (PARTITION BY oi.user_id, oi.product_id) AS INT64) AS customer_product_purchases
  FROM `bigquery-public-data.thelook_ecommerce.order_items` oi
  JOIN `bigquery-public-data.thelook_ecommerce.products` p
    ON oi.product_id = p.id
  JOIN `bigquery-public-data.thelook_ecommerce.orders` o
    ON oi.order_id = o.id
  WHERE oi.status NOT IN ('Cancelled', 'Returned')
),

/* Product Click/View Behavior from Events */
product_interactions AS (
  SELECT
    e.user_id AS customer_id,
    /* Extract product_category from URI */
    CASE
      WHEN e.event_type = 'product' THEN
        REGEXP_EXTRACT(e.uri, r'/product/([^/]+)')
      WHEN e.event_type = 'department' THEN
        REGEXP_EXTRACT(e.uri, r'/department/([^/]+)/category')
      ELSE NULL
    END AS product_category,
    e.event_type AS interaction_type,
    e.created_at AS interaction_timestamp,
    /* Count interactions per customer-product */
    CAST(COUNT(*) OVER (PARTITION BY e.user_id,
                   CASE WHEN e.event_type = 'product' THEN REGEXP_EXTRACT(e.uri, r'/product/([^/]+)')
                        WHEN e.event_type = 'department' THEN REGEXP_EXTRACT(e.uri, r'/department/([^/]+)/category')
                        ELSE NULL END) AS INT64) AS interaction_count
  FROM `bigquery-public-data.thelook_ecommerce.events` e
  WHERE e.event_type IN ('product', 'department', 'cart', 'purchase')
),

/* Inventory and Distribution Data */
product_inventory AS (
  SELECT
    ii.product_id AS product_id,
    dc.id AS distribution_center_id,
    dc.name AS distribution_center_name,
    CAST(dc.latitude AS FLOAT64) AS latitude,
    CAST(dc.longitude AS FLOAT64) AS longitude,
    CAST(AVG(ii.cost) AS FLOAT64) AS avg_inventory_cost,
    CAST(COUNT(DISTINCT ii.id) AS INT64) AS inventory_transactions,
    MIN(ii.created_at) AS first_in_stock,
    MAX(ii.sold_at) AS last_sold_at
  FROM `bigquery-public-data.thelook_ecommerce.inventory_items` ii
  JOIN `bigquery-public-data.thelook_ecommerce.distribution_centers` dc
    ON ii.distribution_center_id = dc.id
  GROUP BY 1, 2, 3, 4, 5
)

/*
=============================================
MAIN QUERY: Unified Customer-Product Interaction Dataset
=============================================
*/
SELECT
  /* Core Identifiers */
  COALESCE(pp.customer_id, pi.customer_id) AS customer_id,
  pp.product_id AS product_id,
  pp.product_name AS product_name,
  pp.brand AS brand,
  pp.category AS product_category,

  /* Customer Demographics */
  cb.gender AS gender,
  cb.age AS age,
  cb.country AS country,
  cb.city AS city,
  cb.state AS state,
  cb.traffic_source AS traffic_source,
  cb.days_as_customer AS days_as_customer,

  /* Purchase Behavior Metrics */
  COALESCE(pp.quantity_purchased, CAST(0 AS INT64)) AS total_purchased_quantity,
  COALESCE(pp.customer_product_purchases, CAST(0 AS INT64)) AS customer_product_purchase_count,
  pp.sale_price AS actual_purchase_price,
  pp.retail_price AS list_price,
  pp.discount_percentage AS discount_percentage,
  pp.product_purchase_count AS global_product_popularity,
  pp.avg_product_price AS avg_product_price,

  /* Interaction/Click Behavior */
  CAST(COUNT(DISTINCT CASE WHEN pi.interaction_type = 'product' THEN pi.interaction_timestamp END) AS INT64) AS product_views,
  CAST(COUNT(DISTINCT CASE WHEN pi.interaction_type = 'cart' THEN pi.interaction_timestamp END) AS INT64) AS cart_adds,
  CAST(COUNT(DISTINCT CASE WHEN pi.interaction_type = 'purchase' THEN pi.interaction_timestamp END) AS INT64) AS purchases,
  CAST(MAX(CASE WHEN pi.interaction_type = 'purchase' THEN 1 ELSE 0 END) AS INT64) AS has_purchased,

  /* Pricing Features */
  pp.sale_price AS sale_price,
  pp.retail_price AS retail_price,
  CAST(ROUND(pp.sale_price * pp.quantity_purchased, 2) AS FLOAT64) AS total_spent,
  pp.discount_percentage AS discount_percentage,

  /* Distribution Features */
  pi_dist.distribution_center_id AS distribution_center_id,
  pi_dist.distribution_center_name AS distribution_center_name,
  pi_dist.latitude AS dc_latitude,
  pi_dist.longitude AS dc_longitude,

  /* Time Features in UTC+9 (Asia/Jayapura timezone) */
  TIMESTAMP(
    DATETIME(
      COALESCE(pp.order_created_at, pi.interaction_timestamp, cb.first_activity_at),
      'UTC'
    ),
    'Asia/Jayapura'
  ) AS interaction_datetime_utc9,

  /* Extract time components in UTC+9 */
  EXTRACT(DATE FROM
    TIMESTAMP(
      DATETIME(COALESCE(pp.order_created_at, pi.interaction_timestamp, cb.first_activity_at), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS interaction_date_utc9,

  CAST(EXTRACT(HOUR FROM
    TIMESTAMP(
      DATETIME(COALESCE(pp.order_created_at, pi.interaction_timestamp, cb.first_activity_at), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS INT64) AS interaction_hour_utc9,

  CAST(EXTRACT(DAYOFWEEK FROM
    TIMESTAMP(
      DATETIME(COALESCE(pp.order_created_at, pi.interaction_timestamp, cb.first_activity_at), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS INT64) AS interaction_day_of_week_utc9,

  /* Behavioral Aggregates */
  cb.purchase_count AS customer_total_purchases,
  cb.cart_add_count AS customer_total_cart_adds,
  cb.product_view_count AS customer_total_product_views,
  cb.department_view_count AS customer_total_department_views,
  cb.total_events AS customer_total_events,

  /* Target Variables for ML */
  CASE
    WHEN pp.product_id IS NOT NULL THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS has_purchased_product,

  /* Recency, Frequency, Monetary metrics */
  CAST(DATE_DIFF(
    CURRENT_DATE(),
    COALESCE(pp.order_created_at, cb.last_activity_at),
    DAY
  ) AS INT64) AS days_since_last_interaction,

  /* Customer lifetime value approximation */
  CAST(ROUND(
    SUM(pp.sale_price * pp.quantity_purchased) OVER (
      PARTITION BY COALESCE(pp.customer_id, pi.customer_id)
    ),
    2
  ) AS FLOAT64) AS customer_lifetime_value

FROM product_purchases pp
FULL OUTER JOIN product_interactions pi
  ON pp.customer_id = pi.customer_id
  AND (
    pp.product_id = REGEXP_EXTRACT(pi.uri, r'/product/([^/]+)') OR
    pp.category = REGEXP_EXTRACT(pi.uri, r'/department/([^/]+)/category')
  )
LEFT JOIN customer_behavior cb
  ON COALESCE(pp.customer_id, pi.customer_id) = cb.customer_id
LEFT JOIN product_inventory pi_dist
  ON pp.product_id = pi_dist.product_id

/* Filter out null customer IDs */
WHERE COALESCE(pp.customer_id, pi.customer_id) IS NOT NULL

/* Deduplicate records */
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY
    COALESCE(pp.customer_id, pi.customer_id),
    COALESCE(pp.product_id, REGEXP_EXTRACT(pi.uri, r'/product/([^/]+)')),
    COALESCE(pp.order_created_at, pi.interaction_timestamp)
  ORDER BY
    CASE WHEN pp.product_id IS NOT NULL THEN 0 ELSE 1 END,
    COALESCE(pp.quantity_purchased, 0) DESC
) = 1

/* Optimize for ML training */
ORDER BY
  COALESCE(pp.customer_id, pi.customer_id),
  COALESCE(pp.product_id, REGEXP_EXTRACT(pi.uri, r'/product/([^/]+)')),
  interaction_datetime_utc9 DESC;
