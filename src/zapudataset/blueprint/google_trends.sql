/*
=============================================
ADVANCED SQL QUERY FOR PRODUCT RECOMMENDATION ML MODEL
Dataset: bigquery-public-data.google_trends
Features: CustomerID (Geographic Region), ProductID (Search Term), Demographics, Behaviors, Counts, Pricing Proxy, UTC+9 Timestamps
Note: This dataset contains aggregated Google Trends data. We treat geographic regions as "customers" and search terms as "products".
Tables: top_terms, top_rising_terms, international_top_terms, international_top_rising_terms
=============================================
*/

WITH
/* Geographic Regions as Customers */
region_demographics AS (
  SELECT
    /* US Domestic Market Areas */
    dma_id AS customer_id,
    dma_name AS customer_name,
    'US' AS country,
    'Domestic' AS region_type,
    /* Create demographic proxies from DMA characteristics */
    CASE
      WHEN dma_name LIKE '%New York%' THEN 'Urban'
      WHEN dma_name LIKE '%Los Angeles%' THEN 'Urban'
      WHEN dma_name LIKE '%Chicago%' THEN 'Urban'
      WHEN dma_name LIKE '%Philadelphia%' THEN 'Urban'
      WHEN dma_name LIKE '%Dallas%' THEN 'Urban'
      ELSE 'Regional'
    END AS region_category,
    /* Market size proxy */
    CASE
      WHEN dma_name LIKE '%New York%' THEN 'Tier 1'
      WHEN dma_name LIKE '%Los Angeles%' THEN 'Tier 1'
      WHEN dma_name LIKE '%Chicago%' THEN 'Tier 2'
      ELSE 'Tier 3'
    END AS market_tier
  FROM (
    SELECT DISTINCT dma_id, dma_name
    FROM `bigquery-public-data.google_trends.top_terms`
    WHERE dma_id IS NOT NULL
  )
  UNION ALL
  SELECT
    /* International Regions */
    country_name AS customer_id,
    country_name AS customer_name,
    country_name AS country,
    'International' AS region_type,
    'Country' AS region_category,
    'Tier 1' AS market_tier
  FROM (
    SELECT DISTINCT country_name
    FROM `bigquery-public-data.google_trends.international_top_terms`
    WHERE country_name IS NOT NULL
  )
),

/* Search Terms as Products */
term_features AS (
  SELECT
    term AS product_id,
    term AS product_name,
    /* Term characteristics */
    LENGTH(term) AS term_length,
    CASE
      WHEN term LIKE '%google%' OR term LIKE '%Google%' THEN 'Brand'
      WHEN term LIKE '%how%' OR term LIKE '%what%' OR term LIKE '%why%' THEN 'Question'
      WHEN term LIKE '%news%' OR term LIKE '%update%' THEN 'News'
      WHEN term LIKE '%buy%' OR term LIKE '%sale%' OR term LIKE '%price%' THEN 'Commercial'
      ELSE 'General'
    END AS term_category,
    /* Popularity metrics */
    CAST(AVG(score) AS FLOAT64) AS avg_score,
    CAST(MAX(score) AS FLOAT64) AS max_score,
    CAST(MIN(score) AS FLOAT64) AS min_score,
    CAST(COUNT(DISTINCT week) AS INT64) AS weeks_trending,
    CAST(COUNT(DISTINCT refresh_date) AS INT64) AS days_trending,
    /* Rank metrics */
    CAST(AVG(rank) AS FLOAT64) AS avg_rank,
    CAST(MIN(rank) AS INT64) AS best_rank
  FROM (
    SELECT term, score, rank, week, refresh_date
    FROM `bigquery-public-data.google_trends.top_terms`
    UNION ALL
    SELECT term, score, rank, week, refresh_date
    FROM `bigquery-public-data.google_trends.international_top_terms`
  )
  WHERE term IS NOT NULL
  GROUP BY 1, 2, 3, 4
),

/* Region-Term Interaction Matrix (Customer-Product) */
region_term_interactions AS (
  SELECT
    /* US Data */
    tt.dma_id AS customer_id,
    tt.term AS product_id,
    tt.week AS interaction_week,
    tt.refresh_date AS interaction_date,
    CAST(tt.rank AS INT64) AS rank,
    CAST(tt.score AS FLOAT64) AS score,
    /* Create engagement metrics */
    CASE
      WHEN tt.rank <= 5 THEN CAST(1 AS INT64)
      ELSE CAST(0 AS INT64)
    END AS is_top_5,
    CASE
      WHEN tt.rank <= 10 THEN CAST(1 AS INT64)
      ELSE CAST(0 AS INT64)
    END AS is_top_10,
    /* Time-based features */
    CAST(EXTRACT(DAYOFWEEK FROM tt.refresh_date) AS INT64) AS day_of_week,
    CAST(EXTRACT(MONTH FROM tt.refresh_date) AS INT64) AS month,
    CAST(EXTRACT(YEAR FROM tt.refresh_date) AS INT64) AS year
  FROM `bigquery-public-data.google_trends.top_terms` tt
  WHERE tt.dma_id IS NOT NULL
    AND tt.term IS NOT NULL

  UNION ALL

  SELECT
    /* International Data */
    it.country_name AS customer_id,
    it.term AS product_id,
    it.week AS interaction_week,
    it.refresh_date AS interaction_date,
    CAST(it.rank AS INT64) AS rank,
    CAST(it.score AS FLOAT64) AS score,
    /* Create engagement metrics */
    CASE
      WHEN it.rank <= 5 THEN CAST(1 AS INT64)
      ELSE CAST(0 AS INT64)
    END AS is_top_5,
    CASE
      WHEN it.rank <= 10 THEN CAST(1 AS INT64)
      ELSE CAST(0 AS INT64)
    END AS is_top_10,
    /* Time-based features */
    CAST(EXTRACT(DAYOFWEEK FROM it.refresh_date) AS INT64) AS day_of_week,
    CAST(EXTRACT(MONTH FROM it.refresh_date) AS INT64) AS month,
    CAST(EXTRACT(YEAR FROM it.refresh_date) AS INT64) AS year
  FROM `bigquery-public-data.google_trends.international_top_terms` it
  WHERE it.country_name IS NOT NULL
    AND it.term IS NOT NULL
),

/* Rising Terms Data for Trend Analysis */
rising_terms AS (
  SELECT
    customer_id,
    term AS product_id,
    CAST(COUNT(*) AS INT64) AS rising_count,
    CAST(AVG(score) AS FLOAT64) AS avg_rising_score,
    CAST(MAX(score) AS FLOAT64) AS max_rising_score
  FROM (
    SELECT
      dma_id AS customer_id,
      term,
      score
    FROM `bigquery-public-data.google_trends.top_rising_terms`
    WHERE dma_id IS NOT NULL
    UNION ALL
    SELECT
      country_name AS customer_id,
      term,
      score
    FROM `bigquery-public-data.google_trends.international_top_rising_terms`
    WHERE country_name IS NOT NULL
  )
  GROUP BY 1, 2
),

/* Region Behavioral Metrics */
region_behavior AS (
  SELECT
    customer_id,
    /* Total engagement */
    CAST(COUNT(DISTINCT product_id) AS INT64) AS unique_terms_engaged,
    CAST(COUNT(*) AS INT64) AS total_interactions,
    CAST(AVG(score) AS FLOAT64) AS avg_score,
    CAST(SUM(CASE WHEN rank <= 5 THEN 1 ELSE 0 END) AS INT64) AS top_5_count,
    CAST(SUM(CASE WHEN rank <= 10 THEN 1 ELSE 0 END) AS INT64) AS top_10_count,
    /* Time metrics */
    MIN(refresh_date) AS first_interaction_date,
    MAX(refresh_date) AS last_interaction_date,
    CAST(DATE_DIFF(MAX(refresh_date), MIN(refresh_date), DAY) AS INT64) AS days_active
  FROM region_term_interactions
  GROUP BY 1
)

/*
=============================================
MAIN QUERY: Unified Region-Term Interaction Dataset for ML
=============================================
*/
SELECT
  /* Core Identifiers */
  rti.customer_id AS customer_id,
  rti.product_id AS product_id,
  tf.product_name AS product_name,
  rd.customer_name AS customer_name,
  rd.country AS customer_country,
  rd.region_type AS customer_type,

  /* Customer Demographics */
  rd.region_category AS region_category,
  rd.market_tier AS market_tier,

  /* Product Features */
  tf.term_length AS product_name_length,
  tf.term_category AS product_category,
  tf.avg_score AS product_avg_score,
  tf.max_score AS product_max_score,
  tf.weeks_trending AS product_weeks_trending,
  tf.best_rank AS product_best_rank,

  /* Purchase Behavior Metrics (Search Behavior) */
  COALESCE(rti.rank, CAST(0 AS INT64)) AS interaction_rank,
  COALESCE(rti.score, CAST(0 AS FLOAT64)) AS interaction_score,
  COALESCE(rti.is_top_5, CAST(0 AS INT64)) AS is_top_5,
  COALESCE(rti.is_top_10, CAST(0 AS INT64)) AS is_top_10,
  COALESCE(rt.rising_count, CAST(0 AS INT64)) AS rising_count,
  COALESCE(rt.avg_rising_score, CAST(0 AS FLOAT64)) AS avg_rising_score,

  /* Pricing Proxy Features (using score as popularity/value proxy) */
  COALESCE(rti.score, CAST(0 AS FLOAT64)) AS popularity_score,
  COALESCE(tf.avg_score, CAST(0 AS FLOAT64)) AS product_avg_popularity,
  CAST(COALESCE(rti.score, 0) - COALESCE(tf.avg_score, 0) AS FLOAT64) AS score_difference,

  /* Behavioral Aggregates */
  rb.unique_terms_engaged AS customer_unique_terms,
  rb.total_interactions AS customer_total_interactions,
  rb.avg_score AS customer_avg_score,
  rb.top_5_count AS customer_top_5_count,
  rb.top_10_count AS customer_top_10_count,

  /* Time Features in UTC+9 (Asia/Jayapura timezone) */
  TIMESTAMP(
    DATETIME(
      COALESCE(rti.refresh_date, rb.first_interaction_date),
      'UTC'
    ),
    'Asia/Jayapura'
  ) AS interaction_datetime_utc9,

  EXTRACT(DATE FROM
    TIMESTAMP(
      DATETIME(COALESCE(rti.refresh_date, rb.first_interaction_date), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS interaction_date_utc9,

  CAST(EXTRACT(HOUR FROM
    TIMESTAMP(
      DATETIME(COALESCE(rti.refresh_date, rb.first_interaction_date), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS INT64) AS interaction_hour_utc9,

  CAST(EXTRACT(DAYOFWEEK FROM
    TIMESTAMP(
      DATETIME(COALESCE(rti.refresh_date, rb.first_interaction_date), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS INT64) AS interaction_day_of_week_utc9,

  /* Temporal Features */
  COALESCE(rti.week, '') AS interaction_week,
  COALESCE(rti.month, CAST(0 AS INT64)) AS interaction_month,
  COALESCE(rti.year, CAST(0 AS INT64)) AS interaction_year,

  /* Recency Metrics */
  CAST(DATE_DIFF(
    CURRENT_DATE(),
    COALESCE(rti.refresh_date, rb.last_interaction_date),
    DAY
  ) AS INT64) AS days_since_last_interaction,

  /* Target Variables for ML */
  CASE
    WHEN rti.is_top_5 = 1 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS is_high_priority,

  CASE
    WHEN rti.is_top_10 = 1 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS is_medium_priority,

  CASE
    WHEN rt.rising_count > 0 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS is_rising_term,

  /* Engagement Score (custom metric) */
  CAST(
    (COALESCE(rti.score, 0) * 0.5) +
    (COALESCE(rt.rising_count, 0) * 0.3) +
    (CASE WHEN rti.is_top_5 = 1 THEN 10 ELSE 0 END) AS FLOAT64
  ) AS engagement_score,

  /* Trend Momentum */
  CAST(
    COALESCE(rti.score, 0) *
    (1 + (COALESCE(rt.rising_count, 0) * 0.1)) AS FLOAT64
  ) AS trend_momentum

FROM region_term_interactions rti
LEFT JOIN region_demographics rd ON rti.customer_id = rd.customer_id
LEFT JOIN term_features tf ON rti.product_id = tf.product_id
LEFT JOIN rising_terms rt ON rti.customer_id = rt.customer_id AND rti.product_id = rt.product_id
LEFT JOIN region_behavior rb ON rti.customer_id = rb.customer_id

/* Filter out invalid records */
WHERE rti.customer_id IS NOT NULL
  AND rti.product_id IS NOT NULL

/* Deduplicate records - keep the highest scoring interaction */
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY rti.customer_id, rti.product_id
  ORDER BY
    rti.score DESC,
    rti.is_top_5 DESC,
    rti.refresh_date DESC
) = 1

/* Optimize for ML training */
ORDER BY
  rti.customer_id,
  rti.product_id,
  rti.score DESC;