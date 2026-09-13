/*
=============================================
ADVANCED SQL QUERY FOR PRODUCT RECOMMENDATION ML MODEL
Dataset: bigquery-public-data.chicago_taxi_trips
Features: CustomerID (Taxi/Pickup Area), ProductID (Dropoff Area/Trip Type), Demographics, Behaviors, Counts, Pricing, UTC+9 Timestamps
Note: This dataset contains Chicago taxi trip data. We treat taxis/pickup areas as "customers" and dropoff areas/trip types as "products".
Tables: taxi_trips
=============================================
*/

WITH
/* Taxi Information as Customers */
taxi_demographics AS (
  SELECT
    taxi_id AS customer_id,
    company AS customer_company,
    /* Taxi characteristics */
    CASE
      WHEN company LIKE '%Flash%' THEN 'Flash Cab'
      WHEN company LIKE '%Checker%' THEN 'Checker'
      WHEN company LIKE '%Yellow%' THEN 'Yellow Cab'
      WHEN company LIKE '%Medallion%' THEN 'Medallion'
      ELSE 'Other'
    END AS company_category,
    /* Vehicle type proxy */
    CASE
      WHEN company LIKE '%Flash%' OR company LIKE '%Checker%' THEN 'Standard'
      WHEN company LIKE '%Yellow%' THEN 'Premium'
      ELSE 'Regular'
    END AS vehicle_type,
    /* First and last activity */
    MIN(trip_start_timestamp) AS first_trip_timestamp,
    MAX(trip_start_timestamp) AS last_trip_timestamp,
    /* Activity metrics */
    CAST(COUNT(*) AS INT64) AS total_trips,
    CAST(AVG(trip_miles) AS FLOAT64) AS avg_trip_miles,
    CAST(AVG(trip_seconds) AS FLOAT64) AS avg_trip_seconds,
    /* Revenue metrics */
    CAST(SUM(fare) AS FLOAT64) AS total_fare,
    CAST(SUM(tips) AS FLOAT64) AS total_tips,
    CAST(SUM(tolls) AS FLOAT64) AS total_tolls,
    CAST(SUM(extras) AS FLOAT64) AS total_extras,
    CAST(SUM(trip_total) AS FLOAT64) AS total_revenue
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE taxi_id IS NOT NULL
  GROUP BY 1, 2, 3, 4, 5
),

/* Pickup Community Areas as Customers (Alternative) */
pickup_areas AS (
  SELECT
    pickup_community_area AS customer_id,
    /* Community area characteristics */
    CASE
      WHEN pickup_community_area = 76 THEN 'O''Hare Airport'
      WHEN pickup_community_area = 32 THEN 'Downtown'
      WHEN pickup_community_area = 8 THEN 'Loop'
      WHEN pickup_community_area BETWEEN 1 AND 10 THEN 'Central'
      WHEN pickup_community_area BETWEEN 11 AND 30 THEN 'North'
      WHEN pickup_community_area BETWEEN 31 AND 50 THEN 'South'
      WHEN pickup_community_area BETWEEN 51 AND 77 THEN 'West'
      ELSE 'Other'
    END AS area_category,
    /* Location info */
    CAST(AVG(pickup_latitude) AS FLOAT64) AS avg_latitude,
    CAST(AVG(pickup_longitude) AS FLOAT64) AS avg_longitude,
    /* Activity metrics */
    CAST(COUNT(*) AS INT64) AS total_trips_from_area,
    CAST(AVG(trip_miles) AS FLOAT64) AS avg_trip_miles_from_area,
    CAST(AVG(fare) AS FLOAT64) AS avg_fare_from_area,
    CAST(SUM(trip_total) AS FLOAT64) AS total_revenue_from_area
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE pickup_community_area IS NOT NULL
    AND pickup_community_area != ''
  GROUP BY 1, 2
),

/* Dropoff Community Areas as Products */
dropoff_products AS (
  SELECT
    dropoff_community_area AS product_id,
    /* Community area characteristics */
    CASE
      WHEN dropoff_community_area = 76 THEN 'O''Hare Airport'
      WHEN dropoff_community_area = 32 THEN 'Downtown'
      WHEN dropoff_community_area = 8 THEN 'Loop'
      WHEN dropoff_community_area BETWEEN 1 AND 10 THEN 'Central'
      WHEN dropoff_community_area BETWEEN 11 AND 30 THEN 'North'
      WHEN dropoff_community_area BETWEEN 31 AND 50 THEN 'South'
      WHEN dropoff_community_area BETWEEN 51 AND 77 THEN 'West'
      ELSE 'Other'
    END AS product_category,
    /* Location info */
    CAST(AVG(dropoff_latitude) AS FLOAT64) AS avg_latitude,
    CAST(AVG(dropoff_longitude) AS FLOAT64) AS avg_longitude,
    /* Activity metrics */
    CAST(COUNT(*) AS INT64) AS total_trips_to_area,
    CAST(AVG(trip_miles) AS FLOAT64) AS avg_trip_miles_to_area,
    CAST(AVG(fare) AS FLOAT64) AS avg_fare_to_area,
    CAST(SUM(trip_total) AS FLOAT64) AS total_revenue_to_area,
    /* Popularity metrics */
    CAST(COUNT(DISTINCT taxi_id) AS INT64) AS unique_taxis_serving,
    CAST(COUNT(DISTINCT pickup_community_area) AS INT64) AS unique_pickup_areas
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE dropoff_community_area IS NOT NULL
    AND dropoff_community_area != ''
  GROUP BY 1, 2
),

/* Trip Types as Product Categories */
trip_categories AS (
  SELECT
    CASE
      WHEN trip_miles < 1 THEN 'Short'
      WHEN trip_miles < 5 THEN 'Medium'
      WHEN trip_miles < 15 THEN 'Long'
      ELSE 'Extra Long'
    END AS trip_distance_category,
    CASE
      WHEN fare < 10 THEN 'Low Fare'
      WHEN fare < 25 THEN 'Medium Fare'
      WHEN fare < 50 THEN 'High Fare'
      ELSE 'Premium Fare'
    END AS fare_category,
    CASE
      WHEN payment_type = 'Credit Card' THEN 'Card'
      WHEN payment_type = 'Cash' THEN 'Cash'
      WHEN payment_type = 'Mobile' THEN 'Mobile'
      WHEN payment_type = 'Dispute' THEN 'Dispute'
      ELSE 'Other'
    END AS payment_category,
    /* Category metrics */
    CAST(COUNT(*) AS INT64) AS trip_count,
    CAST(AVG(fare) AS FLOAT64) AS avg_fare,
    CAST(AVG(trip_miles) AS FLOAT64) AS avg_distance,
    CAST(AVG(trip_seconds) AS FLOAT64) AS avg_duration
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  GROUP BY 1, 2, 3
),

/* Customer-Product Interaction Matrix (Taxi-Dropoff Area) */
taxi_dropoff_interactions AS (
  SELECT
    taxi_id AS customer_id,
    dropoff_community_area AS product_id,
    /* Trip details */
    CAST(COUNT(*) AS INT64) AS trip_count,
    CAST(AVG(fare) AS FLOAT64) AS avg_fare,
    CAST(AVG(tips) AS FLOAT64) AS avg_tips,
    CAST(AVG(tolls) AS FLOAT64) AS avg_tolls,
    CAST(AVG(extras) AS FLOAT64) AS avg_extras,
    CAST(AVG(trip_total) AS FLOAT64) AS avg_trip_total,
    CAST(AVG(trip_miles) AS FLOAT64) AS avg_distance_miles,
    CAST(AVG(trip_seconds) AS FLOAT64) AS avg_duration_seconds,
    /* Timestamps */
    MIN(trip_start_timestamp) AS first_trip_timestamp,
    MAX(trip_start_timestamp) AS last_trip_timestamp,
    /* Payment info */
    MODE(payment_type) AS primary_payment_type,
    /* Time of day patterns */
    CAST(COUNT(DISTINCT EXTRACT(HOUR FROM trip_start_timestamp)) AS INT64) AS hours_active,
    CAST(COUNT(DISTINCT EXTRACT(DAYOFWEEK FROM trip_start_timestamp)) AS INT64) AS days_active
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE taxi_id IS NOT NULL
    AND dropoff_community_area IS NOT NULL
    AND dropoff_community_area != ''
  GROUP BY 1, 2
),

/* Pickup-Dropoff Interaction Matrix (Area-Area) */
area_interactions AS (
  SELECT
    pickup_community_area AS customer_id,
    dropoff_community_area AS product_id,
    /* Trip details */
    CAST(COUNT(*) AS INT64) AS trip_count,
    CAST(AVG(fare) AS FLOAT64) AS avg_fare,
    CAST(AVG(tips) AS FLOAT64) AS avg_tips,
    CAST(AVG(tolls) AS FLOAT64) AS avg_tolls,
    CAST(AVG(extras) AS FLOAT64) AS avg_extras,
    CAST(AVG(trip_total) AS FLOAT64) AS avg_trip_total,
    CAST(AVG(trip_miles) AS FLOAT64) AS avg_distance_miles,
    CAST(AVG(trip_seconds) AS FLOAT64) AS avg_duration_seconds,
    /* Timestamps */
    MIN(trip_start_timestamp) AS first_trip_timestamp,
    MAX(trip_start_timestamp) AS last_trip_timestamp,
    /* Taxi diversity */
    CAST(COUNT(DISTINCT taxi_id) AS INT64) AS unique_taxis,
    CAST(COUNT(DISTINCT company) AS INT64) AS unique_companies
  FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
  WHERE pickup_community_area IS NOT NULL
    AND pickup_community_area != ''
    AND dropoff_community_area IS NOT NULL
    AND dropoff_community_area != ''
  GROUP BY 1, 2
),

/* Customer Behavioral Metrics */
customer_behavior AS (
  SELECT
    customer_id,
    /* Trip metrics */
    CAST(COUNT(*) AS INT64) AS total_trips,
    CAST(AVG(fare) AS FLOAT64) AS avg_fare,
    CAST(AVG(tips) AS FLOAT64) AS avg_tips,
    CAST(AVG(trip_miles) AS FLOAT64) AS avg_distance,
    CAST(AVG(trip_seconds) AS FLOAT64) AS avg_duration,
    /* Revenue metrics */
    CAST(SUM(fare) AS FLOAT64) AS total_fare,
    CAST(SUM(tips) AS FLOAT64) AS total_tips,
    CAST(SUM(trip_total) AS FLOAT64) AS total_revenue,
    /* Temporal metrics */
    MIN(trip_start_timestamp) AS first_trip_timestamp,
    MAX(trip_start_timestamp) AS last_trip_timestamp,
    CAST(DATE_DIFF(
      MAX(trip_start_timestamp),
      MIN(trip_start_timestamp),
      DAY
    ) AS INT64) AS days_active,
    /* Location metrics */
    CAST(COUNT(DISTINCT dropoff_community_area) AS INT64) AS unique_dropoff_areas
  FROM (
    SELECT
      taxi_id AS customer_id,
      fare, tips, trip_miles, trip_seconds, trip_total,
      dropoff_community_area,
      trip_start_timestamp
    FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
    WHERE taxi_id IS NOT NULL
  )
  GROUP BY 1
)

/*
=============================================
MAIN QUERY: Unified Taxi-Dropoff Area Interaction Dataset for ML
=============================================
*/
SELECT
  /* Core Identifiers */
  COALESCE(tdi.customer_id, ai.customer_id) AS customer_id,
  COALESCE(tdi.product_id, ai.product_id) AS product_id,
  td.company AS customer_company,
  td.company_category AS customer_company_category,
  dp.product_category AS product_category,

  /* Customer Demographics */
  td.vehicle_type AS customer_vehicle_type,
  COALESCE(td.total_trips, CAST(0 AS INT64)) AS customer_total_trips,
  COALESCE(td.avg_trip_miles, CAST(0 AS FLOAT64)) AS customer_avg_trip_miles,
  COALESCE(td.total_revenue, CAST(0 AS FLOAT64)) AS customer_total_revenue,

  /* Product Features */
  dp.avg_fare_to_area AS product_avg_fare,
  dp.avg_trip_miles_to_area AS product_avg_distance,
  dp.total_revenue_to_area AS product_total_revenue,
  dp.unique_taxis_serving AS product_unique_customers,
  dp.unique_pickup_areas AS product_unique_sources,

  /* Purchase Behavior Metrics */
  COALESCE(tdi.trip_count, CAST(0 AS INT64)) AS interaction_count,
  COALESCE(tdi.avg_fare, CAST(0 AS FLOAT64)) AS avg_fare,
  COALESCE(tdi.avg_tips, CAST(0 AS FLOAT64)) AS avg_tips,
  COALESCE(tdi.avg_tolls, CAST(0 AS FLOAT64)) AS avg_tolls,
  COALESCE(tdi.avg_extras, CAST(0 AS FLOAT64)) AS avg_extras,
  COALESCE(tdi.avg_trip_total, CAST(0 AS FLOAT64)) AS avg_trip_total,
  COALESCE(tdi.avg_distance_miles, CAST(0 AS FLOAT64)) AS avg_distance_miles,
  COALESCE(tdi.avg_duration_seconds, CAST(0 AS FLOAT64)) AS avg_duration_seconds,

  /* Pricing Features */
  tdi.avg_fare AS avg_fare,
  tdi.avg_tips AS avg_tips,
  tdi.avg_tolls AS avg_tolls,
  tdi.avg_extras AS avg_extras,
  tdi.avg_trip_total AS avg_trip_total,
  CAST(tdi.avg_tips / NULLIF(tdi.avg_fare, 0) * 100 AS FLOAT64) AS avg_tip_percentage,

  /* Behavioral Aggregates */
  cb.total_trips AS customer_total_trips,
  cb.avg_fare AS customer_avg_fare,
  cb.total_revenue AS customer_total_revenue,
  cb.days_active AS customer_days_active,
  cb.unique_dropoff_areas AS customer_unique_products,

  /* Time Features in UTC+9 (Asia/Jayapura timezone) */
  TIMESTAMP(
    DATETIME(
      COALESCE(tdi.last_trip_timestamp, ai.last_trip_timestamp),
      'UTC'
    ),
    'Asia/Jayapura'
  ) AS interaction_datetime_utc9,

  EXTRACT(DATE FROM
    TIMESTAMP(
      DATETIME(COALESCE(tdi.last_trip_timestamp, ai.last_trip_timestamp), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS interaction_date_utc9,

  CAST(EXTRACT(HOUR FROM
    TIMESTAMP(
      DATETIME(COALESCE(tdi.last_trip_timestamp, ai.last_trip_timestamp), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS INT64) AS interaction_hour_utc9,

  CAST(EXTRACT(DAYOFWEEK FROM
    TIMESTAMP(
      DATETIME(COALESCE(tdi.last_trip_timestamp, ai.last_trip_timestamp), 'UTC'),
      'Asia/Jayapura'
    )
  ) AS INT64) AS interaction_day_of_week_utc9,

  /* Trip Characteristics */
  COALESCE(tdi.primary_payment_type, '') AS primary_payment_type,
  COALESCE(tdi.hours_active, CAST(0 AS INT64)) AS hours_active,
  COALESCE(tdi.days_active, CAST(0 AS INT64)) AS days_active,

  /* Recency Metrics */
  CAST(DATE_DIFF(
    CURRENT_DATE(),
    COALESCE(tdi.last_trip_timestamp, ai.last_trip_timestamp),
    DAY
  ) AS INT64) AS days_since_last_interaction,

  /* Target Variables for ML */
  CASE
    WHEN tdi.trip_count > 5 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS is_frequent_route,

  CASE
    WHEN tdi.avg_trip_total > 50 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS is_high_value_route,

  CASE
    WHEN tdi.avg_tip_percentage > 20 THEN CAST(1 AS INT64)
    ELSE CAST(0 AS INT64)
  END AS is_high_tip_route,

  /* Engagement Score (custom metric) */
  CAST(
    (COALESCE(tdi.trip_count, 0) * 0.3) +
    (COALESCE(tdi.avg_trip_total, 0) * 0.3) +
    (COALESCE(tdi.avg_distance_miles, 0) * 0.2) +
    (COALESCE(tdi.avg_tip_percentage, 0) * 0.2) AS FLOAT64
  ) AS engagement_score,

  /* Route Efficiency Score */
  CASE
    WHEN tdi.avg_duration_seconds > 0 AND tdi.avg_distance_miles > 0 THEN
      CAST(tdi.avg_distance_miles / (tdi.avg_duration_seconds / 3600) AS FLOAT64)
    ELSE CAST(0 AS FLOAT64)
  END AS speed_mph,

  /* Route Value Density */
  CASE
    WHEN tdi.avg_distance_miles > 0 THEN
      CAST(tdi.avg_trip_total / tdi.avg_distance_miles AS FLOAT64)
    ELSE CAST(0 AS FLOAT64)
  END AS value_per_mile,

  /* Route Category */
  CASE
    WHEN tdi.avg_distance_miles < 2 THEN 'Short'
    WHEN tdi.avg_distance_miles < 10 THEN 'Medium'
    WHEN tdi.avg_distance_miles < 25 THEN 'Long'
    ELSE 'Extra Long'
  END AS route_distance_category

FROM taxi_dropoff_interactions tdi
FULL OUTER JOIN area_interactions ai
  ON tdi.customer_id = ai.customer_id AND tdi.product_id = ai.product_id
LEFT JOIN taxi_demographics td ON tdi.customer_id = td.customer_id
LEFT JOIN dropoff_products dp ON COALESCE(tdi.product_id, ai.product_id) = dp.product_id
LEFT JOIN customer_behavior cb ON tdi.customer_id = cb.customer_id

/* Filter out invalid records */
WHERE COALESCE(tdi.customer_id, ai.customer_id) IS NOT NULL
  AND COALESCE(tdi.product_id, ai.product_id) IS NOT NULL

/* Deduplicate records - keep the highest value interaction */
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY
    COALESCE(tdi.customer_id, ai.customer_id),
    COALESCE(tdi.product_id, ai.product_id)
  ORDER BY
    COALESCE(tdi.trip_count, 0) DESC,
    COALESCE(tdi.avg_trip_total, 0) DESC,
    COALESCE(tdi.last_trip_timestamp, ai.last_trip_timestamp) DESC
) = 1

/* Optimize for ML training */
ORDER BY
  COALESCE(tdi.customer_id, ai.customer_id),
  COALESCE(tdi.product_id, ai.product_id),
  COALESCE(tdi.avg_trip_total, 0) DESC;