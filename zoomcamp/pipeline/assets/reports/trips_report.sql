/* @bruin


name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table


columns:
  - name: report_date
    data_type: DATE
    description: Trip date (from pickup datetime)
    primary_key: true
  - name: payment_type_name
    data_type: VARCHAR
    description: Payment type
    primary_key: true
  - name: trip_count
    data_type: BIGINT
    description: Number of trips
    checks:
      - name: non_negative
  - name: total_fare
    data_type: DOUBLE
    description: Total fare amount
    checks:
      - name: non_negative
  - name: avg_distance
    data_type: DOUBLE
    description: Average trip distance
  - name: avg_fare
    data_type: DOUBLE
    description: Average fare per trip

@bruin */

-- Reports: Aggregate staging data for dashboards and analytics
--
-- Aggregation: GROUP BY report_date and payment_type_name
-- Metrics: trip count, total fare, average distance, average fare
-- Time window: Filtered by report_date for time_interval strategy

SELECT
  CAST(tpep_pickup_datetime AS DATE) AS report_date,
  COALESCE(payment_type_name, 'UNKNOWN') AS payment_type_name,
  COUNT(*) AS trip_count,
  SUM(fare_amount) AS total_fare,
  AVG(trip_distance) AS avg_distance,
  AVG(fare_amount) AS avg_fare
  
FROM staging.trips

WHERE
  -- Time window filtering (required for time_interval strategy)
  tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'

GROUP BY
  CAST(tpep_pickup_datetime AS DATE),
  COALESCE(payment_type_name, 'UNKNOWN')

ORDER BY
  report_date DESC,
  payment_type_name
