/* @bruin

name: staging.trips

type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup


materialization:
  type: table


@bruin */

-- Staging: Clean, deduplicate, and enrich raw trip data
--
-- Deduplication: ROW_NUMBER partitioned by trip identifiers, keeping latest extraction
-- Enrichment: JOIN with payment_lookup to add payment type names
-- Filtering: Remove invalid records (null/zero/negative values)
-- Time window: Filtered by tpep_pickup_datetime for time_interval strategy incremental processing

WITH deduplicated AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY tpep_pickup_datetime, tpep_dropoff_datetime, trip_distance, fare_amount
      ORDER BY _extracted_at DESC
    ) AS rn
  FROM ingestion.trips
)
SELECT
  -- Trip core fields
  tpep_pickup_datetime,
  tpep_dropoff_datetime,
  passenger_count,
  trip_distance,
  fare_amount,
  extra,
  mta_tax,
  tip_amount,
  tolls_amount,
  total_amount,
  
  -- Payment type enrichment
  CAST(t.payment_type AS INTEGER) AS payment_type_id,
  COALESCE(pl.payment_type_name, 'UNKNOWN') AS payment_type_name,
  
  -- Metadata
  CAST(t._extracted_at AS TIMESTAMP) AS extracted_at,
  t._source_file
  
FROM deduplicated t
LEFT JOIN ingestion.payment_lookup pl
  ON CAST(t.payment_type AS INTEGER) = pl.payment_type_id

WHERE
  -- Deduplication: keep only latest extraction
  rn = 1
  
  -- Data quality: filter invalid records
  AND trip_distance > 0
  AND fare_amount > 0
  AND passenger_count > 0
  AND tpep_pickup_datetime IS NOT NULL
  AND tpep_dropoff_datetime IS NOT NULL
  
  -- Time window filtering (required for time_interval strategy)
  AND tpep_pickup_datetime >= '{{ start_datetime }}'
  AND tpep_pickup_datetime < '{{ end_datetime }}'