-- Staging model for earthquake data
-- Applies deduplication, coordinate parsing, and enrichment

{{ config(
    materialized='incremental',
    unique_key=['event_id']
) }}

WITH raw_data AS (
    SELECT *
    FROM {{ source('raw', 'raw_earthquake') }}
    
    {% if execute %}
        {% if var('start_time', False) %}
            WHERE ingestion_time > '{{ var("start_time") }}'
        {% endif %}
    {% endif %}
),

-- Deduplicate by event_id, keep most recent
deduplicated AS (
    SELECT
        event_id,
        datetime,
        coordinates,
        magnitude,
        depth_km,
        wilayah,
        dirasakan,
        ingestion_time,
        ingestion_date,
        source_file,
        ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY ingestion_time DESC
        ) AS rn
    FROM raw_data
    WHERE rn = 1
),

-- Parse coordinates into latitude and longitude
coordinates_parsed AS (
    SELECT
        event_id,
        datetime AS event_datetime,
        coordinates,
        TRY_CAST(
            SPLIT_PART(coordinates, ',', 1) AS DOUBLE
        ) AS latitude,
        TRY_CAST(
            TRIM(SPLIT_PART(coordinates, ',', 2)) AS DOUBLE
        ) AS longitude,
        magnitude,
        CASE 
            WHEN magnitude < 3 THEN 'Low'
            WHEN magnitude < 5 THEN 'Moderate'
            WHEN magnitude < 6 THEN 'Strong'
            ELSE 'Major'
        END AS magnitude_category,
        depth_km,
        CASE
            WHEN depth_km < 70 THEN 'Shallow'
            WHEN depth_km < 300 THEN 'Intermediate'
            ELSE 'Deep'
        END AS depth_category,
        wilayah,
        dirasakan,
        ingestion_time,
        ingestion_date,
        source_file
    FROM deduplicated
),

-- Add province column (populated by Python enrichment)
enriched AS (
    SELECT
        event_id,
        event_datetime,
        coordinates,
        latitude,
        longitude,
        magnitude,
        magnitude_category,
        depth_km,
        depth_category,
        wilayah,
        dirasakan,
        NULL AS province,
        ingestion_time,
        ingestion_date,
        source_file,
        CURRENT_TIMESTAMP() AS dbt_loaded_at
    FROM coordinates_parsed
)

SELECT * FROM enriched