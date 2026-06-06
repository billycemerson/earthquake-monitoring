-- Staging layer for earthquake data
-- Applies deduplication, coordinate parsing, and enrichment

{{ config(
    materialized='incremental',
    unique_key=['event_id'],
    on_schema_change='sync_all_columns'
) }}

WITH source AS (

    SELECT * 
    FROM raw_earthquake

    {% if is_incremental() %}
    WHERE ingestion_time > (SELECT MAX(ingestion_time) FROM {{ this }})
    {% endif %}

),

-- Deduplicate by event_id, keep most recent
deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY event_id 
            ORDER BY ingestion_time DESC
        ) AS rn
    FROM source
),

-- Parse coordinates and add enrichment columns
cleaned AS (
    SELECT
        event_id,
        CAST(datetime AS TIMESTAMPTZ) AS event_datetime,
        coordinates,
        magnitude,
        depth_km,
        wilayah,
        dirasakan,
        ingestion_time,
        ingestion_date,
        source_file,

        -- Parse coordinates into separate latitude and longitude
        TRY_CAST(
            SPLIT_PART(coordinates, ',', 1) AS DOUBLE
        ) AS latitude,
        TRY_CAST(
            TRIM(SPLIT_PART(coordinates, ',', 2)) AS DOUBLE
        ) AS longitude,

        -- Magnitude categorization
        CASE 
            WHEN magnitude < 3.0 THEN 'Micro'
            WHEN magnitude < 4.0 THEN 'Minor'
            WHEN magnitude < 5.0 THEN 'Light'
            WHEN magnitude < 6.0 THEN 'Moderate'
            WHEN magnitude < 7.0 THEN 'Strong'
            WHEN magnitude < 8.0 THEN 'Major'
            ELSE 'Great'
        END AS magnitude_category,

        -- Depth categorization
        CASE
            WHEN depth_km < 70 THEN 'Shallow'
            WHEN depth_km < 300 THEN 'Intermediate'
            ELSE 'Deep'
        END AS depth_category,

        -- Validation flags
        (magnitude BETWEEN 0 AND 10) AS magnitude_valid,
        (depth_km > 0) AS depth_valid,

        -- Province column (populated by Python enrichment)
        NULL AS province,

        -- Load timestamp
        {{ current_timestamp() }} AS dbt_loaded_at

    FROM deduplicated
    WHERE rn = 1
)

SELECT * FROM cleaned