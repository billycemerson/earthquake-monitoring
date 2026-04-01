SELECT
    CURRENT_TIMESTAMP AS run_time,
    COUNT(*) AS total_records,
    MAX(magnitude) AS max_magnitude,
    SUM(CASE WHEN magnitude >= 6 THEN 1 ELSE 0 END) AS high_magnitude_count,
    MAX(datetime) AS latest_event,
    EXTRACT(EPOCH FROM (CURRENT_TIMESTAMP - MAX(datetime))) / 3600 AS freshness_lag_hours,
    CASE WHEN MAX(magnitude) >= 8 THEN TRUE ELSE FALSE END AS anomaly_flag
FROM {{ ref('stg_earthquake') }}
