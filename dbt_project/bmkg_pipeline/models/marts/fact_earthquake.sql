SELECT
    *,
    CASE WHEN magnitude >= 6 THEN TRUE ELSE FALSE END AS is_high_magnitude
FROM {{ ref('stg_earthquake') }}
